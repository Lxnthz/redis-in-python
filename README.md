[![progress-banner](https://backend.codecrafters.io/progress/redis/7319cfc0-ee29-4a12-b1d3-ead11ef3570b)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

This repository contains my attempt to replicate Redis that is capable of handling basic command like `PING`, `SET`, and `GET` as part of CodeCrafter Build Your Own X challenges. Along the way i also learned about event loops, Redis protocol, and more.

Redis is an in-memory data structure store used as a high-performance database, cache, message broker, and streaming engine. It stores data in memory, rather than on disk, providing sub-millisecond latency for real-time applications. It supports versatile data types like strings, hashes, lists, sets, and JSON.

In this project, I'm rebuilding the idea using 100% Python.

## Chapter 1 - Basics

Chapter 1 focused on turning a raw socket server into a small Redis-like server that can accept connections, parse RESP commands, and store values in memory.

1. Bind to a port

   I started by creating the TCP listener in [main()](app/main.py#L117) and binding it to `localhost:6379` in [the server setup](app/main.py#L122). This gave the program a real network endpoint before I added any command logic.

2. Respond to PING

   I added the first command branch in [read_client()](app/main.py#L58) and returned `+PONG` from [the PING handler](app/main.py#L76-L78). I kept the response simple so I could confirm the request loop worked end to end.

3. Respond to multiple PINGs

   I left the server running inside the selector loop in [main()](app/main.py#L127-L131), which let the same connection handle repeated reads without restarting the process. The key idea was to keep the socket open and only react when new data arrived.

4. Handle concurrent clients

   I used [selectors.DefaultSelector](app/main.py#L120) together with [accept_connection()](app/main.py#L53-L56) so one process could manage many clients at once. That let the server register each client socket separately instead of blocking on a single connection.

5. Implement the ECHO command

   I added RESP parsing in [parse_resp_command()](app/main.py#L7-L30) and returned the argument as a bulk string in [the ECHO handler](app/main.py#L80-L82). The approach was to decode just enough of the protocol to extract the command name and its payload.

6. Implement the SET & GET commands

   I introduced an in-memory dictionary in [store](app/main.py#L5) and handled command state in [the SET and GET branches](app/main.py#L84-L105). `SET` saves the key-value pair, and `GET` looks it up and responds with the correct RESP bulk string.

7. Expiry

   I extended `SET` in [the expiry logic](app/main.py#L88-L99) to store an expiration timestamp alongside each value, and I added [get_value()](app/main.py#L41-L51) so expired keys are removed before a `GET` response is sent. I kept the implementation local to reads so the code stayed small and easy to reason about.

## Chapter 2 - Lists

1. Create a List

   I used lazy initialization in [get_list_for_write()](app/main.py#L78-L88), so the list is created the first time a push command targets a missing key. This kept list creation implicit and close to write paths.

2. Append an element

   I implemented append behavior with [the RPUSH handler](app/main.py#L203-L214), where incoming values are added to the right side of the list. For a single value, the same path works and returns the new list length.

3. Append multiple elements

   I reused the same [RPUSH implementation](app/main.py#L203-L214) and accepted all remaining arguments as elements. The code extends the list in one pass, which makes single and multi append behavior consistent.

4. List elements (positive indexes)

   I handled positive index ranges in [LRANGE](app/main.py#L231-L242) and delegated slicing to [trim_lrange()](app/main.py#L99-L115). This separates command parsing from index logic and keeps responses predictable.

5. List elements (negative indexes)

   Negative index normalization is done in [trim_lrange()](app/main.py#L104-L108), where negative values are translated relative to the list length. After normalization, bounds are clamped before returning a RESP array.

6. Prepend elements

   I added prepend support in [the LPUSH handler](app/main.py#L216-L229), inserting each element at the left side of the list. This mirrors Redis behavior and returns the final list size.

7. Query list length

   I added [LLEN](app/main.py#L244-L253), which reads the list value and returns an integer reply. The path also checks for wrong-type keys to avoid mixing string and list semantics.

8. Remove an element

   I implemented single-element removal via [LPOP](app/main.py#L255-L271) and [pop_from_list()](app/main.py#L118-L124). The command pops from the left and returns a bulk string reply.

9. Remove multiple elements

   I extended [LPOP](app/main.py#L255-L267) with the optional count argument and reused [pop_from_list()](app/main.py#L126-L136) to remove up to count items. This returns a RESP array and deletes the key when the list becomes empty.

10. Blocking retrieval with timeout

    I implemented [BLPOP](app/main.py) as a non-blocking wait flow: [read_client()](app/main.py) first tries to pop immediately, then stores the request in a pending queue when no values are available. New pushes wake waiting clients through [wake_pending_blpop_requests()](app/main.py), and timed waits are completed by [expire_pending_blpop_requests()](app/main.py) from the main event loop. This keeps the server responsive to concurrent clients while still supporting blocking list retrieval.

## Chapter 3 - Streams

Chapter 3 extended the server with Redis Streams support, including stream creation, ID validation, range queries, and blocking reads.

1. The TYPE command

   I added [TYPE handling](app/main.py) so keys now report `stream` when they point to stream entries. The command still returns `none` for missing or expired keys, which keeps it aligned with the existing store behavior.

2. Create a stream

   I introduced stream entries in [the stream helpers](app/main.py) and created them lazily when `XADD` targets a missing key. This keeps stream creation consistent with the rest of the in-memory data model.

3. Validating entry IDs

   I added ID parsing and validation in [the XADD path](app/main.py), so malformed stream IDs are rejected before they can be stored. That keeps the stream state ordered and predictable.

4. Partially auto-generated IDs

   I supported `ms-*` style IDs in [the stream ID logic](app/main.py), which lets Redis generate the sequence number while the millisecond portion is provided by the caller. This preserves ordering within the same millisecond.

5. Fully auto-generated IDs

   I kept `XADD` with `*` fully automatic in [generate_stream_id()](app/main.py), allowing Redis to generate both the millisecond and sequence components. The implementation also increments the sequence number when multiple entries land in the same millisecond.

6. Query entries from stream

   I added [XRANGE](app/main.py) to return stream entries as nested RESP arrays. The command walks the stream in insertion order and emits each matching entry with its fields.

7. Query with -

   I handled `-` as the lower bound in [XRANGE](app/main.py), which means the query starts from the beginning of the stream. That makes open-ended left ranges work like Redis.

8. Query with +

   I handled `+` as the upper bound in [XRANGE](app/main.py), which means the query can extend through the end of the stream. This completes the open-ended range syntax.

9. Query single stream using XREAD

   I added [XREAD](app/main.py) support for one stream by reading from a key and returning only entries newer than the supplied cursor. The response format matches Redis stream replies, so the caller gets the stream name and matching entries together.

10. Query multiple streams using XREAD

    I extended [XREAD](app/main.py) to accept multiple key/cursor pairs in one request. Each stream is evaluated independently, and the response includes only the streams that have new entries.

11. Blocking reads

    I implemented blocking stream reads in [the XREAD path](app/main.py) by storing pending requests when no matching entries are available. When a new stream entry arrives, waiting readers are checked and released if their cursor can advance.

12. Blocking reads without timeout

    I supported `BLOCK 0` in [XREAD](app/main.py), which leaves the request pending until new data arrives. That matches the indefinite blocking behavior expected by Redis.

13. Blocking reads using $

    I supported `$` cursors in [XREAD](app/main.py) so blocking consumers can wait for entries that arrive after the current stream tail. This makes the consumer start from the latest known position and only receive future updates.
