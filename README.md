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

## Chapter 4 - Transactions

Chapter 4 added Redis-style transactions so multiple commands can be queued and executed atomically from the client perspective.

1. Key exists and has a numerical value INCR

   I implemented [INCR](app/main.py) so existing numeric string values are parsed, incremented, and stored back as strings. This keeps the counter behavior aligned with Redis while still using the in-memory store.

2. Key doesn't exist INCR

   I made [INCR](app/main.py) create a missing key with value `1` and return the incremented integer reply. That gives the command the expected counter initialization behavior.

3. Key exists but doesn't have a numerical value INCR

   I added integer parsing checks in [the INCR handler](app/main.py), so non-numeric strings return an error instead of being coerced. This preserves Redis's strict numeric semantics.

4. The MULTI command

   I introduced [MULTI](app/main.py) to begin a transaction for a connection and switch subsequent commands into queued mode. The server now tracks transaction state per client connection.

5. The EXEC command

   I implemented [EXEC](app/main.py) so queued commands are replayed in order and returned as a RESP array. Each queued command is executed against the shared in-memory state when the transaction is committed.

6. Empty transaction

   I handled empty transaction execution in [EXEC](app/main.py), which returns an empty RESP array when no commands were queued. That keeps the transaction boundary behavior predictable.

7. Queueing commands

   I made commands entered after [MULTI](app/main.py) return `QUEUED` instead of executing immediately. This lets the client build up a batch before committing it with `EXEC`.

8. Executing a transaction

   I added queued-command execution in [execute_transaction_queue()](app/main.py), which runs each stored command in order and serializes the results as one transaction reply. The implementation preserves response types such as simple strings, integers, bulk strings, and errors.

9. The DISCARD command

   I implemented [DISCARD](app/main.py) to cancel the current transaction and clear any queued commands for that connection. This lets a client abandon a transaction cleanly before execution.

10. Failures within transactions

    I preserved command failures inside [EXEC](app/main.py) as error entries in the returned array rather than aborting the whole transaction. That matches the expected Redis behavior where a failing command still occupies its slot in the result list.

11. Multiple transactions

    I kept transaction state isolated per connection in [transaction_commands](app/main.py), so different clients can use `MULTI`/`EXEC` independently. This allows multiple active transactions without one client interfering with another.

## Chapter 5 - Optimistic Locking

Chapter 5 added optimistic locking support so transactions can be safely retried when watched keys change, enabling conflict detection without blocking locks.

1. The WATCH command

   I implemented [WATCH](app/main.py) to start monitoring one or more keys for changes. When `WATCH` is called, I snapshot the current version of each key in [watch_keys_for_connection()](app/main.py) and store the per-connection watch state in [watched_keys](app/main.py). This lets the client set up a watch before starting a transaction.

2. WATCH inside transaction

   I added a check in [the WATCH handler](app/main.py) to reject `WATCH` if a transaction is already active on that connection. This matches Redis behavior where `WATCH` must be called before `MULTI`, not inside the transaction. The command returns an error if attempted inside a transaction.

3. Tracking key modifications

   I introduced a global [key_versions](app/main.py) dictionary that tracks a version counter for every key. Whenever a key is modified (via `SET`, `INCR`, `XADD`, `RPUSH`, `LPUSH`, `LPOP`, or expiry), I call [touch_key()](app/main.py) to increment its version. This leaves a timestamp of when each key was last written.

4. Watching multiple keys

   I extended [WATCH](app/main.py) to accept multiple key arguments in a single command. The implementation stores all watched keys and their versions together in the connection's watch dictionary, so a single `WATCH` call can protect multiple keys from concurrent modification.

5. Watching missing keys

   I allow [WATCH](app/main.py) to snapshot keys that don't exist yet by calling [get_key_version()](app/main.py), which initializes missing keys with version 0. This lets the client watch for the creation of a key that might not exist initially, which is useful for implementing optimistic locking patterns on newly created data.

6. The UNWATCH command

   I implemented [UNWATCH](app/main.py) to clear all watched keys for a connection. This is called in [clear_watched_keys()](app/main.py) and is useful if the client wants to abandon a watch or start fresh with a different set of keys.

7. Unwatch on EXEC

   I added a call to [clear_watched_keys()](app/main.py) inside [execute_transaction_queue()](app/main.py) after a transaction executes successfully. This ensures that all watches are automatically cleared after `EXEC` completes, whether the transaction succeeded or was aborted due to a conflict.

8. Unwatch on DISCARD

   I extended [the DISCARD handler](app/main.py) to call [clear_watched_keys()](app/main.py) when a transaction is discarded. This ensures that abandoning a transaction also clears its watches, so subsequent transactions don't inherit stale watch state. Additionally, watches are cleared on connection close in [close_client()](app/main.py) to prevent watch leaks.

## Chapter 6 - Replication

Chapter 6 added master-replica replication flow, including handshake, command propagation, replica acknowledgements, and `WAIT` semantics.

1. The INFO command

   I implemented `INFO replication` in [execute_command()](app/main.py), where the server returns replication metadata as a RESP bulk string. The payload is assembled in [replication_info_text()](app/main.py), which keeps the output centralized and easy to extend.

2. The INFO command on a replica

   I made [replication_info_text()](app/main.py) return replica-specific fields when role is `slave`, including `master_host`, `master_port`, and `master_link_status`. This gives clients a direct way to verify that a node is configured as a replica and linked to its upstream master.

3. Initial replication ID and offset

   I generate the initial master replication ID in [random_replid()](app/main.py) and initialize offset tracking with [master_repl_offset](app/main.py). During startup in [main()](app/main.py), each master instance gets a unique replid and starts at offset `0`.

4. The replica sends a PING to the master

   In [connect_to_master_and_handshake()](app/main.py), the replica first sends `PING` and waits for `+PONG`. This verifies basic connectivity before moving into replication-specific negotiation.

5. The replica sends REPLCONF twice to the master

   I added both required `REPLCONF` calls in [connect_to_master_and_handshake()](app/main.py): `REPLCONF listening-port <port>` and `REPLCONF capa psync2`. This shares replica metadata and replication capabilities with the master.

6. The replica sends PSYNC to the master

   After capability negotiation, the replica sends `PSYNC ? -1` in [connect_to_master_and_handshake()](app/main.py). It then reads `FULLRESYNC` and the initial RDB payload to complete bootstrap.

7. Receiving a replication handshake as a master

   I implemented handshake handling in [execute_command()](app/main.py) for `PING`, `REPLCONF`, and `PSYNC`, allowing the server to act as a master for inbound replica connections. Connection parsing for handshake commands is shared with regular client processing in [read_client()](app/main.py).

8. Support for receiving the PSYNC command from the replica

   In the `PSYNC` branch of [execute_command()](app/main.py), the master replies with `FULLRESYNC <replid> <offset>` and records the connection in [replica_connections](app/main.py). This marks the client as an active replica for future propagation and ACK tracking.

9. Empty RDB transfer

   I added an empty/small RDB bootstrap transfer using [EMPTY_RDB_HEX](app/main.py), which is encoded and sent right after `FULLRESYNC` in [execute_command()](app/main.py). On the replica side, [read_bulk_string_response()](app/main.py) reads that payload during handshake.

10. Single-replica propagation

    I implemented write propagation in [propagate_to_replicas()](app/main.py). When one replica is connected, write commands (for example `SET`) are forwarded to that replica and master replication offset advances by the propagated command length.

11. Multi-replica propagation

    The same [propagate_to_replicas()](app/main.py) logic fans out writes to all connected replicas and cleans up dead replica sockets. This allows one master to keep multiple replicas synchronized.

12. Command processing

    I added replica-side command apply logic in [apply_replicated_write()](app/main.py), and wired it through [process_master_commands()](app/main.py) and [read_master()](app/main.py). This ensures propagated writes are actually executed on replicas, not just acknowledged at the protocol layer.

13. ACKs with no commands

    I implemented `REPLCONF GETACK *` handling so replicas respond with `REPLCONF ACK <offset>` in [process_master_commands()](app/main.py) and `REPLCONF` handling in [execute_command()](app/main.py). This works even when no new writes were processed, using the current processed offset.

14. ACKs with commands

    I track replica progress in [replica_processed_offset](app/main.py) and update master-side ack state in [replica_ack_offsets](app/main.py). After commands are propagated, ACK offsets let the master know which replicas have caught up to the target replication offset.

15. WAIT with no replicas

    In [handle_wait()](app/main.py), `WAIT` returns `0` immediately when [replica_connections](app/main.py) is empty. This matches expected behavior when there are no connected replicas to acknowledge writes.

16. WAIT with no commands

    I made [count_acked_replicas()](app/main.py) return connected replica count when `master_repl_offset == 0`, so `WAIT` can succeed without waiting if there are no pending writes to replicate.

17. WAIT with multiple comma

    I implemented `WAIT` polling in [handle_wait()](app/main.py) with `REPLCONF GETACK *` fan-out via [request_replica_acks()](app/main.py), then counts matching ACKs across multiple replicas in [count_acked_replicas()](app/main.py). This supports multi-replica/multi-round acknowledgement scenarios in one `WAIT` flow.

## Chapter 7 - RDB Persistence

Chapter 7 added RDB-based startup persistence so the server can load keys and values from disk, including expiry-aware values.

1. RDB file config

   I added command-line configuration in [parse_server_config()](app/main.py) for `--dir` and `--dbfilename`, and exposed them through `CONFIG GET` in [execute_command()](app/main.py). The full file path is resolved by [get_rdb_path()](app/main.py), so startup loading uses the configured directory and filename.

2. Read a key

   I implemented `KEYS` matching in [execute_command()](app/main.py), which reads from the in-memory store populated by RDB loading and returns a RESP array. This made it possible to verify that keys were successfully loaded at boot.

3. Read a string value

   I added a minimal RDB parser in [load_rdb_file()](app/main.py) that supports string object entries (`opcode 0x00`) and loads them into the existing string store model. Once loaded, normal [GET handling](app/main.py) can return values without any special-case path.

4. Read multiple keys

   I made `KEYS` work with wildcard patterns using [fnmatch](app/main.py) in [execute_command()](app/main.py), so it can return multiple matching keys from a loaded RDB dataset. The handler also skips expired entries by calling [get_entry()](app/main.py) before including a key.

5. Read multiple string values

   Because [load_rdb_file()](app/main.py) iterates through the RDB keyspace and stores each parsed string entry, multiple values are available immediately after startup. Repeated `GET` calls against different keys read directly from the loaded in-memory state.

6. Read value with expiry

   I added expiry opcode handling (`0xFC` milliseconds, `0xFD` seconds) in [load_rdb_file()](app/main.py), convert absolute UNIX expiry to monotonic deadline with [unix_ms_to_monotonic_deadline()](app/main.py), and keep expiry enforcement centralized in [get_entry()](app/main.py). This ensures expired values are not returned after load.

## Chapter 8 - Pub/Sub

Chapter 8 added Redis Pub/Sub behavior for channel subscriptions, message broadcasting, and subscribed-mode command handling.

1. Subscribe to a channel

   I implemented `SUBSCRIBE` in [read_client()](app/main.py), where a connection is registered to a channel and receives a subscribe confirmation reply. Subscription state is tracked with [subscribed_connections](app/main.py) and [channel_subscribers](app/main.py) through [subscribe_connection_to_channel()](app/main.py).

2. Subscribe to multiple channels

   I made `SUBSCRIBE` accept multiple channel names in one command inside [read_client()](app/main.py). The server registers each channel one by one and emits a confirmation array for each, with an updated subscription count.

3. Enter subscribed mode

   I added subscribed-mode detection using [get_subscription_count()](app/main.py). When a client is subscribed, [read_client()](app/main.py) restricts allowed commands and rejects unsupported ones with a Redis-style error that includes the blocked command name.

4. PING in subscribed mode

   I handled `PING` specially while subscribed in [read_client()](app/main.py), returning Pub/Sub-style `pong` arrays instead of normal simple-string `PONG`. This keeps behavior aligned with Redis subscribed-mode semantics.

5. Publish a message

   I implemented `PUBLISH` in [execute_command()](app/main.py). It calls [publish_message()](app/main.py), broadcasts to current channel subscribers, and returns the integer count of recipients that received the message.

6. Deliver messages

   I send message payloads in Pub/Sub format from [publish_message()](app/main.py) as `message` arrays containing channel and data. This ensures each subscribed client receives the same published event through its open connection.

7. Unsubscribe

   I implemented `UNSUBSCRIBE` in [read_client()](app/main.py) for both explicit channels and full unsubscribe (no args). The logic uses [unsubscribe_connection_from_channel()](app/main.py) and [unsubscribe_all_channels()](app/main.py), and I also clean subscriptions on disconnect in [remove_pending_requests_for_connection()](app/main.py).

## Chapter 9 - Sorted Sets

Chapter 9 added sorted set support, including score-based ordering, rank lookups, and member deletion.

1. Create a sorted set

   I added a dedicated sorted set entry type through [make_zset_entry()](app/main.py). New sorted sets are created lazily in [get_zset_entry_for_write()](app/main.py), which keeps behavior consistent with other data structures in the server.

2. Add members

   I implemented `ZADD` in [execute_command()](app/main.py), accepting one or more `score member` pairs and storing them in the sorted set map. Ordering is derived from score and member name using [zset_sorted_items()](app/main.py).

3. Retrieve member rank

   I added `ZRANK` in [execute_command()](app/main.py), which sorts members by score then returns the member index as an integer reply. Missing members return null bulk response like Redis.

4. List sorted set members

   I implemented `ZRANGE` in [execute_command()](app/main.py) with score-order output and optional `WITHSCORES`. This allows listing members or interleaving members with score strings from the same command path.

5. ZRANGE with negative indexes

   I reused [trim_lrange()](app/main.py) for index slicing, which already handles negative offsets safely. That gives `ZRANGE` correct start/stop behavior for both positive and negative indexes.

6. Count sorted set members

   I added `ZCARD` in [execute_command()](app/main.py), returning the number of members stored in a sorted set as an integer response.

7. Retrieve member score

   I implemented `ZSCORE` in [execute_command()](app/main.py), returning the member score as a bulk string when present or null bulk when missing.

8. Remove a member

   I added `ZREM` in [execute_command()](app/main.py) to delete one or more members and return the number removed. When a set becomes empty, the key is deleted from the store.

## Chapter 10 - Geospatial Commands

Chapter 10 added geo operations on top of sorted sets, including coordinate storage, score encoding, distance calculation, and radius search.

1. Respond to GEOADD

   I implemented `GEOADD` in [execute_command()](app/main.py), parsing one or more `longitude latitude member` triples and returning how many new members were inserted.

2. Validate coordinates

   I added [validate_geo_coordinates()](app/main.py) to enforce valid longitude/latitude ranges before a location is stored. Invalid coordinates return an error response and skip write.

3. Store a location

   I store geo member coordinates in the sorted set entry’s geo map (`entry["geo"]`) while also writing score data to `entry["value"]`. This keeps coordinate lookup and sorted-set integration in one structure.

4. Calculate location score

   I implemented geospatial score encoding in [calculate_geo_score()](app/main.py), using fixed-point normalization plus bit interleaving ([interleave_26_bits()](app/main.py)). That produces sortable geo scores compatible with sorted set storage.

5. Respond to GEOPOS

   I added `GEOPOS` in [execute_command()](app/main.py), returning an array of coordinate pairs for requested members, with null entries for unknown members.

6. Decode coordinates

   I return coordinates directly from the stored geo map in `GEOPOS`, formatting values through [format_float()](app/main.py). This gives stable RESP output for longitude/latitude reads.

7. Calculate distance

   I implemented `GEODIST` in [execute_command()](app/main.py) using Haversine distance from [geo_distance_meters()](app/main.py), with unit conversion from [unit_to_meters_multiplier()](app/main.py) for `m`, `km`, `mi`, and `ft`.

8. Search within radius

   I implemented radius search with `GEOSEARCH` in [execute_command()](app/main.py), parsing arguments in [parse_geosearch_arguments()](app/main.py) and filtering results in [run_geosearch()](app/main.py). I also added `GEORADIUS` as a compatibility wrapper that translates to `GEOSEARCH`.
