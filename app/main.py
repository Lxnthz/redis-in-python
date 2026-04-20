import socket  # noqa: F401
import selectors  # noqa: F401
import sys
import time

store = {}
pending_blpop_requests = []

def parse_resp_command(data):
  parts = data.split(b"\r\n")
  items = []
  
  i = 0
  while i < len(parts):
    part = parts[i]
    if not part:
      i += 1
      continue
    
    if part.startswith(b"*"):
      i += 1
      continue
    
    if part.startswith(b"$"):
      if i + 1 < len(parts):
        items.append(parts[i + 1].decode())
      i += 2
      continue
    
    i += 1
    
  return items

def encode_simple_string(value):
  return f"+{value}\r\n".encode()

def encode_bulk_string(value):
  if value is None:
    return b"$-1\r\n"
  return f"${len(value)}\r\n{value}\r\n".encode()


def encode_integer(value):
  return f":{value}\r\n".encode()


def encode_error(message):
  return f"-{message}\r\n".encode()


def encode_array(values):
  if values is None:
    return b"*-1\r\n"

  return encode_resp(values)


def encode_resp(value):
  if value is None:
    return b"$-1\r\n"

  if isinstance(value, int):
    return encode_integer(value)

  if isinstance(value, str):
    return encode_bulk_string(value)

  if isinstance(value, list):
    response = f"*{len(value)}\r\n".encode()
    for item in value:
      response += encode_resp(item)
    return response

  raise TypeError(f"Unsupported RESP value: {type(value)!r}")


def make_string_entry(value, expires_at=None):
  return {"type": "string", "value": value, "expires_at": expires_at}


def make_list_entry(values=None):
  if values is None:
    values = []

  return {"type": "list", "value": values, "expires_at": None}


def get_entry(key):
  entry = store.get(key)
  if entry is None:
    return None

  expires_at = entry["expires_at"]
  if expires_at is not None and time.monotonic() >= expires_at:
    del store[key]
    return None

  return entry


def get_value(key):
  entry = get_entry(key)
  if entry is None:
    return None

  if entry["type"] != "string":
    return None

  return entry["value"]


def get_list_for_write(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_list_entry()
    return store[key]["value"]

  if entry["type"] != "list":
    return None

  return entry["value"]

def make_stream_entry(values=None, last_ms=0, last_seq=0):
  if values is None:
    values = []
  
  return {
    "type": "stream",
    "value": values,
    "expires_at": None,
    "last_ms": last_ms,
    "last_seq": last_seq,
  }
  
def get_stream_for_write(key):
  entry = get_entry(key)
  if entry is None:
    store[key] = make_stream_entry()
    return store[key]["value"]
  
  if entry["type"] != "stream":
    return None
  
  return entry["value"]

def generate_stream_id(entry, requested_ms=None):
  current_ms = int(time.time() * 1000) if requested_ms is None else requested_ms
  current_ms = max(current_ms, entry["last_ms"])

  if current_ms == entry["last_ms"]:
    entry["last_seq"] += 1
  else:
    entry["last_ms"] = current_ms
    entry["last_seq"] = 0

  return f"{entry['last_ms']}-{entry['last_seq']}"

def get_list_for_read(key):
  entry = get_entry(key)
  if entry is None:
    return []

  if entry["type"] != "list":
    return None

  return entry["value"]


def trim_lrange(values, start, stop):
  length = len(values)
  if length == 0:
    return []

  if start < 0:
    start += length
  if stop < 0:
    stop += length

  start = max(start, 0)
  stop = min(stop, length - 1)

  if start > stop or start >= length:
    return []

  return values[start:stop + 1]


def pop_from_list(key, count=None):
  list_values = get_list_for_read(key)
  if list_values is None or len(list_values) == 0:
    return None

  if count is None:
    popped = list_values.pop(0)
    if len(list_values) == 0:
      del store[key]
    return popped

  popped_items = []
  pop_count = min(count, len(list_values))
  for _ in range(pop_count):
    popped_items.append(list_values.pop(0))

  if len(list_values) == 0:
    del store[key]

  return popped_items


def try_pop_from_keys(keys):
  for key in keys:
    popped = pop_from_list(key)
    if popped is not None:
      return [key, popped]

  return None


def wake_pending_blpop_requests():
  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    response = try_pop_from_keys(request["keys"])

    if response is None:
      index += 1
      continue

    connection = request["connection"]
    try:
      connection.sendall(encode_array(response))
    except OSError:
      pass

    pending_blpop_requests.pop(index)


def expire_pending_blpop_requests():
  now = time.monotonic()

  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    deadline = request["deadline"]

    if deadline is None or now < deadline:
      index += 1
      continue

    connection = request["connection"]
    try:
      connection.sendall(encode_array(None))
    except OSError:
      pass

    pending_blpop_requests.pop(index)


def get_selector_timeout():
  if not pending_blpop_requests:
    return None

  nearest_deadline = None
  for request in pending_blpop_requests:
    deadline = request["deadline"]
    if deadline is None:
      continue

    if nearest_deadline is None or deadline < nearest_deadline:
      nearest_deadline = deadline

  if nearest_deadline is None:
    return None

  return max(0, nearest_deadline - time.monotonic())

def accept_connection(server_socket, selector):
  connection, _ = server_socket.accept()
  connection.setblocking(False)
  selector.register(connection, selectors.EVENT_READ, read_client)
  
def parse_stream_id(stream_id):
  if stream_id in ("-", "+"):
    return stream_id

  parts = stream_id.split("-", 1)
  if len(parts) != 2:
    return None

  ms_text, seq_text = parts
  if not ms_text.isdigit():
    return None

  if seq_text == "*":
    return int(ms_text), "*"

  if not seq_text.isdigit():
    return None

  return int(ms_text), int(seq_text)
  
def read_client(connection, selector):
  try:
    data = connection.recv(1024)
  except ConnectionResetError:
    close_client(connection, selector)
    return
  
  if not data:
    close_client(connection, selector)
    return
  
  command_parts = parse_resp_command(data)
  if not command_parts:
    connection.sendall(encode_simple_string("PONG"))
    return
  
  command = command_parts[0].upper()
  
  if command == "PING":
    connection.sendall(encode_simple_string("PONG"))
    return

  if command == "ECHO" and len(command_parts) >= 2:
    connection.sendall(encode_bulk_string(command_parts[1]))
    return
  
  if command == "SET" and len(command_parts) >= 3:
    key = command_parts[1]
    value = command_parts[2]

    expires_at = None
    if len(command_parts) >= 5:
      option = command_parts[3].upper()
      option_value = command_parts[4]

      if option == "PX":
        expires_at = time.monotonic() + (int(option_value) / 1000)
      elif option == "EX":
        expires_at = time.monotonic() + int(option_value)

    store[key] = make_string_entry(value, expires_at)
    connection.sendall(encode_simple_string("OK"))
    return

  if command == "GET" and len(command_parts) >= 2:
    key = command_parts[1]
    connection.sendall(encode_bulk_string(get_value(key)))
    return
  
  if command == "TYPE" and len(command_parts) >= 2:
    key = command_parts[1]
    entry = get_entry(key)
    
    if entry is None:
      connection.sendall(encode_simple_string("none"))
      return
    
    connection.sendall(encode_simple_string(entry["type"]))
    return
  
  if command == "XADD" and len(command_parts) >= 5:
    key = command_parts[1]
    id_token = command_parts[2]
    field_values = command_parts[3:]

    if len(field_values) % 2 != 0:
      connection.sendall(encode_error("ERR wrong number of arguments for 'XADD' command"))
      return

    stream_values = get_stream_for_write(key)
    if stream_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    stream_entry = store[key]

    if id_token == "*":
      entry_id = generate_stream_id(stream_entry)
    else:
      parsed_id = parse_stream_id(id_token)
      if parsed_id is None:
        connection.sendall(encode_error("ERR Invalid stream ID specified as stream command argument"))
        return

      if parsed_id[1] == "*":
        requested_ms = parsed_id[0]
        if requested_ms < stream_entry["last_ms"]:
          connection.sendall(encode_error("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
          return

        entry_id = generate_stream_id(stream_entry, requested_ms=requested_ms)
      else:
        if parsed_id <= (0, 0):
          connection.sendall(encode_error("ERR The ID specified in XADD must be greater than 0-0"))
          return

        last_stream_id = (0, 0)
        if stream_values:
          last_stream_id = parse_stream_id(stream_values[-1]["id"])
          if last_stream_id is None:
            connection.sendall(encode_error("ERR Invalid stream state"))
            return

        if parsed_id <= last_stream_id:
          connection.sendall(encode_error("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
          return

        stream_entry["last_ms"], stream_entry["last_seq"] = parsed_id
        entry_id = id_token

    entry_fields = {}
    for i in range(0, len(field_values), 2):
      entry_fields[field_values[i]] = field_values[i + 1]

    stream_values.append({
      "id": entry_id,
      "fields": entry_fields,
    })

    connection.sendall(encode_bulk_string(entry_id))
    return
    
  if command == "RPUSH" and len(command_parts) >= 3:
    key = command_parts[1]
    values = command_parts[2:]

    list_values = get_list_for_write(key)
    if list_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    list_values.extend(values)
    connection.sendall(encode_integer(len(list_values)))
    wake_pending_blpop_requests()
    return

  if command == "LPUSH" and len(command_parts) >= 3:
    key = command_parts[1]
    values = command_parts[2:]

    list_values = get_list_for_write(key)
    if list_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    for value in values:
      list_values.insert(0, value)

    connection.sendall(encode_integer(len(list_values)))
    wake_pending_blpop_requests()
    return

  if command == "LRANGE" and len(command_parts) >= 4:
    key = command_parts[1]
    start = int(command_parts[2])
    stop = int(command_parts[3])

    list_values = get_list_for_read(key)
    if list_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    connection.sendall(encode_array(trim_lrange(list_values, start, stop)))
    return

  if command == "LLEN" and len(command_parts) >= 2:
    key = command_parts[1]
    list_values = get_list_for_read(key)

    if list_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    connection.sendall(encode_integer(len(list_values)))
    return

  if command == "LPOP" and len(command_parts) >= 2:
    key = command_parts[1]

    if len(command_parts) >= 3:
      count = int(command_parts[2])
      popped_items = pop_from_list(key, count)

      if popped_items is None:
        connection.sendall(encode_array(None))
        return

      connection.sendall(encode_array(popped_items))
      return

    popped_item = pop_from_list(key)
    connection.sendall(encode_bulk_string(popped_item))
    return

  if command == "BLPOP" and len(command_parts) >= 3:
    keys = command_parts[1:-1]
    timeout_seconds = float(command_parts[-1])
    response = try_pop_from_keys(keys)
    if response is not None:
      connection.sendall(encode_array(response))
      return

    deadline = None
    if timeout_seconds > 0:
      deadline = time.monotonic() + timeout_seconds

    pending_blpop_requests.append({
      "connection": connection,
      "keys": keys,
      "deadline": deadline,
    })
    return
  
  connection.sendall(encode_simple_string("OK"))

def close_client(connection, selector):
  index = 0
  while index < len(pending_blpop_requests):
    request = pending_blpop_requests[index]
    if request["connection"] == connection:
      pending_blpop_requests.pop(index)
      continue

    index += 1

  try:
    selector.unregister(connection)
  except Exception:
    pass
  
  connection.close()

def main():
    print("Logs from your program will appear here!")

    selector = selectors.DefaultSelector()
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=(sys.platform != "win32"))
    server_socket.setblocking(False)
    
    selector.register(server_socket, selectors.EVENT_READ, accept_connection)
    
    while True:
      events = selector.select(timeout=get_selector_timeout())
      for key, _ in events:
        callback = key.data
        callback(key.fileobj, selector)

      expire_pending_blpop_requests()

if __name__ == "__main__":
    main()
