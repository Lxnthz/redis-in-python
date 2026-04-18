import socket  # noqa: F401
import selectors  # noqa: F401
import time

store = {}

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

  response = f"*{len(values)}\r\n".encode()
  for value in values:
    response += encode_bulk_string(value)
  return response


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


def blpop(keys, timeout_seconds):
  deadline = time.monotonic() + timeout_seconds

  while True:
    for key in keys:
      popped = pop_from_list(key)
      if popped is not None:
        return [key, popped]

    if timeout_seconds == 0:
      time.sleep(0.01)
      continue

    if time.monotonic() >= deadline:
      return None

    time.sleep(0.01)

def accept_connection(server_socket, selector):
  connection, _ = server_socket.accept()
  connection.setblocking(False)
  selector.register(connection, selectors.EVENT_READ, read_client)
  
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

  if command == "RPUSH" and len(command_parts) >= 3:
    key = command_parts[1]
    values = command_parts[2:]

    list_values = get_list_for_write(key)
    if list_values is None:
      connection.sendall(encode_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
      return

    list_values.extend(values)
    connection.sendall(encode_integer(len(list_values)))
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
    response = blpop(keys, timeout_seconds)
    connection.sendall(encode_array(response))
    return
  
  connection.sendall(encode_simple_string("OK"))

def close_client(connection, selector):
  try:
    selector.unregister(connection)
  except Exception:
    pass
  
  connection.close()

def main():
    print("Logs from your program will appear here!")

    selector = selectors.DefaultSelector()
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(False)
    
    selector.register(server_socket, selectors.EVENT_READ, accept_connection)
    
    while True:
      events = selector.select()
      for key, _ in events:
        callback = key.data
        callback(key.fileobj, selector)

if __name__ == "__main__":
    main()
