import socket  # noqa: F401
import selectors  # noqa: F401

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

  if command == "ECHO" and len(command_parts) > 2:
    connection.sendall(encode_bulk_string(command_parts[1]))
    return
  
  if command == "SET" and len(command_parts) > 3:
    key = command_parts[1]
    value = command_parts[2]
    store[key] = value
    connection.sendall(encode_simple_string("OK"))
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
