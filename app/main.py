import socket  # noqa: F401
import selectors  # noqa: F401

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
  
  connection.sendall(b"+PONG\r\n")
  
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
