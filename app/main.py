import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True) # [STAGE 1] Create a server socket and wait for a client to connect.
    connection, _ = server_socket.accept() # [STAGE 2] Accept a client connection, _ is the address of the client, which we can ignore for this task.
    connection.sendall(b"+PONG\r\n") # [STAGE 2] Send the "+PONG\r\n" response to the client. `b` prefix convert the string to a bytes object, which is required for sending data over a socket.

if __name__ == "__main__":
    main()
