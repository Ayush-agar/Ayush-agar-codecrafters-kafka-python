import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, raddr = server.accept() # wait for client
    # data = conn.recv(32)
    # print(f"data from client {data.decode()}")
    # conn.sendall(b"hello from the server side")
    conn.sendall((0).to_bytes(4, signed=True) + (7).to_bytes(4, signed=True))



if __name__ == "__main__":
    main()
