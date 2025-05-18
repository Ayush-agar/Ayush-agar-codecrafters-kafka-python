import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    import struct
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, raddr = server.accept() # wait for client
    data = conn.recv(1024)
    print(f"data {data}")
    offset = 0
    # message= struct.unpack_from('>I', buffer=data, offset=offset) # int32 for message
    # offset += 4
    # print(f"message {message[0]}")
    request_api_key, request_api_version = struct.unpack_from(">HH", buffer=data, offset=offset)
    # offset+=4
    # print(f"request_api_key {request_api_key} and request_api_version {request_api_version}")
    correlation_id = struct.unpack_from('>I', buffer=data, offset=8)
    # print(f"correlation_id {correlation_id}")
    # print(f"data from client {data.decode()}")
    # conn.sendall(b"hello from the server side")
    conn.sendall(correlation_id[0].to_bytes(4, signed=True))



if __name__ == "__main__":
    main()
