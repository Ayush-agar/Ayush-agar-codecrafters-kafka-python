import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    import struct
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, raddr = server.accept() # wait for client
    data = conn.recv(1024)
    # print(f"data {data}")
    REQUEST_STRUCT = {
        "message_size": 4,
        "request_api_key": 2,
        "request_api_version": 2,
        "correlation_id": 4,
    }
    REQUEST_SIZE = sum(size for size in REQUEST_STRUCT.values())
    req_values = dict()
    offset = 0
    for k, v in REQUEST_STRUCT.items():
        value = int.from_bytes(data[offset : offset + v], byteorder="big")
        req_values[k] = value
        offset += v
    # message= struct.unpack_from('>I', buffer=data, offset=offset) # int32 for message
    # offset += 4
    # print(f"message {message[0]}")
    # request_api_key, request_api_version = struct.unpack_from(">HH", buffer=data, offset=offset)
    # offset+=4
    # print(f"request_api_key {request_api_key} and request_api_version {request_api_version}")
    # correlation_id = struct.unpack_from('>I', buffer=data, offset=8)
    # print(f"correlation_id {correlation_id}")
    # print(f"data from client {data.decode()}")
    # conn.sendall(b"hello from the server side")
    # conn.sendall(message[0].to_bytes(4, signed=True) + correlation_id[0].to_bytes(4, signed=True) + (35).to_bytes(2, signed=True))
    conn.sendall((req_values['message_size']).to_bytes(4, signed=True) + (req_values['correlation_id']).to_bytes(4, signed=True)
                 + (0).to_bytes(2, signed=True) + (0).to_bytes(1, signed=True) + (18).to_bytes(2, signed=True)
                 + (0).to_bytes(2, signed=True) + (4).to_bytes(2, signed=True))
    conn.close()


if __name__ == "__main__":
    main()
