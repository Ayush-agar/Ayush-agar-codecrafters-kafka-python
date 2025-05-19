import socket  # noqa: F401
from traceback import format_exc


def main():
    print("Logs from your program will appear here!")
    import struct
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        try:
            conn, raddr = server.accept() # wait for client
            message_len = int.from_bytes(conn.recv(4), byteorder="big", signed=True)
            print(message_len)
            data = conn.recv(message_len + 8)
            # data = conn.recv(12)
            print(f"data {data}")
            # print(f"data {conn.recv(1024)}")
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
            print(req_values)
            request_api_key = int.from_bytes(data[:2], byteorder="big", signed=True)
            request_api_version = int.from_bytes(data[2:4], byteorder="big", signed=True)
            correlation_id = int.from_bytes(data[4:8], byteorder="big", signed=True)
            client_id = bytes.decode(data[8:], "utf-8")
            # correlation_id = struct.unpack_from('>I', buffer=data, offset=8)
            tag_buffer = b"\x00"
            # correlation , error code, num_api_keys, api_key, min_version, max_version, tag_buffer, throttle_time_ms, tag_buffer
            message_bytes = correlation_id.to_bytes(4, signed=True)
            message_bytes += int(0).to_bytes(2, signed=True)
            message_bytes += int(2).to_bytes(1, signed=True)
            message_bytes += int(request_api_key).to_bytes(2, signed=True)
            message_bytes += int(0).to_bytes(2, signed=True)
            message_bytes += int(4).to_bytes(2, signed=True)
            message_bytes += tag_buffer
            message_bytes += int(0).to_bytes(4, signed=True)
            message_bytes += tag_buffer
            print(message_bytes)
            print(len(message_bytes))
            print(req_values['message_size'])
            print(int.from_bytes(data[0:4], byteorder="big"))
            req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=True)
            response = req_len + message_bytes
            print(response)
            conn.sendall(response)
            # conn.close()
        except Exception as e:
            import traceback
            tb = format_exc()
            print(tb)
            print(f"error {e}")


if __name__ == "__main__":
    main()

