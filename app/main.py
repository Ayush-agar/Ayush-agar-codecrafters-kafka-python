import socket  # noqa: F401
from traceback import format_exc


def main():
    try:
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
        print(req_values)
        # correlation_id = struct.unpack_from('>I', buffer=data, offset=8)
        tag_buffer = b"\x00"
        # correlation , error code, num_api_keys, api_key, min_version, max_version, tag_buffer, throttle_time_ms, tag_buffer
        message_bytes = (req_values['correlation_id']).to_bytes(4, signed=True) + (0).to_bytes(2, signed=True) + (1).to_bytes(1, signed=True) + (18).to_bytes(2, signed=True) + (0).to_bytes(2, signed=True) + (4).to_bytes(2, signed=True) + tag_buffer + (0).to_bytes(4, signed=True) + tag_buffer
        print(message_bytes)
        print(len(message_bytes))
        print(req_values['message_size'])
        print(int.from_bytes(data[0:4], byteorder="big"))
        req_len = req_values['message_size'].to_bytes(4, byteorder="big", signed=True)
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

