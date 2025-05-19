import socket  # noqa: F401
from traceback import format_exc


def main():
    print("Logs from your program will appear here!")
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        while True:
            conn, raddr = server.accept() # wait for client
            message_len = int.from_bytes(conn.recv(4), byteorder="big", signed=True)
            print(message_len)
            data = conn.recv(message_len + 8)
            print(f"data {data}")
            request_api_key = int.from_bytes(data[:2], byteorder="big", signed=True)
            request_api_version = int.from_bytes(data[2:4], byteorder="big", signed=True)
            correlation_id = int.from_bytes(data[4:8], byteorder="big", signed=True)
            print("correlation_id, request_api_key====")
            print(correlation_id, request_api_key)
            client_id = bytes.decode(data[8:], "utf-8")
            tag_buffer = b"\x00"
            # correlation , error code, num_api_keys, api_key, min_version, max_version, tag_buffer, throttle_time_ms, tag_buffer
            message_bytes = correlation_id.to_bytes(4, byteorder="big", signed=True)
            message_bytes += int(0).to_bytes(2, byteorder="big", signed=True)
            message_bytes += int(2).to_bytes(1, byteorder="big", signed=True)
            message_bytes += int(request_api_key).to_bytes(2, byteorder="big", signed=True)
            message_bytes += int(0).to_bytes(2, byteorder="big", signed=True)
            message_bytes += int(4).to_bytes(2, byteorder="big", signed=True)
            message_bytes += tag_buffer
            message_bytes += int(0).to_bytes(4, byteorder="big", signed=True)
            message_bytes += tag_buffer
            req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=True)
            response = req_len + message_bytes
            conn.sendall(response)
            # conn.close()

if __name__ == "__main__":
    main()

