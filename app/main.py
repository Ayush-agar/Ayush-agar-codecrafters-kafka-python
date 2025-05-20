import socket
import threading
from ast import parse

NO_ERROR = 0
class RequestValidationException(Exception):
    code: int
    message: str
class ApiVersionInvalidException(RequestValidationException):
    code = 35
    message = "UNSUPPORTED_VERSION"
class Request:
    request_api_key: int
    request_api_version: int
    correlation_id: int
    client_id_len: int | None
    tagged_fields: list[str] | None
    def __init__(
        self,
        request_api_key: int,
        request_api_version: int,
        correlation_id: int,
        client_id_len: int,
    ):
        self.request_api_key = request_api_key
        self.request_api_version = request_api_version
        self.correlation_id = correlation_id
        self.client_id_len = client_id_len
    def validate(self):
        if self.request_api_version not in [0, 1, 2, 3, 4]:
            raise ApiVersionInvalidException
def parse_request_length(header: bytes) -> int:
    return int.from_bytes(header, byteorder="big", signed=True)

def parse_request(request: bytes) -> Request:
    request_api_key = int.from_bytes(request[:2], byteorder="big", signed=True)
    request_api_version = int.from_bytes(request[2:4], byteorder="big", signed=True)
    correlation_id = int.from_bytes(request[4:8], byteorder="big", signed=True)
    client_id_len = int.from_bytes(request[8:10], byteorder="big")
    print("request_api_key, request_api_version, correlation_id, client_id_len ====== ")
    print(request_api_key, request_api_version, correlation_id, client_id_len)
    return Request(request_api_key, request_api_version, correlation_id, client_id_len)

def create_response_75(request_bytes):
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    parsed_request = parse_request(request_bytes)
    try:
        parsed_request.validate()
        error_bytes = NO_ERROR.to_bytes(2, byteorder="big", signed=True)
    except RequestValidationException as ex:
        error_bytes = ex.code.to_bytes(2, byteorder="big", signed=True)
    client_id_len = parsed_request.client_id_len
    if client_id_len > 0:
        client_id = request_bytes[10:10 + client_id_len].decode('utf-8', errors='ignore')
        tagged = request_bytes[10 + client_id_len]
    else:
        client_id = ""
        tagged = request_bytes[10]
    array_len_finder = 10 + client_id_len + 1
    array_length = request_bytes[array_len_finder]
    topic_name_length = request_bytes[array_len_finder + 1]
    topic_name_starter = array_len_finder + 2
    topic_name = bytes(request_bytes[topic_name_starter:topic_name_starter + (topic_name_length - 1)])
    cursor_length = topic_name_starter + topic_name_length + 4
    cursor = request_bytes[cursor_length]
    cursor_bytes = int(cursor).to_bytes(1, byteorder="big")
    print(f"P_DTP_R: array_length={array_length}, topic_name_length={topic_name_length}, "
          f"topic_name={topic_name.decode('utf-8', errors='ignore')}, cursor={cursor}")
    response_header = parsed_request.correlation_id.to_bytes(4, byteorder="big") + tag_buffer
    error_code = int(3).to_bytes(2, byteorder="big")
    throttle_time_ms = int(0).to_bytes(4, byteorder="big")
    is_internal = int(0).to_bytes(1, byteorder="big")
    topic_authorized_operations = b"\x00\x00\x0d\xf8"
    topic_id = int(0).to_bytes(16, byteorder="big")
    partition_array = b"\x01"
    response_body = (
        throttle_time_ms
        + int(array_length).to_bytes(1, byteorder="big")
        + error_code
        + int(topic_name_length).to_bytes(1, byteorder="big")
        + topic_name
        + topic_id
        + is_internal
        + partition_array
        + topic_authorized_operations
        + tag_buffer
        + cursor_bytes
        + tag_buffer # Response tag buffer
    )
    total_len = len(response_header) + len(response_body)
    return int(total_len).to_bytes(4, byteorder="big") + response_header + response_body


def create_response_18(request: Request) -> bytes:
    message_bytes = request.correlation_id.to_bytes(4, byteorder="big", signed=True)
    min_version_18, max_version_18 = 0, 4
    min_version_75, max_version_75 = 0, 0
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    try:
        request.validate()
        error_bytes = NO_ERROR.to_bytes(2, byteorder="big", signed=True)
    except RequestValidationException as ex:
        error_bytes = ex.code.to_bytes(2, byteorder="big", signed=True)
    message_bytes += error_bytes
    message_bytes += int(3).to_bytes(1, byteorder="big", signed=True) # no. of entries + 1 = 3
    message_bytes += request.request_api_key.to_bytes(2, byteorder="big", signed=True)
    message_bytes += min_version_18.to_bytes(2, byteorder="big", signed=True)
    message_bytes += max_version_18.to_bytes(2, byteorder="big", signed=True)
    message_bytes += tag_buffer
    message_bytes += int(75).to_bytes(2, byteorder="big", signed=True)
    message_bytes += min_version_75.to_bytes(2, byteorder="big", signed=True)
    message_bytes += max_version_75.to_bytes(2, byteorder="big", signed=True)
    message_bytes += tag_buffer
    message_bytes += throttle_time_ms.to_bytes(4, byteorder="big", signed=True)
    message_bytes += tag_buffer
    req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=True)
    response = req_len + message_bytes
    return response
def handler(client_conn, addr):
    while True:
        message_len = parse_request_length(client_conn.recv(4))
        print(f"message_len {message_len}")
        request_bytes = client_conn.recv(message_len + 8)
        print(f"request_bytes are {request_bytes}")
        print(int.from_bytes(request_bytes, byteorder="big", signed=True))
        # create response
        parsed_req = parse_request(request_bytes)
        if parsed_req.request_api_key == 18:
            response = create_response_18(parsed_req)
            # send response
            client_conn.sendall(response)
        if parsed_req.request_api_key == 75:
            response = create_response_75(request_bytes)
            # send response
            client_conn.sendall(response)

def main():
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        # client_conn, addr = server.accept()
        while True:
            client_conn, addr = server.accept()
            # receive
            thread = threading.Thread(target=handler, args=(client_conn, addr), daemon=True)
            thread.start()

if __name__ == "__main__":
    main()

