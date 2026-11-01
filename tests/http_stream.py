"""Build in-memory HTTP responses with real HTTP parsing and length checks."""

from http.client import HTTPResponse
from io import BytesIO
from unittest.mock import Mock

from urllib3.response import HTTPResponse as Urllib3HTTPResponse


def make_http_stream(content: bytes, content_length: int) -> Urllib3HTTPResponse:
    """Simulate a server response whose declared length may exceed its body."""

    message = (
        f"HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii") + content
    socket = Mock()
    socket.makefile.return_value = BytesIO(message)
    response = HTTPResponse(socket)
    response.begin()

    return Urllib3HTTPResponse(
        body=response,
        headers=dict(response.getheaders()),
        status=response.status,
        original_response=response,
        preload_content=False,
        decode_content=False,
    )
