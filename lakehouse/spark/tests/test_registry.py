import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError

import pytest
from lakehouse_spark.registry import SchemaNotFound, SchemaRegistry

SCHEMA = '{"type":"record","name":"R","fields":[]}'


@pytest.fixture
def server() -> Iterator[tuple[str, dict[str, int]]]:
    hits = {"count": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            hits["count"] += 1
            if self.path == "/schemas/ids/1":
                body = json.dumps({"schema": SCHEMA}).encode()
                self.send_response(200)
            elif self.path == "/schemas/ids/3":
                body = b"{}"
                self.send_response(500)
            else:
                body = b'{"error_code":40403}'
                self.send_response(404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", hits
    httpd.shutdown()


def test_get_returns_schema_and_caches(server: tuple[str, dict[str, int]]) -> None:
    url, hits = server
    registry = SchemaRegistry(url)
    assert registry.get(1) == SCHEMA
    assert registry.get(1) == SCHEMA
    assert hits["count"] == 1


def test_404_raises_schema_not_found(server: tuple[str, dict[str, int]]) -> None:
    registry = SchemaRegistry(server[0])
    with pytest.raises(SchemaNotFound):
        registry.get(2)


def test_other_http_errors_propagate_unchanged(server: tuple[str, dict[str, int]]) -> None:
    registry = SchemaRegistry(server[0])
    with pytest.raises(HTTPError) as error:
        registry.get(3)
    assert error.value.code == 500


def test_connection_refused_is_not_schema_not_found() -> None:
    registry = SchemaRegistry("http://127.0.0.1:1")
    with pytest.raises(URLError) as error:
        registry.get(1)
    assert not isinstance(error.value, SchemaNotFound)
