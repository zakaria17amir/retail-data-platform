import json
import threading
from collections.abc import Iterator
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from lakehouse_spark.realtime import push
from lakehouse_spark.realtime.push import push_rows


class FakeFeast:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.statuses: list[int] = []

    def reply(self, *statuses: int) -> None:
        self.statuses = list(statuses)


@pytest.fixture
def feast() -> Iterator[tuple[FakeFeast, str]]:
    fake = FakeFeast()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            fake.requests.append((self.path, json.loads(body)))
            status = fake.statuses.pop(0) if fake.statuses else 200
            self.send_response(status)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield fake, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _rows(n: int) -> list[dict[str, Any]]:
    return [
        {"product_id": f"p{i}", "views_1h": i, "event_ts": datetime(2017, 6, 1, 12, 0, i % 60)}
        for i in range(n)
    ]


def test_push_posts_column_payload_in_chunks_of_500(feast: tuple[FakeFeast, str]) -> None:
    fake, url = feast
    assert push_rows(url, "popularity_push", _rows(1200)) is True
    assert [path for path, _ in fake.requests] == ["/push"] * 3
    assert [len(body["df"]["product_id"]) for _, body in fake.requests] == [500, 500, 200]
    first = fake.requests[0][1]
    assert first["push_source_name"] == "popularity_push"
    assert first["to"] == "online"
    assert first["df"]["views_1h"][:2] == [0, 1]
    assert first["df"]["event_ts"][1] == "2017-06-01T12:00:01"


def test_push_skips_nothing_on_empty_batch(feast: tuple[FakeFeast, str]) -> None:
    fake, url = feast
    assert push_rows(url, "popularity_push", []) is True
    assert fake.requests == []


def test_push_retries_server_errors_then_succeeds(feast: tuple[FakeFeast, str]) -> None:
    fake, url = feast
    fake.reply(503, 200)
    assert push_rows(url, "popularity_push", _rows(3), backoff_s=0) is True
    assert len(fake.requests) == 2


def test_push_fails_the_batch_when_server_errors_persist(feast: tuple[FakeFeast, str]) -> None:
    fake, url = feast
    fake.reply(500, 500, 500)
    with pytest.raises(OSError):
        push_rows(url, "popularity_push", _rows(3), backoff_s=0)
    assert len(fake.requests) == 3


def test_push_fails_the_batch_when_server_unreachable() -> None:
    with pytest.raises(OSError):
        push_rows("http://127.0.0.1:9", "popularity_push", _rows(1), attempts=1)


def test_bad_payload_skips_the_batch_and_counts_it(feast: tuple[FakeFeast, str]) -> None:
    fake, url = feast
    fake.reply(422)
    before = push.SKIPPED_BATCHES["session_push"]
    assert push_rows(url, "session_push", _rows(1200), backoff_s=0) is False
    assert len(fake.requests) == 1
    assert push.SKIPPED_BATCHES["session_push"] == before + 1
