import pytest
from lakehouse_spark.wire import parse_wire_header


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (b"", None),
        (b"\x00\x00\x00", None),
        (b"\x01\x00\x00\x00\x07abc", None),
        (b"\x00\x00\x00\x00\x07abc", 7),
        (b"\x00\x00\x00\x01\x00", 256),
    ],
)
def test_parse_wire_header(value: bytes | None, expected: int | None) -> None:
    assert parse_wire_header(value) == expected
