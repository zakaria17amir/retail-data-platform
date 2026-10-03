MAGIC = 0
HEADER_LENGTH = 5


def parse_wire_header(value: bytes | None) -> int | None:
    if value is None or len(value) < HEADER_LENGTH or value[0] != MAGIC:
        return None
    return int.from_bytes(value[1:HEADER_LENGTH], "big")
