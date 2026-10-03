import json
from typing import Protocol
from urllib.error import HTTPError
from urllib.request import urlopen

TIMEOUT_SECONDS = 10


class SchemaSource(Protocol):
    def get(self, schema_id: int) -> str: ...


class SchemaNotFound(Exception):
    pass


class SchemaRegistry:
    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._cache: dict[int, str] = {}

    def get(self, schema_id: int) -> str:
        if schema_id in self._cache:
            return self._cache[schema_id]
        try:
            with urlopen(  # noqa: S310
                f"{self._base_url}/schemas/ids/{schema_id}", timeout=TIMEOUT_SECONDS
            ) as response:
                payload = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                raise SchemaNotFound(f"schema id {schema_id}") from error
            raise
        schema = str(payload["schema"])
        self._cache[schema_id] = schema
        return schema
