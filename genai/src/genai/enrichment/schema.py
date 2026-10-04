"""Output contract for `silver/product_enriched` and its validation."""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic_core import PydanticCustomError

_EN = "the and of to in for with is it this that on as be are by from or an at your you its can"
_EN_MORE = "will has have was not but all our more which their they"
_PT = "de da do das dos em para com que o os as um uma no na nos nas e muito por mais ao se"
EN_STOPWORDS = frozenset(f"{_EN} {_EN_MORE}".split(" "))
PT_STOPWORDS = frozenset(_PT.split(" "))
MIN_EN_RATIO = 0.15
WORD = re.compile(r"[^\W\d_]+")


def _ascii(value: str) -> str:
    if not value.isascii():
        raise PydanticCustomError("not_ascii", "must be plain ASCII English text")
    return value


def _english(value: str) -> str:
    words = [w.lower() for w in WORD.findall(value)]
    en = sum(w in EN_STOPWORDS for w in words)
    pt = sum(w in PT_STOPWORDS for w in words)
    if en < MIN_EN_RATIO * len(words) or pt >= en:
        raise PydanticCustomError("not_english", "must be written in English")
    return value


ASCII = AfterValidator(_ascii)


class Enrichment(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    title: Annotated[str, Field(min_length=1, max_length=80), ASCII]
    description: Annotated[
        str, Field(min_length=40, max_length=600), ASCII, AfterValidator(_english)
    ]
    tags: list[Annotated[str, Field(min_length=1, max_length=40), ASCII]] = Field(
        min_length=3, max_length=8
    )
    language: Literal["en"]

    @field_validator("tags")
    @classmethod
    def _lowercase(cls, tags: list[str]) -> list[str]:
        return [t.lower() for t in tags]


def json_schema() -> dict[str, Any]:
    return Enrichment.model_json_schema()


def describe_error(err: ValidationError) -> tuple[str, str]:
    """(rule_id, reason) of the first error, e.g. ('tags:too_short', 'tags: List should ...')."""
    first = err.errors()[0]
    loc = ".".join(str(p) for p in first["loc"])
    rule_id = f"{loc}:{first['type']}" if loc else first["type"]
    reasons = "; ".join(
        f"{'.'.join(str(p) for p in e['loc']) or 'output'}: {e['msg']}" for e in err.errors()
    )
    return rule_id, reasons
