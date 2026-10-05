import re
from collections.abc import Iterable
from dataclasses import dataclass, field

_EDGE_L, _EDGE_R = r"(?<![\w.@])", r"(?![\w@]|\.\d)"
PII_PATTERNS = {
    "cpf": re.compile(_EDGE_L + r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}" + _EDGE_R),
    "email": re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"),
    "phone": re.compile(
        _EDGE_L + r"(?:\+?55[\s-]?)?(?:\(\d{2}\)|\d{2})[\s-]?9?\d{4}[\s-]?\d{4}" + _EDGE_R
    ),
    "card": re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)"),
}
INJECTION = re.compile(
    r"ignore\s+(?:all\s+|the\s+|any\s+)?(?:previous|prior|above|earlier)\s+instructions"
    r"|disregard\s+(?:all\s+|the\s+|any\s+)?(?:previous|prior|above|earlier)?\s*instructions"
    r"|ignore\s+(?:as\s+|todas\s+as\s+)?instru[cç][oõ]es"
    r"|system\s+prompt|you\s+are\s+now|developer\s+mode|jailbreak"
    r"|</?\s*(?:data|system|instructions?)\s*>",
    re.IGNORECASE,
)
_NUMBER = re.compile(
    r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?:\s*%|\s*(?:million|billion|thousand|mn|bn|[kKmM])\b)?"
)
_SCALE = {"thousand": 1e3, "k": 1e3, "million": 1e6, "mn": 1e6, "m": 1e6, "billion": 1e9, "bn": 1e9}
SMALL_INT = 10
TOLERANCE = 0.005


@dataclass(frozen=True)
class GuardResult:
    ok: bool
    reasons: list[str] = field(default_factory=list)


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch) * (2 if i % 2 else 1)
        total += d - 9 if d > 9 else d
    return total % 10 == 0


def find_pii(text: str) -> list[str]:
    found = []
    for kind, pattern in PII_PATTERNS.items():
        for match in pattern.finditer(text):
            if kind == "card" and not _luhn(re.sub(r"\D", "", match.group())):
                continue
            found.append(kind)
            break
    return found


def check_input(text: str) -> GuardResult:
    reasons = [f"pii:{kind}" for kind in find_pii(text)]
    if INJECTION.search(text):
        reasons.append("injection")
    return GuardResult(not reasons, reasons)


def wrap_untrusted(text: str) -> str:
    """Delimit tool/retrieved content; it is data, never instructions."""
    neutral = re.sub(r"</?\s*data\s*>", "[data-tag]", text, flags=re.IGNORECASE)
    return f"<data>\n{neutral}\n</data>"


def _numbers(text: str) -> list[tuple[str, float, int]]:
    """(token, value, decimals) for every number; % is a fraction, 'million' scales."""
    out = []
    for match in _NUMBER.finditer(text):
        token = match.group().strip()
        number = re.match(r"-?[\d,]*\d(?:\.\d+)?", token.replace(" ", ""))
        if number is None:
            continue
        raw = number.group().replace(",", "")
        value = float(raw)
        decimals = len(raw.split(".")[1]) if "." in raw else 0
        suffix = token[number.end() :].strip().lower()
        if suffix == "%":
            value, decimals = value / 100, decimals + 2
        elif suffix in _SCALE:
            value, decimals = value * _SCALE[suffix], 0
        out.append((number.group(), value, decimals))
    return out


def _grounded(value: float, decimals: int, sources: list[float]) -> bool:
    for s in sources:
        if round(s, decimals) == round(value, decimals):
            return True
        if s and abs(value - s) / abs(s) <= TOLERANCE:
            return True
    return False


def check_output(answer: str, sources: Iterable[str]) -> GuardResult:
    """No PII echo; every number in the answer must come from the sources (tools, question)."""
    reasons = [f"pii:{kind}" for kind in find_pii(answer)]
    known = [v for text in sources for _, v, _ in _numbers(text)]
    for token, value, decimals in _numbers(answer):
        is_small_int = value.is_integer() and abs(value) <= SMALL_INT
        if not is_small_int and not _grounded(value, decimals, known):
            reasons.append(f"ungrounded:{token}")
    return GuardResult(not reasons, reasons)
