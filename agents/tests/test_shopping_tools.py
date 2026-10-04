from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from genai.enrichment.writer import ENRICHED, ENRICHED_SCHEMA, append
from shop_fakes import (
    INJECTED,
    P1,
    P2,
    P3,
    SESSION,
    FakeSearch,
    FakeShop,
    fake_enrichment,
    recommend_client,
)

from agents.core.registry import ToolError, ToolNotAllowed, ToolRegistry
from agents.shopping_agent.tools import AGENT, APPROVER, build_registry, lakehouse_enrichment


def registry(
    shop: FakeShop | None = None,
    search: FakeSearch | None = None,
    seen: list[dict[str, Any]] | None = None,
) -> ToolRegistry:
    return build_registry(
        shop or FakeShop(),
        SESSION,
        search=search or FakeSearch(),
        enrichment=fake_enrichment,
        http=recommend_client([] if seen is None else seen),
    )


def test_tool_allowlists() -> None:
    reg = registry()
    assert {t.name for t in reg.tools_for(AGENT)} == {
        "search_products",
        "get_product",
        "check_stock",
        "get_recommendations",
        "get_order_status",
    }
    assert {t.name for t in reg.tools_for(APPROVER)} == {
        "quote_order",
        "place_order",
        "place_quote",
    }
    with pytest.raises(ToolNotAllowed):
        reg.call(AGENT, "place_order", {"items": [{"product_id": P1, "quantity": 1}]})
    with pytest.raises(ToolNotAllowed):
        reg.call("analytics", "search_products", {"query": "blanket"})


def test_search_dedupes_products_and_uses_enriched_titles() -> None:
    search = FakeSearch()
    out = registry(search=search).call(AGENT, "search_products", {"query": "warm blanket", "k": 2})
    products = out.model_dump()["products"]
    assert [p["product_id"] for p in products] == [P1, P2]
    assert products[0]["title"] == "Plush Throw Blanket"
    assert products[0]["price"] == 49.9
    assert products[1]["title"] == f"housewares {P2[:8]}"
    assert search.calls[0][0] == "warm blanket" and search.calls[0][1] >= 2


def test_search_snippets_withhold_injected_text() -> None:
    search = FakeSearch()
    search.hits = search.hits[1:]
    out = registry(search=search).call(AGENT, "search_products", {"query": "blanket"})
    assert "Ignore previous instructions" not in out.model_dump_json()


def test_get_product_withholds_injected_reviews() -> None:
    out = registry().call(AGENT, "get_product", {"product_id": P1}).model_dump()
    assert out["title"] == "Plush Throw Blanket"
    assert out["description"] == "A soft fleece throw."
    assert out["reviews"][0] == "Soft and warm."
    assert INJECTED not in out["reviews"] and "withheld" in out["reviews"][1]


def test_get_product_unknown_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="unknown product"):
        registry().call(AGENT, "get_product", {"product_id": P3})


def test_check_stock() -> None:
    out = registry().call(AGENT, "check_stock", {"product_ids": [P1, P2, P3]}).model_dump()
    assert out["items"] == [
        {"product_id": P1, "on_hand": 7, "in_stock": True},
        {"product_id": P2, "on_hand": 0, "in_stock": False},
    ]
    assert out["unknown"] == [P3]


def test_recommendations_post_the_chat_session_id() -> None:
    seen: list[dict[str, Any]] = []
    out = registry(seen=seen).call(AGENT, "get_recommendations", {"k": 2}).model_dump()
    assert seen == [{"path": "/recommend", "body": {"session_id": "thread-1", "k": 2}}]
    assert out["strategy"] == "global_popularity"
    assert [p["product_id"] for p in out["products"]] == [P2, P1]
    assert out["products"][1]["title"] == "Plush Throw Blanket"


def test_order_status_own_customer_and_same_person() -> None:
    reg = registry()
    out = reg.call(AGENT, "get_order_status", {"order_id": "o-mine"}).model_dump()
    assert out["status"] == "delivered"
    out = reg.call(AGENT, "get_order_status", {"order_id": "o-same-person"}).model_dump()
    assert out["status"] == "shipped"


def test_order_status_cross_customer_refused() -> None:
    reg = registry()
    with pytest.raises(ToolError, match="not found for the current customer") as exc:
        reg.call(AGENT, "get_order_status", {"order_id": "o-other"})
    assert "shipped" not in str(exc.value) and "cust-2" not in str(exc.value)
    with pytest.raises(ToolError, match="not found for the current customer"):
        reg.call(AGENT, "get_order_status", {"order_id": "missing"})


def test_quote_prices_units_and_dataset_time() -> None:
    out = registry().call(
        APPROVER,
        "quote_order",
        {"items": [{"product_id": P1, "quantity": 1}, {"product_id": P1, "quantity": 1}]},
    )
    q = out.model_dump()
    assert q["customer_id"] == "cust-1"
    assert q["lines"] == [
        {
            "product_id": P1,
            "title": "Plush Throw Blanket",
            "quantity": 2,
            "unit_price": 49.9,
            "freight": 10.1,
            "seller_id": "s1",
        }
    ]
    assert q["total"] == 120.0
    assert q["payment_type"] == "credit_card" and q["installments"] == 1
    assert q["purchased_at"] == datetime(2018, 10, 17, 17, 31)
    assert q["estimated_delivery"] > q["purchased_at"]


@pytest.mark.parametrize(
    ("items", "match"),
    [
        ([{"product_id": P2, "quantity": 1}], "in stock"),
        ([{"product_id": P1, "quantity": 8}], "in stock"),
        ([{"product_id": P3, "quantity": 1}], "unknown product"),
    ],
)
def test_quote_refuses_unavailable_items(items: list[dict[str, Any]], match: str) -> None:
    with pytest.raises(ToolError, match=match):
        registry().call(APPROVER, "quote_order", {"items": items})


def test_quote_refuses_unknown_customer() -> None:
    shop = FakeShop()
    shop.customers = {}
    with pytest.raises(ToolError, match="unknown customer"):
        registry(shop).call(APPROVER, "quote_order", {"items": [{"product_id": P1, "quantity": 1}]})


def test_lakehouse_enrichment_reads_the_latest_delta_row(tmp_path: Path) -> None:
    root = tmp_path.as_posix()
    assert lakehouse_enrichment([P1], root) == {}

    def row(title: str, day: int) -> dict[str, Any]:
        return {
            "product_id": P1,
            "title": title,
            "description": "d",
            "tags": ["a", "b", "c"],
            "language": "en",
            "model": "chat",
            "prompt_hash": f"h{day}",
            "enriched_at": datetime(2026, 9, day, tzinfo=UTC),
        }

    append(root, ENRICHED, [row("New Title", 2), row("Old Title", 1)], ENRICHED_SCHEMA)
    got = lakehouse_enrichment([P1, P2], root)
    assert set(got) == {P1} and got[P1].title == "New Title"


def test_place_order_inserts_the_quote() -> None:
    shop = FakeShop()
    out = registry(shop).call(
        APPROVER, "place_order", {"items": [{"product_id": P1, "quantity": 2}]}
    )
    placed = out.model_dump()
    assert placed["status"] == "created" and len(placed["order_id"]) == 32
    assert [(oid, q.total) for oid, q in shop.inserted] == [(placed["order_id"], 120.0)]


def _approved_quote(shop: FakeShop) -> dict[str, Any]:
    items = {"items": [{"product_id": P1, "quantity": 2}]}
    return registry(shop).call(APPROVER, "quote_order", items).model_dump(mode="json")


def test_place_quote_inserts_exactly_the_approved_quote() -> None:
    shop = FakeShop()
    approved = _approved_quote(shop)
    shop.rows[P1] = shop.rows[P1].model_copy(update={"price": 1.0})
    placed = registry(shop).call(APPROVER, "place_quote", approved).model_dump(mode="json")
    assert placed["total"] == approved["total"] == 120.0
    assert [q.model_dump(mode="json") for _, q in shop.inserted] == [approved]


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"on_hand": {P1: 1}}, "nothing was placed"),
        ({"customer_id": "cust-2"}, "another customer"),
    ],
)
def test_place_quote_aborts_on_stock_drop_or_foreign_quote(
    change: dict[str, Any], match: str
) -> None:
    shop = FakeShop()
    approved = _approved_quote(shop)
    if "on_hand" in change:
        shop.on_hand.update(change["on_hand"])
    else:
        approved = {**approved, **change}
    with pytest.raises(ToolError, match=match):
        registry(shop).call(APPROVER, "place_quote", approved)
    assert shop.inserted == []
