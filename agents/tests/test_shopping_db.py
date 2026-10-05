import hashlib
import math
import os
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import psycopg
import pytest
from conftest import call, fake_llm
from langgraph.types import Command
from psycopg import sql
from psycopg.conninfo import make_conninfo
from shop_fakes import P1, P2, FakeSearch, fake_enrichment, recommend_client

from agents.cli import main
from agents.core.checkpoint import postgres_checkpointer
from agents.shopping_agent.db import OLIST_SCHEMA_SQL, PostgresShop, init_shop
from agents.shopping_agent.graph import build_graph
from agents.shopping_agent.tools import Session, build_registry

LATEST = datetime(2018, 9, 1, 12)
COUNT_ORDER_ROWS = """
    select (select count(*) from olist.orders where customer_id = 'cust-1'),
           (select count(*) from olist.order_items i join olist.orders o using (order_id)
             where o.customer_id = 'cust-1'),
           (select count(*) from olist.order_payments p join olist.orders o using (order_id)
             where o.customer_id = 'cust-1')
"""


def _seed(conn: psycopg.Connection[Any]) -> None:
    conn.execute(OLIST_SCHEMA_SQL.read_text(encoding="utf-8"))
    conn.execute(
        "insert into olist.customers (customer_id, customer_unique_id) values "
        "('cust-1', 'u-1'), ('cust-2', 'u-2')"
    )
    conn.execute(
        "insert into olist.product_category_name_translation values "
        "('cama_mesa_banho', 'bed_bath_table', now())"
    )
    conn.execute(
        "insert into olist.products (product_id, product_category_name) values (%s, %s), (%s, %s)",
        [P1, "cama_mesa_banho", P2, None],
    )
    old, recent = LATEST - timedelta(days=200), LATEST - timedelta(days=10)
    conn.execute(
        "insert into olist.orders (order_id, customer_id, order_status, order_purchase_timestamp)"
        " values ('o-old', 'cust-2', 'delivered', %s), ('o-new', 'cust-2', 'shipped', %s),"
        " ('o-last', 'cust-2', 'delivered', %s)",
        [old, recent, LATEST],
    )
    conn.execute(
        "insert into olist.order_items values "
        "('o-old', 1, %s, 's1', %s, 40.00, 9.00, now()),"
        "('o-new', 1, %s, 's1', %s, 49.90, 10.10, now()),"
        "('o-new', 2, %s, 's1', %s, 49.90, 10.10, now()),"
        "('o-last', 1, %s, 's1', %s, 49.90, 10.10, now())",
        [P1, old, P1, recent, P1, recent, P1, LATEST],
    )


@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    base = os.environ.get("POSTGRES_DSN")
    if not base:
        pytest.skip("POSTGRES_DSN is not set")
    try:
        admin = psycopg.connect(base, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Postgres is unreachable")
    name = f"shop_test_{uuid.uuid4().hex[:8]}"
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            url = make_conninfo(base, dbname=name)
            with psycopg.connect(url, autocommit=True) as conn:
                _seed(conn)
            init_shop(url)
            yield url
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture(scope="module")
def writer_dsn(dsn: str) -> str:
    return make_conninfo(dsn, user="shop_writer", password="shop_writer")


def _expected_on_hand(product_id: str, units_90d: int) -> int:
    b = hashlib.md5(f"shop.stock:v1:{product_id}".encode()).digest()[0]
    return 0 if b < 13 else math.ceil(units_90d * 30 / 90) + 1 + b % 10


def test_stock_is_deterministic_and_init_is_idempotent(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        before = dict(conn.execute("select product_id, on_hand from shop.stock").fetchall())
    assert before == {P1: _expected_on_hand(P1, 3), P2: _expected_on_hand(P2, 0)}
    init_shop(dsn)
    with psycopg.connect(dsn) as conn:
        after = dict(conn.execute("select product_id, on_hand from shop.stock").fetchall())
        published = conn.execute(
            "select count(*) from pg_publication_tables where schemaname = 'shop'"
        ).fetchone()
    assert after == before
    assert published == (0,)


def test_cli_shop_init(dsn: str, monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    monkeypatch.setenv("POSTGRES_DSN", dsn)
    main(["shop", "init"])
    assert "shop.stock rows: 2" in capsys.readouterr().out


def test_shop_writer_is_insert_only(writer_dsn: str) -> None:
    for statement in (
        "select * from olist.orders",
        "update olist.orders set order_status = 'x'",
        "delete from olist.order_items",
        "select * from shop.stock",
        "insert into olist.customers (customer_id) values ('evil')",
    ):
        with (
            psycopg.connect(writer_dsn) as conn,
            pytest.raises(psycopg.errors.InsufficientPrivilege),
        ):
            conn.execute(statement)


def test_postgres_shop_reads(dsn: str, writer_dsn: str) -> None:
    shop = PostgresShop(dsn, writer_dsn)
    rows = shop.products([P1, P2, "nope"])
    assert set(rows) == {P1, P2}
    assert rows[P1].category_en == "bed_bath_table" and rows[P1].price == 49.9
    assert rows[P1].freight == 10.1 and rows[P1].seller_id == "s1"
    assert rows[P2].price is None
    assert shop.dataset_now() == LATEST
    assert shop.customer_unique_id("cust-1") == "u-1" and shop.customer_unique_id("x") is None
    order = shop.order("o-new")
    assert order is not None and order.customer_unique_id == "u-2" and order.status == "shipped"
    assert shop.order("missing") is None
    assert set(shop.stock([P1, "nope"])) == {P1}


def _graph(dsn: str, writer_dsn: str, saver: Any, *replies: Any) -> Any:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("update shop.stock set on_hand = 5 where product_id = %s", [P1])
    registry = build_registry(
        PostgresShop(dsn, writer_dsn),
        Session(customer_id="cust-1", session_id="chat-1"),
        search=FakeSearch(),
        enrichment=fake_enrichment,
        http=recommend_client([]),
    )
    return build_graph(fake_llm(*replies), registry, checkpointer=saver)


def _order_rows(dsn: str) -> tuple[int, ...]:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(COUNT_ORDER_ROWS).fetchone()
    assert row is not None
    return tuple(row)


ORDER = {"items": [{"product_id": P1, "quantity": 2}], "payment_type": "boleto"}


def test_rejected_interrupt_writes_no_rows(dsn: str, writer_dsn: str) -> None:
    cfg: Any = {"configurable": {"thread_id": "reject-1"}}
    with postgres_checkpointer(dsn) as saver:
        first = _graph(dsn, writer_dsn, saver, call("place_order", ORDER)).invoke(
            {"question": "buy 2 blankets"}, cfg
        )
    assert first["__interrupt__"][0].value["quote"]["total"] == 120.0
    assert _order_rows(dsn) == (0, 0, 0)
    with postgres_checkpointer(dsn) as saver:
        out = _graph(dsn, writer_dsn, saver, "Not placed.").invoke(
            Command(resume={"approved": False}), cfg
        )
    assert out["answer"] == "Not placed."
    assert _order_rows(dsn) == (0, 0, 0)


def test_approved_interrupt_writes_a_replayer_like_order(dsn: str, writer_dsn: str) -> None:
    cfg: Any = {"configurable": {"thread_id": "approve-1"}}
    with postgres_checkpointer(dsn) as saver:
        _graph(dsn, writer_dsn, saver, call("place_order", ORDER)).invoke(
            {"question": "buy 2 blankets"}, cfg
        )
    assert _order_rows(dsn) == (0, 0, 0)
    with postgres_checkpointer(dsn) as saver:
        out = _graph(dsn, writer_dsn, saver, "Placed.").invoke(
            Command(resume={"approved": True}), cfg
        )
    order_id = out["runs"][-1]["output"]["order_id"]
    assert _order_rows(dsn) == (1, 2, 1)
    with psycopg.connect(dsn) as conn:
        order = conn.execute(
            "select order_status, order_purchase_timestamp, order_approved_at,"
            " order_estimated_delivery_date from olist.orders where order_id = %s",
            [order_id],
        ).fetchone()
        payment = conn.execute(
            "select payment_sequential, payment_type, payment_installments, payment_value"
            " from olist.order_payments where order_id = %s",
            [order_id],
        ).fetchone()
        items = conn.execute(
            "select order_item_id, product_id, seller_id, price, freight_value"
            " from olist.order_items where order_id = %s order by order_item_id",
            [order_id],
        ).fetchall()
    assert order is not None and payment is not None
    assert order[0] == "created" and order[2] is None
    assert order[1] == LATEST + timedelta(minutes=1) and order[3] > order[1]
    assert payment[:3] == (1, "boleto", 1) and float(payment[3]) == 120.0
    assert [(i[0], i[1], i[2], float(i[3])) for i in items] == [
        (1, P1, "s1", 49.9),
        (2, P1, "s1", 49.9),
    ]
