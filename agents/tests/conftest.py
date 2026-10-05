import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatResult

FIXTURES = Path(__file__).parent / "fixtures"


class FakeLLM(GenericFakeChatModel):
    """Scripted chat model; records every prompt it receives."""

    seen: list[list[BaseMessage]] = []

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> "FakeLLM":
        return self

    def _generate(self, messages: list[BaseMessage], *args: Any, **kwargs: Any) -> ChatResult:
        self.seen.append(list(messages))
        return super()._generate(messages, *args, **kwargs)


def fake_llm(*replies: AIMessage | str) -> FakeLLM:
    return FakeLLM(messages=iter(replies), seen=[])


def call(name: str, args: dict[str, Any], call_id: str = "c1") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id}])


@pytest.fixture
def gold_dir(tmp_path: Path) -> Path:
    gold = tmp_path / "gold"
    gold.mkdir()
    con = duckdb.connect()
    con.execute(
        """
        create table fct_orders as select * from (values
          ('o1', date '2017-01-05', 'delivered', true, true, false),
          ('o2', date '2017-02-10', 'delivered', true, true, true),
          ('o3', date '2018-03-01', 'canceled', false, false, false),
          ('o4', date '2018-04-02', 'shipped', true, false, false)
        ) t(order_id, order_purchase_date, order_status, is_revenue_order, is_delivered, is_late)
        """
    )
    con.execute(
        """
        create table fct_order_items as select * from (values
          ('o1', date '2017-01-05', true, 100.0),
          ('o1', date '2017-01-05', true, 50.0),
          ('o2', date '2017-02-10', true, 30.0),
          ('o3', date '2018-03-01', false, 999.0),
          ('o4', date '2018-04-02', true, 20.0)
        ) t(order_id, order_purchase_date, is_revenue_order, item_revenue)
        """
    )
    con.execute("create table big as select range as x from range(20000)")
    for table in ("fct_orders", "fct_order_items", "big"):
        con.execute(f"copy {table} to '{(gold / table).as_posix()}.parquet' (format parquet)")
    con.close()
    return gold


@pytest.fixture
def dbt_target(tmp_path: Path) -> Path:
    target = tmp_path / "analytics" / "target"
    target.mkdir(parents=True)
    (target / "semantic_manifest.json").write_bytes(
        (FIXTURES / "semantic_manifest.json").read_bytes()
    )
    manifest = {
        "nodes": {
            "model.retail.fct_orders": {
                "resource_type": "model",
                "name": "fct_orders",
                "description": "One row per order.",
                "relation_name": '"retail"."main_marts"."fct_orders"',
                "columns": {
                    "order_id": {"name": "order_id", "description": "Order key."},
                    "order_status": {"name": "order_status", "description": "Status."},
                },
            },
            "test.retail.not_null_x": {"resource_type": "test", "name": "not_null_x"},
        }
    }
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return target


@pytest.fixture
def chart_dir(tmp_path: Path) -> Path:
    return tmp_path / "charts"
