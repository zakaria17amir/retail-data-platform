"""Golden sets. Analytics expected answers are computed from gold by the agent's own tools
(MetricFlow + read-only DuckDB), never typed by hand or produced by an LLM."""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agents.analytics_agent.tools import AGENT
from agents.core.registry import ToolRegistry
from agents.evals.scoring import TOLERANCE

DATA_DIR = Path(__file__).resolve().parents[3] / "evals" / "data"
ANALYTICS_GOLDEN = DATA_DIR / "analytics_golden.jsonl"
SHOPPING_GOLDEN = DATA_DIR / "shopping_golden.jsonl"
SHOPPING_TOOLS = frozenset(
    {
        "search_products",
        "get_product",
        "check_stock",
        "get_recommendations",
        "get_order_status",
        "place_order",
    }
)
_GRAIN_CHARS = {"__year": 4, "__quarter": 7, "__month": 7, "__week": 10, "__day": 10}


@dataclass(frozen=True)
class Spec:
    question: str
    category: str
    tool: str
    args: dict[str, Any] = field(default_factory=dict)


def _m(
    question: str,
    category: str,
    metrics: list[str],
    group_by: Sequence[str] = (),
    where: Sequence[str] = (),
    order_by: Sequence[str] = (),
    limit: int | None = None,
) -> Spec:
    args: dict[str, Any] = {"metrics": metrics}
    for key, value in (("group_by", group_by), ("where", where), ("order_by", order_by)):
        if value:
            args[key] = list(value)
    if limit:
        args["limit"] = limit
    return Spec(question, category, "query_metric", args)


def _s(question: str, category: str, sql: str) -> Spec:
    return Spec(question, category, "run_sql", {"sql": " ".join(sql.split())})


def _between(start: str, end: str) -> list[str]:
    day = "{{ TimeDimension('metric_time', 'day') }}"
    return [f"{day} >= '{start}'", f"{day} < '{end}'"]


def _status(status: str) -> str:
    return f"{{{{ Dimension('order__order_status') }}}} = '{status}'"


Y17, Y18 = _between("2017-01-01", "2018-01-01"), _between("2018-01-01", "2019-01-01")
YEAR, QUARTER, MONTH, DAY = (f"metric_time__{g}" for g in ("year", "quarter", "month", "day"))
STATUS = "order__order_status"
REVENUE_ITEMS = "fct_order_items i join dim_product p using (product_sk) where i.is_revenue_order"

SPECS: list[Spec] = [
    # metric by time grain
    _m("What was total revenue by year?", "time_grain", ["revenue"], [YEAR], order_by=[YEAR]),
    _m("How many orders were placed each year?", "time_grain", ["orders"], [YEAR]),
    _m("What was the average order value by year?", "time_grain", ["aov"], [YEAR]),
    _m("Show monthly revenue for 2017.", "time_grain", ["revenue"], [MONTH], Y17),
    _m("Show the number of orders per month in 2018.", "time_grain", ["orders"], [MONTH], Y18),
    _m("What was revenue by quarter in 2017?", "time_grain", ["revenue"], [QUARTER], Y17),
    _m("How many delivered orders were there per quarter in 2018?", "time_grain",
       ["delivered_orders"], [QUARTER], Y18),
    _m("What was the late delivery rate by year?", "time_grain", ["late_delivery_rate"], [YEAR]),
    _m("How many late deliveries were there each year?", "time_grain",
       ["late_delivered_orders"], [YEAR]),
    _m("What was the monthly average order value in 2018?", "time_grain", ["aov"], [MONTH], Y18),
    _m("Show revenue per quarter for 2018.", "time_grain", ["revenue"], [QUARTER], Y18),
    _m("How many delivered orders were there each year?", "time_grain",
       ["delivered_orders"], [YEAR]),
    # metric by dimension
    _m("What was total revenue by order status?", "by_dimension", ["revenue"], [STATUS]),
    _m("How many revenue orders are there by order status?", "by_dimension", ["orders"], [STATUS]),
    _m("What is the average order value by order status?", "by_dimension", ["aov"], [STATUS]),
    _m("What was revenue by order status in 2018?", "by_dimension", ["revenue"], [STATUS], Y18),
    _m("How many orders were there by order status in 2017?", "by_dimension",
       ["orders"], [STATUS], Y17),
    _m("Show revenue by year and order status.", "by_dimension", ["revenue"], [YEAR, STATUS]),
    # totals
    _m("What is the total revenue across all time?", "total", ["revenue"]),
    _m("How many orders are there in total?", "total", ["orders"]),
    _m("What is the overall average order value?", "total", ["aov"]),
    _m("What is the overall late delivery rate?", "total", ["late_delivery_rate"]),
    _m("How many orders have been delivered in total?", "total", ["delivered_orders"]),
    _m("How many orders were delivered late in total?", "total", ["late_delivered_orders"]),
    # filters
    _m("What was revenue in 2018?", "filter", ["revenue"], where=Y18),
    _m("How many orders were placed in November 2017?", "filter", ["orders"],
       where=_between("2017-11-01", "2017-12-01")),
    _m("What was the average order value of delivered orders?", "filter", ["aov"],
       where=[_status("delivered")]),
    _m("What was revenue from delivered orders in 2017?", "filter", ["revenue"],
       where=[*Y17, _status("delivered")]),
    _m("What was the late delivery rate in 2017?", "filter", ["late_delivery_rate"], where=Y17),
    _m("How many orders were placed in the first quarter of 2018?", "filter", ["orders"],
       where=_between("2018-01-01", "2018-04-01")),
    _m("What was revenue on Black Friday 2017 (2017-11-24)?", "filter", ["revenue"],
       where=_between("2017-11-24", "2017-11-25")),
    _m("How many orders currently have the status shipped?", "filter", ["orders"],
       where=[_status("shipped")]),
    # top-N
    _m("Which 5 months had the highest revenue?", "top_n", ["revenue"], [MONTH],
       order_by=["-revenue"], limit=5),
    _m("Which 3 days had the most orders?", "top_n", ["orders"], [DAY],
       order_by=["-orders"], limit=3),
    _s("What are the top 5 product categories (English names) by revenue?", "top_n",
       f"select p.product_category_name_english as category, sum(i.item_revenue) as revenue"
       f" from {REVENUE_ITEMS} group by 1 order by 2 desc limit 5"),
    _s("Which 5 customer states placed the most orders?", "top_n",
       "select customer_state, sum(orders) as orders from rpt_daily_sales"
       " group by 1 order by 2 desc limit 5"),
    _s("Which 5 customer states generated the most revenue?", "top_n",
       "select customer_state, sum(revenue) as revenue from rpt_daily_sales"
       " group by 1 order by 2 desc limit 5"),
    _s("What are the top 3 payment types by total payment value?", "top_n",
       "select payment_type, sum(payment_value) as payment_value from fct_payments"
       " group by 1 order by 2 desc limit 3"),
    _s("Which 5 states have the most sellers?", "top_n",
       "select seller_state, count(*) as sellers from dim_seller where is_current"
       " group by 1 order by 2 desc limit 5"),
    _s("What are the top 5 product categories (English names) by number of items sold?", "top_n",
       f"select p.product_category_name_english as category, count(*) as items"
       f" from {REVENUE_ITEMS} group by 1 order by 2 desc limit 5"),
    # ratios
    _s("What share of total revenue came from customers in São Paulo state (SP)?", "ratio",
       "select sum(revenue) filter (where customer_state = 'SP') / sum(revenue) as sp_share"
       " from rpt_daily_sales"),
    _s("What share of total payment value was paid by credit card?", "ratio",
       "select sum(payment_value) filter (where payment_type = 'credit_card')"
       " / sum(payment_value) as credit_card_share from fct_payments"),
    _s("What fraction of all orders were canceled?", "ratio",
       "select avg(case when order_status = 'canceled' then 1.0 else 0.0 end) as canceled_ratio"
       " from fct_orders"),
    _s("What fraction of reviews have a score of 5?", "ratio",
       "select avg(case when review_score = 5 then 1.0 else 0.0 end) as five_star_share"
       " from fct_reviews"),
    _s("What fraction of customers in the customer 360 report placed more than one order?",
       "ratio",
       "select avg(case when orders > 1 then 1.0 else 0.0 end) as repeat_ratio"
       " from rpt_customer_360"),
    _m("What was the late delivery rate by quarter in 2018?", "ratio",
       ["late_delivery_rate"], [QUARTER], Y18),
    _s("What was the late delivery rate for customers in Rio de Janeiro state (RJ)?", "ratio",
       "select sum(late_orders) / sum(delivered_orders) as late_rate from rpt_delivery_sla"
       " where customer_state = 'RJ'"),
    _s("Across revenue order items, what is total freight value as a fraction of total price?",
       "ratio",
       "select sum(freight_value) / sum(price) as freight_ratio from fct_order_items"
       " where is_revenue_order"),
    # SQL over marts
    _s("What is the average review score?", "sql_marts",
       "select avg(review_score) as avg_review_score from fct_reviews"),
    _s("What was the average review score by year the review was created?", "sql_marts",
       "select year(review_creation_date) as year, avg(review_score) as avg_review_score"
       " from fct_reviews group by 1 order by 1"),
    _s("What is the average purchase-to-delivery time in hours for delivered orders?",
       "sql_marts",
       "select avg(purchase_to_customer_hours) as avg_hours from fct_orders where is_delivered"),
    _s("What is the average number of installments for credit card payments?", "sql_marts",
       "select avg(payment_installments) as avg_installments from fct_payments"
       " where payment_type = 'credit_card'"),
    _s("How many distinct (unique) customers have placed orders?", "sql_marts",
       "select count(distinct customer_unique_id) as customers from fct_orders"),
    _s("How many current sellers are based in São Paulo state (SP)?", "sql_marts",
       "select count(*) as sellers from dim_seller where is_current and seller_state = 'SP'"),
    _s("How many products are in the current catalogue?", "sql_marts",
       "select count(*) as products from dim_product where is_current"),
    _s("What is the average number of items per revenue order?", "sql_marts",
       "select avg(item_count) as avg_items from fct_orders where is_revenue_order"),
    _s("Which 5 customer states with at least 1000 delivered orders have the highest late"
       " delivery rate?", "sql_marts",
       "select customer_state, sum(late_orders) / sum(delivered_orders) as late_rate"
       " from rpt_delivery_sla group by 1 having sum(delivered_orders) >= 1000"
       " order by 2 desc limit 5"),
    _s("Which 5 customer states have the slowest average delivery time in hours?", "sql_marts",
       "select customer_state, avg(purchase_to_customer_hours) as avg_hours"
       " from fct_orders o join dim_customer c using (customer_sk)"
       " where o.is_delivered group by 1 order by 2 desc limit 5"),
    _s("What is the average customer lifetime value (ltv) in the customer 360 report?",
       "sql_marts", "select avg(ltv) as avg_ltv from rpt_customer_360"),
    _s("How many customers placed their first order in 2017?", "sql_marts",
       "select count(*) as customers from fct_orders"
       " where is_first_order and year(order_purchase_date) = 2017"),
]  # fmt: skip


def _canonical(column: str, value: Any) -> Any:
    """Time keys at their grain ('2017', '2017-03', '2017-03-01'), so any format can match."""
    width = next((w for suffix, w in _GRAIN_CHARS.items() if column.endswith(suffix)), None)
    return str(value)[:width] if width and value is not None else value


def build_cases(registry: ToolRegistry, specs: Iterable[Spec]) -> list[dict[str, Any]]:
    cases = []
    for i, spec in enumerate(specs, start=1):
        out = registry.call(AGENT, spec.tool, spec.args).model_dump(mode="json")
        columns, rows = out["columns"], out["rows"]
        if not rows:
            raise ValueError(f"empty gold result for {spec.question!r}")
        expected: dict[str, Any]
        if len(rows) == 1 and len(columns) == 1 and isinstance(rows[0][0], int | float):
            expected = {"value": rows[0][0]}
        else:
            canon = [[_canonical(c, v) for c, v in zip(columns, r, strict=True)] for r in rows]
            expected = {"columns": columns, "rows": canon}
        cases.append(
            {
                "id": f"an-{i:02d}",
                "question": spec.question,
                "category": spec.category,
                "expected": expected,
                "tolerance": TOLERANCE,
                "reference": {"tool": spec.tool, "args": spec.args, "sql": out["sql"]},
            }
        )
    return cases


def build_analytics(path: Path = ANALYTICS_GOLDEN) -> int:
    from agents.analytics_agent.tools import default_registry

    cases = build_cases(default_registry(), SPECS)
    write_jsonl(path, cases)
    print(f"wrote {len(cases)} analytics cases to {path}")
    return len(cases)


def write_jsonl(path: Path, cases: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(c, ensure_ascii=False) for c in cases]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]
