import importlib
import logging
import os
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Protocol

import httpx
from pydantic import BaseModel, Field

from agents.core.guardrails import INJECTION
from agents.core.registry import Tool, ToolError, ToolRegistry

log = logging.getLogger(__name__)

AGENT = "shopping"
# place_order (and its quote) are allowlisted only for the approval node, never for the LLM loop
APPROVER = "shopping_approver"
HITS_PER_PRODUCT = 4
SNIPPET_CHARS = 300
ORDER_OFFSET = timedelta(minutes=1)
SHIPPING_LIMIT = timedelta(days=6)
ESTIMATED_DELIVERY = timedelta(days=15)
WITHHELD = "[withheld: this text contained instructions aimed at the assistant]"


@dataclass(frozen=True)
class Session:
    customer_id: str
    session_id: str


class ProductRow(BaseModel):
    product_id: str
    category: str | None = None
    category_en: str | None = None
    photos: int | None = None
    weight_g: int | None = None
    price: float | None = None
    freight: float | None = None
    seller_id: str | None = None


class OrderRow(BaseModel):
    order_id: str
    customer_id: str | None
    customer_unique_id: str | None = None
    status: str | None = None
    purchased_at: datetime | None = None
    approved_at: datetime | None = None
    shipped_at: datetime | None = None
    delivered_at: datetime | None = None
    estimated_delivery: datetime | None = None


class Enriched(BaseModel):
    title: str
    description: str
    tags: list[str]


class QuoteLine(BaseModel):
    product_id: str
    title: str
    quantity: int
    unit_price: float
    freight: float
    seller_id: str


class Quote(BaseModel):
    customer_id: str
    lines: list[QuoteLine]
    payment_type: str
    installments: int
    total: float
    purchased_at: datetime
    estimated_delivery: datetime


class ShopData(Protocol):
    def products(self, ids: Sequence[str]) -> dict[str, ProductRow]: ...
    def reviews(self, product_id: str) -> list[str]: ...
    def stock(self, ids: Sequence[str]) -> dict[str, int]: ...
    def order(self, order_id: str) -> OrderRow | None: ...
    def customer_unique_id(self, customer_id: str) -> str | None: ...
    def dataset_now(self) -> datetime: ...
    def insert_order(self, order_id: str, quote: Quote) -> None: ...


class Hit(Protocol):
    @property
    def product_id(self) -> str: ...
    @property
    def text(self) -> str: ...
    @property
    def score(self) -> float: ...


Searcher = Callable[[str, int, str | None], Sequence[Hit]]
Enrichment = Callable[[Sequence[str]], dict[str, Enriched]]

ProductId = Annotated[str, Field(min_length=1, max_length=64)]


class SearchIn(BaseModel):
    query: str = Field(min_length=1, max_length=200, description="What the customer wants.")
    k: int = Field(default=5, ge=1, le=10)
    category: str | None = Field(default=None, description="Optional product category filter.")


class ProductSummary(BaseModel):
    product_id: str
    title: str
    category: str | None
    price: float | None
    score: float | None = None
    snippet: str | None = None


class SearchOut(BaseModel):
    products: list[ProductSummary]


class ProductIn(BaseModel):
    product_id: ProductId


class ProductOut(BaseModel):
    product_id: str
    title: str
    description: str | None
    tags: list[str]
    category: str | None
    price: float | None
    freight: float | None
    photos: int | None
    weight_g: int | None
    reviews: list[str]


class StockIn(BaseModel):
    product_ids: list[str] = Field(min_length=1, max_length=20)


class StockItem(BaseModel):
    product_id: str
    on_hand: int
    in_stock: bool


class StockOut(BaseModel):
    items: list[StockItem]
    unknown: list[str]


class RecommendIn(BaseModel):
    k: int = Field(default=5, ge=1, le=10)


class RecommendOut(BaseModel):
    strategy: str
    products: list[ProductSummary]


class OrderStatusIn(BaseModel):
    order_id: str = Field(min_length=1, max_length=64)


class OrderStatusOut(BaseModel):
    order_id: str
    status: str | None
    purchased_at: datetime | None
    approved_at: datetime | None
    shipped_at: datetime | None
    delivered_at: datetime | None
    estimated_delivery: datetime | None


class OrderLine(BaseModel):
    product_id: ProductId
    quantity: int = Field(ge=1, le=10)


class PlaceOrderIn(BaseModel):
    items: list[OrderLine] = Field(min_length=1, max_length=10)
    payment_type: Literal["credit_card", "boleto", "voucher", "debit_card"] = "credit_card"
    installments: int = Field(default=1, ge=1, le=12)


class PlaceOrderOut(Quote):
    order_id: str
    status: str


def screen(text: str) -> str:
    """Untrusted product/review text that tries to instruct the assistant is withheld."""
    return WITHHELD if INJECTION.search(text) else text


def _title(product_id: str, row: ProductRow | None, enriched: Enriched | None) -> str:
    if enriched is not None:
        return enriched.title
    category = (row.category_en or row.category) if row else None
    return f"{category or 'product'} {product_id[:8]}"


def summaries(
    shop: ShopData, enrichment: Enrichment, ids: Sequence[str]
) -> dict[str, ProductSummary]:
    rows, texts = shop.products(ids), enrichment(ids)
    return {
        i: ProductSummary(
            product_id=i,
            title=_title(i, rows.get(i), texts.get(i)),
            category=(rows[i].category_en or rows[i].category) if i in rows else None,
            price=rows[i].price if i in rows else None,
        )
        for i in ids
    }


def quote(shop: ShopData, enrichment: Enrichment, customer_id: str, args: PlaceOrderIn) -> Quote:
    if shop.customer_unique_id(customer_id) is None:
        raise ToolError(f"unknown customer {customer_id!r}")
    wanted: dict[str, int] = {}
    for line in args.items:
        wanted[line.product_id] = wanted.get(line.product_id, 0) + line.quantity
    ids = list(wanted)
    rows, on_hand, texts = shop.products(ids), shop.stock(ids), enrichment(ids)
    lines = []
    for pid, qty in wanted.items():
        row = rows.get(pid)
        if row is None:
            raise ToolError(f"unknown product {pid!r}")
        if row.price is None or row.freight is None or row.seller_id is None:
            raise ToolError(f"product {pid!r} has no offer")
        if on_hand.get(pid, 0) < qty:
            raise ToolError(f"only {on_hand.get(pid, 0)} of product {pid!r} in stock")
        lines.append(
            QuoteLine(
                product_id=pid,
                title=_title(pid, row, texts.get(pid)),
                quantity=qty,
                unit_price=row.price,
                freight=row.freight,
                seller_id=row.seller_id,
            )
        )
    purchased_at = shop.dataset_now() + ORDER_OFFSET
    return Quote(
        customer_id=customer_id,
        lines=lines,
        payment_type=args.payment_type,
        installments=args.installments,
        total=round(sum(ln.quantity * (ln.unit_price + ln.freight) for ln in lines), 2),
        purchased_at=purchased_at,
        estimated_delivery=purchased_at + ESTIMATED_DELIVERY,
    )


def rag_search(query: str, k: int, category: str | None) -> Sequence[Hit]:
    """genai.rag.search(query, k, category) -> list[Hit], imported lazily (heavy, needs RAG_DSN)."""
    hits: Sequence[Hit] = importlib.import_module("genai.rag").search(query, k=k, category=category)
    return hits


def lakehouse_enrichment(ids: Sequence[str], root: str | None = None) -> dict[str, Enriched]:
    """Latest enriched title/description/tags from Delta silver/product_enriched ({} on error)."""
    if not ids:
        return {}
    from deltalake import DeltaTable
    from genai.enrichment.sources import storage_options
    from genai.enrichment.writer import ENRICHED

    root = root or f"s3://{os.environ.get('LAKEHOUSE_BUCKET') or 'lakehouse'}"
    try:
        table = DeltaTable(f"{root}/{ENRICHED}", storage_options=storage_options(root))
        rows: list[dict[str, Any]] = table.to_pyarrow_table(
            columns=["product_id", "title", "description", "tags", "enriched_at"],
            filters=[("product_id", "in", list(ids))],
        ).to_pylist()
    except Exception as exc:  # noqa: BLE001 - titles fall back to the category without enrichment
        log.warning("enrichment_unavailable", extra={"error": f"{type(exc).__name__}: {exc}"})
        return {}
    latest: dict[str, Enriched] = {}
    for r in sorted(rows, key=lambda r: r["enriched_at"]):
        latest[r["product_id"]] = Enriched(
            title=r["title"], description=r["description"], tags=r["tags"]
        )
    return latest


def build_registry(
    shop: ShopData,
    session: Session,
    *,
    search: Searcher = rag_search,
    enrichment: Enrichment = lakehouse_enrichment,
    http: httpx.Client | None = None,
) -> ToolRegistry:
    client = http or httpx.Client(
        base_url=os.environ.get("RECOMMEND_URL", "http://127.0.0.1:8000"), timeout=5
    )

    def search_products(args: SearchIn) -> SearchOut:
        best: dict[str, Hit] = {}
        for hit in search(args.query, args.k * HITS_PER_PRODUCT, args.category):
            best.setdefault(hit.product_id, hit)
        ids = list(best)[: args.k]
        found = summaries(shop, enrichment, ids)
        for pid in ids:
            found[pid].score = best[pid].score
            found[pid].snippet = screen(best[pid].text[:SNIPPET_CHARS])
        return SearchOut(products=[found[i] for i in ids])

    def get_product(args: ProductIn) -> ProductOut:
        row = shop.products([args.product_id]).get(args.product_id)
        if row is None:
            raise ToolError(f"unknown product {args.product_id!r}")
        text = enrichment([args.product_id]).get(args.product_id)
        return ProductOut(
            product_id=row.product_id,
            title=_title(row.product_id, row, text),
            description=text.description if text else None,
            tags=text.tags if text else [],
            category=row.category_en or row.category,
            price=row.price,
            freight=row.freight,
            photos=row.photos,
            weight_g=row.weight_g,
            reviews=[screen(r) for r in shop.reviews(row.product_id)],
        )

    def check_stock(args: StockIn) -> StockOut:
        on_hand = shop.stock(args.product_ids)
        return StockOut(
            items=[
                StockItem(product_id=p, on_hand=on_hand[p], in_stock=on_hand[p] > 0)
                for p in args.product_ids
                if p in on_hand
            ],
            unknown=[p for p in args.product_ids if p not in on_hand],
        )

    def get_recommendations(args: RecommendIn) -> RecommendOut:
        try:
            resp = client.post("/recommend", json={"session_id": session.session_id, "k": args.k})
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise ToolError(f"recommendations are unavailable: {exc}") from exc
        body = resp.json()
        ids = [str(item["product_id"]) for item in body["items"]]
        found = summaries(shop, enrichment, ids)
        return RecommendOut(strategy=body["strategy"], products=[found[i] for i in ids])

    def get_order_status(args: OrderStatusIn) -> OrderStatusOut:
        order = shop.order(args.order_id)
        me = shop.customer_unique_id(session.customer_id)
        mine = order is not None and (
            order.customer_id == session.customer_id
            or (me is not None and order.customer_unique_id == me)
        )
        if order is None or not mine:
            raise ToolError(f"order {args.order_id!r} was not found for the current customer")
        return OrderStatusOut.model_validate(order.model_dump())

    def quote_order(args: PlaceOrderIn) -> Quote:
        return quote(shop, enrichment, session.customer_id, args)

    def place_quote(q: Quote) -> PlaceOrderOut:
        """Insert exactly the approved quote: prices/time are not re-read; a stock drop aborts."""
        if q.customer_id != session.customer_id:
            raise ToolError("this quote belongs to another customer; nothing was placed")
        on_hand = shop.stock([ln.product_id for ln in q.lines])
        for ln in q.lines:
            if on_hand.get(ln.product_id, 0) < ln.quantity:
                raise ToolError(
                    f"stock changed since approval: only {on_hand.get(ln.product_id, 0)} of "
                    f"product {ln.product_id!r} left; nothing was placed"
                )
        order_id = uuid.uuid4().hex
        shop.insert_order(order_id, q)
        log.info("order_placed", extra={"agent": AGENT, "order_id": order_id, "total": q.total})
        return PlaceOrderOut(order_id=order_id, status="created", **q.model_dump())

    def place_order(args: PlaceOrderIn) -> PlaceOrderOut:
        return place_quote(quote_order(args))

    llm = frozenset({AGENT})
    approver = frozenset({APPROVER})
    reg = ToolRegistry()
    for tool in [
        Tool(
            "search_products",
            "Search the catalogue by meaning and keywords; returns products, titles and prices.",
            SearchIn,
            SearchOut,
            search_products,
            llm,
        ),
        Tool(
            "get_product",
            "Details of one product: title, description, tags, price, freight and reviews.",
            ProductIn,
            ProductOut,
            get_product,
            llm,
        ),
        Tool(
            "check_stock",
            "Units on hand for up to 20 product ids.",
            StockIn,
            StockOut,
            check_stock,
            llm,
        ),
        Tool(
            "get_recommendations",
            "Personalised product recommendations for this chat session.",
            RecommendIn,
            RecommendOut,
            get_recommendations,
            llm,
        ),
        Tool(
            "get_order_status",
            "Status and dates of one of the current customer's orders, by order id.",
            OrderStatusIn,
            OrderStatusOut,
            get_order_status,
            llm,
        ),
        Tool(
            "quote_order",
            "Price an order for human approval.",
            PlaceOrderIn,
            Quote,
            quote_order,
            approver,
        ),
        Tool(
            "place_order",
            "Place an order for the current customer. The customer must approve it first.",
            PlaceOrderIn,
            PlaceOrderOut,
            place_order,
            approver,
        ),
        Tool(
            "place_quote",
            "Insert the quote the customer approved, unchanged.",
            Quote,
            PlaceOrderOut,
            place_quote,
            approver,
        ),
    ]:
        reg.register(tool)
    return reg
