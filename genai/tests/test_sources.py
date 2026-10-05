from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

from genai.enrichment.prompt import ProductContext
from genai.enrichment.sources import load_contexts, storage_options, stratified


def product(
    pid: str, category: str | None, english: str = "unknown", **kw: object
) -> dict[str, object]:
    return {
        "product_id": pid,
        "product_category_name": category,
        "product_category_name_english": english,
        "product_photos_qty": 1,
        "product_weight_g": 500.0,
        "product_length_cm": 20.0,
        "product_height_cm": 5.0,
        "product_width_cm": 10.0,
        "is_current": True,
        "_is_deleted": False,
        **kw,
    }


def write(root: Path, path: str, rows: list[dict[str, object]]) -> None:
    write_deltalake(str(root / path), pa.Table.from_pylist(rows))


def fixture_lake(root: Path) -> None:
    write(
        root,
        "silver/catalog/products",
        [
            product("p1", "cama_mesa_banho", "bed_bath_table"),
            product("p1", "moveis", "furniture", is_current=False),
            product("p2", "brinquedos"),
            product("p3", "brinquedos", _is_deleted=True),
        ],
    )
    write(
        root,
        "silver/catalog/categories",
        [
            {
                "product_category_name": "cama_mesa_banho",
                "product_category_name_english": "bed_bath_table",
                "_is_deleted": False,
            },
            {
                "product_category_name": "brinquedos",
                "product_category_name_english": "toys",
                "_is_deleted": False,
            },
        ],
    )
    write(
        root,
        "silver/sales/order_items",
        [
            {"order_id": "o1", "order_item_id": 1, "product_id": "p1"},
            {"order_id": "o1", "order_item_id": 2, "product_id": "p1"},
            {"order_id": "o2", "order_item_id": 1, "product_id": "p1"},
            {"order_id": "o3", "order_item_id": 1, "product_id": "p1"},
            {"order_id": "o4", "order_item_id": 1, "product_id": "p1"},
            {"order_id": "o5", "order_item_id": 1, "product_id": "p1"},
        ],
    )
    reviews: list[dict[str, object]] = [
        {
            "order_id": "o1",
            "review_comment_title": "Top",
            "review_comment_message": "muito bom mesmo",
        },
        {"order_id": "o2", "review_comment_title": None, "review_comment_message": "ok"},
        {
            "order_id": "o3",
            "review_comment_title": None,
            "review_comment_message": "chegou rapido, recomendo a todos",
        },
        {"order_id": "o4", "review_comment_title": None, "review_comment_message": None},
        {
            "order_id": "o5",
            "review_comment_title": None,
            "review_comment_message": "produto excelente",
        },
    ]
    write(root, "silver/sales/order_reviews", [{**r, "_is_deleted": False} for r in reviews])


def test_load_contexts_current_products_translation_and_top_reviews(tmp_path: Path) -> None:
    fixture_lake(tmp_path)
    contexts = {c.product_id: c for c in load_contexts(str(tmp_path))}
    assert set(contexts) == {"p1", "p2"}
    p1, p2 = contexts["p1"], contexts["p2"]
    assert (p1.category_pt, p1.category_en) == ("cama_mesa_banho", "bed_bath_table")
    assert p2.category_en == "toys"
    assert p1.reviews == (
        "chegou rapido, recomendo a todos",
        "Top. muito bom mesmo",
        "produto excelente",
    )
    assert (p1.weight_g, p1.length_cm, p1.width_cm, p1.height_cm, p1.photos) == (
        500.0,
        20.0,
        10.0,
        5.0,
        1,
    )
    assert p2.reviews == ()


def ctx(pid: str, category: str) -> ProductContext:
    return ProductContext(pid, category, category, None, None, None, None, None, ())


def test_stratified_round_robins_categories_deterministically() -> None:
    contexts = [ctx(f"a{i}", "a") for i in range(10)] + [ctx(f"b{i}", "b") for i in range(2)]
    picked = stratified(contexts, limit=6)
    assert [c.category_en for c in picked].count("b") == 2
    assert len(picked) == 6
    assert stratified(list(reversed(contexts)), limit=6) == picked
    assert len(stratified(contexts, limit=None)) == 12
    assert {c.category_en for c in stratified(contexts, limit=None, category="b")} == {"b"}


def test_storage_options() -> None:
    assert storage_options("/tmp/lake", {}) == {}
    opts = storage_options("s3://lakehouse", {"MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p"})
    assert opts["AWS_ENDPOINT_URL"] == "http://127.0.0.1:9000"
    assert (opts["AWS_ACCESS_KEY_ID"], opts["AWS_SECRET_ACCESS_KEY"]) == ("u", "p")
    assert opts["AWS_ALLOW_HTTP"] == opts["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
