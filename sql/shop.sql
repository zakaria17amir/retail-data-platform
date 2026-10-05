-- Shopping assistant side tables and the restricted order writer; applied by `agents shop init`
-- after the olist schema exists. Idempotent. Schema shop is outside the olist_cdc publication.
CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE IF NOT EXISTS shop.stock (
    product_id text PRIMARY KEY,
    on_hand integer NOT NULL CHECK (on_hand >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- on_hand = 30 days of cover at the 90-day sales velocity (dataset time, not wall clock) plus a
-- seeded 1-10 buffer; a seeded ~5 % of products are out of stock so that case is exercised.
WITH horizon AS (
    SELECT coalesce(max(order_purchase_timestamp), now()::timestamp) AS t FROM olist.orders
),

sold AS (
    SELECT
        i.product_id,
        count(*) AS units
    FROM olist.order_items AS i
    INNER JOIN olist.orders AS o ON i.order_id = o.order_id
    CROSS JOIN horizon AS h
    WHERE o.order_purchase_timestamp > h.t - interval '90 days'
    GROUP BY i.product_id
),

seeded AS (
    SELECT
        p.product_id,
        coalesce(s.units, 0) AS units,
        get_byte(decode(md5('shop.stock:v1:' || p.product_id), 'hex'), 0) AS b
    FROM olist.products AS p
    LEFT JOIN sold AS s ON p.product_id = s.product_id
)

INSERT INTO shop.stock (product_id, on_hand)
SELECT
    product_id,
    CASE WHEN b < 13 THEN 0 ELSE ceil(units * 30 / 90.0)::integer + 1 + b % 10 END
FROM seeded
ON CONFLICT (product_id) DO UPDATE
    SET on_hand = excluded.on_hand, updated_at = now()
    WHERE shop.stock.on_hand IS DISTINCT FROM excluded.on_hand;

-- Local-dev password (matches SHOP_DSN in .env.example); rotate with ALTER ROLE elsewhere.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'shop_writer') THEN
        CREATE ROLE shop_writer LOGIN PASSWORD 'shop_writer';
    END IF;
END $$;

GRANT USAGE ON SCHEMA olist TO shop_writer;
GRANT INSERT ON olist.orders, olist.order_items, olist.order_payments TO shop_writer;
