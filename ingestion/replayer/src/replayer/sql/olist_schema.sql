CREATE SCHEMA IF NOT EXISTS olist;

CREATE OR REPLACE FUNCTION olist.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS olist.customers (
    customer_id text PRIMARY KEY,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.orders (
    order_id text PRIMARY KEY,
    customer_id text,
    order_status text,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.order_items (
    order_id text,
    order_item_id integer,
    product_id text,
    seller_id text,
    shipping_limit_date timestamp,
    price numeric(12, 2),
    freight_value numeric(12, 2),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS olist.order_payments (
    order_id text,
    payment_sequential integer,
    payment_type text,
    payment_installments integer,
    payment_value numeric(12, 2),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS olist.order_reviews (
    review_pk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    review_id text,
    order_id text,
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.products (
    product_id text PRIMARY KEY,
    product_category_name text,
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.sellers (
    seller_id text PRIMARY KEY,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.geolocation (
    geolocation_pk bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    geolocation_zip_code_prefix integer,
    geolocation_lat double precision,
    geolocation_lng double precision,
    geolocation_city text,
    geolocation_state text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS olist.product_category_name_translation (
    product_category_name text PRIMARY KEY,
    product_category_name_english text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'customers', 'orders', 'order_items', 'order_payments', 'order_reviews',
        'products', 'sellers', 'geolocation', 'product_category_name_translation'
    ] LOOP
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON olist.%I
             FOR EACH ROW EXECUTE FUNCTION olist.set_updated_at()', t);
    END LOOP;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'olist_cdc') THEN
        CREATE PUBLICATION olist_cdc FOR TABLES IN SCHEMA olist;
    END IF;
END $$;
