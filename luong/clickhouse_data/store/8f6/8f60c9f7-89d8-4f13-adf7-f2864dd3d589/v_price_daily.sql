ATTACH VIEW _ UUID '1f9cbe0d-e617-4cda-abd1-dcbab3a85dfb'
(
    `date` Date,
    `f.item_no` String,
    `product_name` String,
    `brand_name` String,
    `category_name` String,
    `mch4_name` String,
    `mch5_name` String,
    `uom_name` String,
    `market_name` String,
    `f.store_code` UInt32,
    `original_price` Float64,
    `selling_price` Float64,
    `is_promo` UInt8,
    `discount_pct` Float64,
    `stock_quantity` Float64,
    `captured_at` DateTime('Asia/Ho_Chi_Minh')
)
AS SELECT
    d.full_date AS date,
    f.item_no,
    p.product_name,
    p.brand_name,
    p.category_name,
    p.mch4_name,
    p.mch5_name,
    p.uom_name,
    m.market_name,
    f.store_code,
    f.original_price,
    f.selling_price,
    f.is_promo,
    round(((f.original_price - f.selling_price) / f.original_price) * 100, 1) AS discount_pct,
    f.stock_quantity,
    f.captured_at
FROM winmart.fact_price AS f
LEFT JOIN
(
    SELECT *
    FROM winmart.dim_product
    FINAL
) AS p ON f.item_no = p.item_no
LEFT JOIN
(
    SELECT *
    FROM winmart.dim_market
    FINAL
) AS m ON f.store_code = m.store_code
LEFT JOIN
(
    SELECT *
    FROM winmart.dim_date
    FINAL
) AS d ON f.date_id = d.date_id
