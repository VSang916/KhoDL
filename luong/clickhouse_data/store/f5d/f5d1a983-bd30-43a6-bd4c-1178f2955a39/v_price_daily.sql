ATTACH VIEW _ UUID '61b49cbd-aa7f-4979-b940-1e23d3457b66'
(
    `date` Date,
    `f.product_code` String,
    `product_name` String,
    `category` String,
    `standard_unit` String,
    `market_name` String,
    `district` String,
    `province` String,
    `original_price` Float64,
    `selling_price` Float64,
    `discount_pct` Float64
)
AS SELECT
    d.full_date AS date,
    f.product_code,
    p.product_name,
    p.category,
    p.standard_unit,
    m.market_name,
    m.district,
    m.province,
    f.original_price,
    f.selling_price,
    round(((f.original_price - f.selling_price) / f.original_price) * 100, 1) AS discount_pct
FROM agri.fact_price AS f
LEFT JOIN
(
    SELECT *
    FROM agri.dim_product
    FINAL
) AS p ON f.product_code = p.product_code
LEFT JOIN
(
    SELECT *
    FROM agri.dim_market
    FINAL
) AS m ON f.market_id = m.market_id
LEFT JOIN
(
    SELECT *
    FROM agri.dim_date
    FINAL
) AS d ON f.date_id = d.date_id
