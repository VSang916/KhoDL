ATTACH VIEW _ UUID '7b3d7a89-aa55-424c-9123-f50b76ddbd43'
(
    `month` Date,
    `year` UInt16,
    `quarter` UInt8,
    `month_no` UInt8,
    `nhom_san_pham` String,
    `san_pham` String,
    `mat_hang` String,
    `tinh_cu` String,
    `tinh_moi` String,
    `loai_gia` String,
    `cap_gia` String,
    `don_vi_tinh` String,
    `avg_price` Float64,
    `median_price` Float64,
    `min_price` Float64,
    `max_price` Float64,
    `n_obs` UInt32,
    `n_outliers` UInt32
)
AS SELECT
    d.first_day AS month,
    d.year,
    d.quarter,
    d.month AS month_no,
    p.nhom_san_pham,
    p.san_pham,
    p.mat_hang,
    m.tinh_cu,
    m.tinh_moi,
    f.loai_gia,
    f.cap_gia,
    f.don_vi_tinh,
    f.avg_price,
    f.median_price,
    f.min_price,
    f.max_price,
    f.n_obs,
    f.n_outliers
FROM ttns.fact_price_monthly AS f
FINAL
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_product
    FINAL
) AS p ON f.product_id = p.product_id
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_market
    FINAL
) AS m ON f.market_id = m.market_id
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_date
    FINAL
) AS d ON f.date_id = d.date_id
WHERE f.is_deleted = 0
