ATTACH VIEW _ UUID '43f7fd33-3d3b-4ae9-aff0-1623c4253bff'
(
    `run_date` Date,
    `op` String,
    `change_kind` String,
    `month` Date,
    `san_pham` String,
    `mat_hang` String,
    `tinh_moi` String,
    `loai_gia` String,
    `don_vi_tinh` String,
    `old_avg_price` Nullable(Float64),
    `new_avg_price` Nullable(Float64),
    `pct_change` Nullable(Float64),
    `old_n_obs` Nullable(UInt32),
    `new_n_obs` Nullable(UInt32),
    `scope` String
)
AS SELECT
    c.run_date,
    c.op,
    c.change_kind,
    d.first_day AS month,
    p.san_pham,
    p.mat_hang,
    m.tinh_moi,
    c.loai_gia,
    c.don_vi_tinh,
    c.old_avg_price,
    c.new_avg_price,
    round(((c.new_avg_price - c.old_avg_price) / c.old_avg_price) * 100, 1) AS pct_change,
    c.old_n_obs,
    c.new_n_obs,
    c.scope
FROM ttns.cdc_log AS c
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_product
    FINAL
) AS p ON c.product_id = p.product_id
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_market
    FINAL
) AS m ON c.market_id = m.market_id
LEFT JOIN
(
    SELECT *
    FROM ttns.dim_date
    FINAL
) AS d ON c.date_id = d.date_id
