ATTACH TABLE _ UUID '644d7272-0656-44ea-b72b-cda49933aa57'
(
    `run_date` Date,
    `scope` String,
    `product_id` UInt32,
    `market_id` UInt32,
    `date_id` UInt32,
    `loai_gia` String,
    `don_vi_tinh` String,
    `op` String,
    `change_kind` String,
    `old_n_obs` Nullable(UInt32),
    `new_n_obs` Nullable(UInt32),
    `old_avg_price` Nullable(Float64),
    `new_avg_price` Nullable(Float64),
    `old_median_price` Nullable(Float64),
    `new_median_price` Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (run_date, product_id, market_id, date_id)
SETTINGS index_granularity = 8192
