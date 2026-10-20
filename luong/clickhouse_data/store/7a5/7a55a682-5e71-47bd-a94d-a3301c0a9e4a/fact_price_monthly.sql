ATTACH TABLE _ UUID 'b485459f-ead7-4230-bcdf-4ef475223ee1'
(
    `product_id` UInt32,
    `market_id` UInt32,
    `date_id` UInt32,
    `loai_gia` String,
    `cap_gia` String,
    `don_vi_tinh` String,
    `n_obs` UInt32,
    `n_outliers` UInt32,
    `avg_price` Float64,
    `median_price` Float64,
    `min_price` Float64,
    `max_price` Float64,
    `version` UInt64,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (product_id, market_id, date_id, loai_gia, don_vi_tinh)
SETTINGS index_granularity = 8192
