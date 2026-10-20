ATTACH TABLE _ UUID '2a97f2a2-2dd9-410d-bc6b-9e1e7affb939'
(
    `product_code` String,
    `market_id` UInt32,
    `date_id` UInt32,
    `original_price` Float64,
    `selling_price` Float64
)
ENGINE = MergeTree
PARTITION BY date_id
ORDER BY (product_code, market_id)
SETTINGS index_granularity = 8192
