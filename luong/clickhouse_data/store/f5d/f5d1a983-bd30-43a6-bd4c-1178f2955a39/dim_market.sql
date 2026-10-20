ATTACH TABLE _ UUID 'fffcef27-30ea-4236-90ad-ec7c00f71312'
(
    `market_id` UInt32,
    `market_name` String,
    `market_type` String,
    `district` String,
    `province` String
)
ENGINE = ReplacingMergeTree
ORDER BY market_id
SETTINGS index_granularity = 8192
