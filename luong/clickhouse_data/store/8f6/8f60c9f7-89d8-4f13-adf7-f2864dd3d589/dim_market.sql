ATTACH TABLE _ UUID '289bfe83-f44c-464d-9bda-38600e26d6d5'
(
    `store_code` UInt32,
    `store_group_code` UInt32,
    `market_name` String
)
ENGINE = ReplacingMergeTree
ORDER BY store_code
SETTINGS index_granularity = 8192
