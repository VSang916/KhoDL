ATTACH TABLE _ UUID '5429f1be-dd05-4c90-bc62-e57dabe9172c'
(
    `market_id` UInt32,
    `tinh_cu` String,
    `tinh_moi` String
)
ENGINE = ReplacingMergeTree
ORDER BY market_id
SETTINGS index_granularity = 8192
