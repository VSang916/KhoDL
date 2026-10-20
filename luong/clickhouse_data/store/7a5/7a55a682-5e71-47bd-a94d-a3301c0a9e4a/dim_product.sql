ATTACH TABLE _ UUID 'f34db491-e071-4add-8727-8a0001713c7a'
(
    `product_id` UInt32,
    `nhom_san_pham` String,
    `san_pham` String,
    `mat_hang` String
)
ENGINE = ReplacingMergeTree
ORDER BY product_id
SETTINGS index_granularity = 8192
