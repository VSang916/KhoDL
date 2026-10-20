ATTACH TABLE _ UUID '2d1016cf-e928-4f37-96e7-97f55f7b9f0d'
(
    `product_code` String,
    `product_name` String,
    `category` String,
    `standard_unit` String
)
ENGINE = ReplacingMergeTree
ORDER BY product_code
SETTINGS index_granularity = 8192
