ATTACH TABLE _ UUID 'edcca500-8eec-4ea1-959e-cdc006334bbb'
(
    `item_no` String,
    `product_name` String,
    `brand_name` String,
    `category_name` String,
    `mch3_name` String,
    `mch4_name` String,
    `mch5_name` String,
    `uom_name` String,
    `seo_name` String
)
ENGINE = ReplacingMergeTree
ORDER BY item_no
SETTINGS index_granularity = 8192
