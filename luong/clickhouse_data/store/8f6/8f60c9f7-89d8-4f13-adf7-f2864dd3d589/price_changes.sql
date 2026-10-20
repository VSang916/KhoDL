ATTACH TABLE _ UUID '11e34137-d04f-4700-b226-c729bd93aaa0'
(
    `change_date_id` UInt32,
    `item_no` String,
    `store_code` UInt32,
    `op` String,
    `change_type` String,
    `old_original_price` Nullable(Float64),
    `new_original_price` Nullable(Float64),
    `old_selling_price` Nullable(Float64),
    `new_selling_price` Nullable(Float64),
    `old_promotion_code` Nullable(String),
    `new_promotion_code` Nullable(String),
    `old_is_promo` Nullable(UInt8),
    `new_is_promo` Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY (item_no, store_code, change_date_id)
SETTINGS index_granularity = 8192
