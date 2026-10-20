ATTACH TABLE _ UUID '880ad9be-0715-4183-a817-a3aafe2f35d8'
(
    `item_no` String,
    `store_code` UInt32,
    `date_id` UInt32,
    `captured_at` DateTime('Asia/Ho_Chi_Minh'),
    `original_price` Float64,
    `selling_price` Float64,
    `stock_quantity` Float64,
    `promotion_code` String,
    `promotion_type` String,
    `is_promo` UInt8
)
ENGINE = MergeTree
PARTITION BY date_id
ORDER BY (item_no, store_code)
SETTINGS index_granularity = 8192
