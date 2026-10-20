ATTACH TABLE _ UUID '33e2392c-77f6-4f9e-b65e-0e135c933cfc'
(
    `product_id` UInt32,
    `market_id` UInt32,
    `date_id` UInt32,
    `nhom_san_pham` String,
    `san_pham` String,
    `mat_hang` String,
    `nam` UInt16,
    `thang` UInt8,
    `tinh_cu` String,
    `tinh_moi` String,
    `loai_gia` String,
    `cap_gia` String,
    `don_vi_tinh` String,
    `gia` Float64,
    `is_outlier` UInt8,
    `nguon` String,
    `source_file` String
)
ENGINE = MergeTree
ORDER BY (product_id, market_id, date_id)
SETTINGS index_granularity = 8192
