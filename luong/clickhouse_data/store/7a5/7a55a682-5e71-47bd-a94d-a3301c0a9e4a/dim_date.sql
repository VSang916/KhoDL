ATTACH TABLE _ UUID '596e7578-7b4c-4dfc-96ed-87370e9539e9'
(
    `date_id` UInt32,
    `first_day` Date,
    `year` UInt16,
    `quarter` UInt8,
    `month` UInt8
)
ENGINE = ReplacingMergeTree
ORDER BY date_id
SETTINGS index_granularity = 8192
