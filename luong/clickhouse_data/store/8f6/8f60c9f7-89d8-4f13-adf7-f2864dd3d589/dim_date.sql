ATTACH TABLE _ UUID '24f8f13b-75b9-4ed1-bb48-546c1c40f4ae'
(
    `date_id` UInt32,
    `full_date` Date,
    `day` UInt8,
    `month` UInt8,
    `year` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY date_id
SETTINGS index_granularity = 8192
