ATTACH TABLE _ UUID '1daed46d-f5dc-40ef-a58b-8a5f340172d1'
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
