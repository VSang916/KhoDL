ATTACH TABLE _ UUID 'd607f27e-2dea-4cb6-b8cb-39c4961e7217'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server executing the query.' CODEC(ZSTD(1)),
    `clickhouse_version` LowCardinality(String) COMMENT 'Version of the ClickHouse server that produced the row.' CODEC(ZSTD(1)),
    `system_processor` LowCardinality(String) COMMENT 'CPU architecture of the ClickHouse server that produced the row.' CODEC(ZSTD(1)),
    `event_date` Date COMMENT 'Event date.' CODEC(Delta(2), ZSTD(1)),
    `event_time` DateTime COMMENT 'Event time.' CODEC(Delta(4), ZSTD(1)),
    `metric` LowCardinality(String) COMMENT 'Metric name.' CODEC(ZSTD(1)),
    `key` LowCardinality(String) COMMENT 'The key of a key-value metric, e.g. the CPU core number or the block device name. Empty for scalar metrics.' CODEC(ZSTD(1)),
    `value` Float64 COMMENT 'Metric value.' CODEC(ZSTD(3)),
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (metric, event_date, event_time, key)
SETTINGS index_granularity = 8192
COMMENT 'Contains the historical values for system.asynchronous_metrics, once per time interval (one second by default).\n\nIt is safe to truncate or drop this table at any time.'
