ATTACH TABLE _ UUID 'c5e6daca-dc70-4904-afb6-3297ce12992c'
(
    `hostname` LowCardinality(String) COMMENT 'Hostname of the server executing the query.' CODEC(ZSTD(1)),
    `clickhouse_version` LowCardinality(String) COMMENT 'Version of the ClickHouse server that produced the row.' CODEC(ZSTD(1)),
    `system_processor` LowCardinality(String) COMMENT 'CPU architecture of the ClickHouse server that produced the row.' CODEC(ZSTD(1)),
    `event_date` Date COMMENT 'Event date.' CODEC(Delta(2), ZSTD(1)),
    `event_time` DateTime COMMENT 'Event time.' CODEC(Delta(4), ZSTD(1)),
    `code` Int32 COMMENT 'Error code.' CODEC(ZSTD(1)),
    `error` LowCardinality(String) COMMENT 'Error name.' CODEC(ZSTD(1)),
    `value` UInt64 COMMENT 'Number of errors happened in time interval.' CODEC(ZSTD(3)),
    `remote` UInt8 COMMENT 'Remote exception (i.e. received during one of the distributed queries).' CODEC(ZSTD(1)),
    `last_error_time` DateTime COMMENT 'The time when the last error happened.' CODEC(ZSTD(1)),
    `last_error_message` String COMMENT 'Message for the last error.' CODEC(ZSTD(1)),
    `last_error_query_id` String COMMENT 'Id of a query that caused the last error (if available).' CODEC(ZSTD(1)),
    `last_error_trace` Array(UInt64) COMMENT 'A stack trace of the last error. On ELF platforms except FreeBSD, addresses inside the main ClickHouse binary are stored as physical file offsets, and other addresses are virtual memory addresses inside the ClickHouse server process.' CODEC(ZSTD(1)),
    INDEX event_time_index event_time TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192
COMMENT 'Contains history of error values from table system.errors, periodically flushed to disk.\n\nIt is safe to truncate or drop this table at any time.'
