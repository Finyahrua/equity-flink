-- Debug Flink SQL (Print Sink)
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE IF NOT EXISTS share_capital_src (
    fspInformationId STRING,
    shareCapitalData ARRAY<ROW<
        reportingDate STRING,
        tzsAmount DOUBLE,
        sector STRING
    >>,
    ts TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'share-capital',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sql-debug-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE print_sink (
    fsp_id STRING,
    sector STRING,
    reporting_date DATE,
    share_capital_total DOUBLE,
    last_computed_at TIMESTAMP(3),
    event_count BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO print_sink
SELECT 
    fspInformationId,
    item.sector,
    CAST(item.reportingDate AS DATE),
    SUM(item.tzsAmount),
    MAX(ts),
    COUNT(*)
FROM share_capital_src CROSS JOIN UNNEST(shareCapitalData) AS item
GROUP BY fspInformationId, item.sector, item.reportingDate;
