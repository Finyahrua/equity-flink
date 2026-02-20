-- Realtime Capital Position Aggregator (Flink SQL)
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- 1. Source: Share Capital
CREATE TABLE IF NOT EXISTS share_capital_src (
    fspInformationId STRING,
    shareCapitalData ARRAY<ROW<
        reportingDate STRING,
        tzsAmount DOUBLE,
        sectorSnaClassification STRING
    >>,
    ts TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'share-capital',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sql-aggregator-v5',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 2. Source: Reserves
CREATE TABLE IF NOT EXISTS reserves_src (
    fspInformationId STRING,
    otherCapitalAccountData ARRAY<ROW<
        reportingDate STRING,
        tzsAmount DOUBLE,
        sector STRING
    >>,
    ts TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'other-capital',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sql-aggregator-v5',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 3. Source: Dividends
CREATE TABLE IF NOT EXISTS dividends_src (
    fspInformationId STRING,
    dividendsPayableData ARRAY<ROW<
        reportingDate STRING,
        tzsAmount DOUBLE,
        sector STRING
    >>,
    ts TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'dividends-payable',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sql-aggregator-v5',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 4. Source: Deductions
CREATE TABLE IF NOT EXISTS deductions_src (
    fspInformationId STRING,
    coreCapitalDeductionsData ARRAY<ROW<
        reportingDate STRING,
        tzsAmount DOUBLE,
        sector STRING
    >>,
    ts TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'core-capital-deductions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sql-aggregator-v5',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 5. Sink: State Table
CREATE TABLE IF NOT EXISTS capital_state_sink (
    fsp_id STRING,
    sector STRING,
    reporting_date DATE,
    share_capital_total DOUBLE,
    reserves_total DOUBLE,
    dividends_payable_total DOUBLE,
    deductions_total DOUBLE,
    last_computed_at TIMESTAMP(3),
    event_count BIGINT,
    PRIMARY KEY (fsp_id, sector, reporting_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://host.docker.internal:5432/capital_db',
    'table-name' = 'capital_state',
    'username' = 'postgres',
    'password' = 'krukas',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- 6. Sink: Event Log Table
CREATE TABLE IF NOT EXISTS capital_event_log_sink (
    source_type     STRING,
    fsp_id          STRING,
    sector          STRING,
    reporting_date  DATE,
    amount          DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://host.docker.internal:5432/capital_db',
    'table-name' = 'capital_event_log',
    'username' = 'postgres',
    'password' = 'krukas',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- 7. Job: Execute both sinks
EXECUTE STATEMENT SET BEGIN

-- A. Update State Table (Aggregated)
INSERT INTO capital_state_sink
SELECT 
    fsp_id,
    sector,
    CAST(reporting_date AS DATE),
    SUM(share_capital),
    SUM(reserves),
    SUM(dividends),
    SUM(deductions),
    MAX(ts),
    COUNT(*)
FROM (
    -- Normalize Share Capital
    SELECT fspInformationId as fsp_id, item.sectorSnaClassification as sector, item.reportingDate as reporting_date, item.tzsAmount as share_capital, 0.0 as reserves, 0.0 as dividends, 0.0 as deductions, ts
    FROM share_capital_src CROSS JOIN UNNEST(shareCapitalData) AS item
    WHERE item.sectorSnaClassification IS NOT NULL
    
    UNION ALL
    
    -- Normalize Reserves
    SELECT fspInformationId as fsp_id, item.sector as sector, item.reportingDate as reporting_date, 0.0 as share_capital, item.tzsAmount as reserves, 0.0 as dividends, 0.0 as deductions, ts
    FROM reserves_src CROSS JOIN UNNEST(otherCapitalAccountData) AS item
    WHERE item.sector IS NOT NULL
    
    UNION ALL
    
    -- Normalize Dividends
    SELECT fspInformationId as fsp_id, item.sector as sector, item.reportingDate as reporting_date, 0.0 as share_capital, 0.0 as reserves, item.tzsAmount as dividends, 0.0 as deductions, ts
    FROM dividends_src CROSS JOIN UNNEST(dividendsPayableData) AS item
    WHERE item.sector IS NOT NULL
    
    UNION ALL
    
    -- Normalize Deductions
    SELECT fspInformationId as fsp_id, item.sector as sector, item.reportingDate as reporting_date, 0.0 as share_capital, 0.0 as reserves, 0.0 as dividends, item.tzsAmount as deductions, ts
    FROM deductions_src CROSS JOIN UNNEST(coreCapitalDeductionsData) AS item
    WHERE item.sector IS NOT NULL
) 
GROUP BY fsp_id, sector, reporting_date;

-- B. Log Raw Events
INSERT INTO capital_event_log_sink
SELECT 'SHARE', fspInformationId, item.sectorSnaClassification, CAST(item.reportingDate AS DATE), item.tzsAmount
FROM share_capital_src CROSS JOIN UNNEST(shareCapitalData) AS item
WHERE item.sectorSnaClassification IS NOT NULL;

INSERT INTO capital_event_log_sink
SELECT 'RESERVE', fspInformationId, item.sector, CAST(item.reportingDate AS DATE), item.tzsAmount
FROM reserves_src CROSS JOIN UNNEST(otherCapitalAccountData) AS item
WHERE item.sector IS NOT NULL;

INSERT INTO capital_event_log_sink
SELECT 'DIVIDEND', fspInformationId, item.sector, CAST(item.reportingDate AS DATE), item.tzsAmount
FROM dividends_src CROSS JOIN UNNEST(dividendsPayableData) AS item
WHERE item.sector IS NOT NULL;

INSERT INTO capital_event_log_sink
SELECT 'DEDUCTION', fspInformationId, item.sector, CAST(item.reportingDate AS DATE), item.tzsAmount
FROM deductions_src CROSS JOIN UNNEST(coreCapitalDeductionsData) AS item
WHERE item.sector IS NOT NULL;

END;
