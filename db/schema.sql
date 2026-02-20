-- Realtime Capital Position — Database Schema
-- Primary state table: one row per (fsp_id, sector, reporting_date)

CREATE TABLE IF NOT EXISTS capital_state (
    fsp_id                  VARCHAR(100)    NOT NULL,
    sector                  VARCHAR(100)    NOT NULL,
    reporting_date          DATE            NOT NULL,

    share_capital_total     NUMERIC(20, 4)  NOT NULL DEFAULT 0,
    reserves_total          NUMERIC(20, 4)  NOT NULL DEFAULT 0,
    dividends_payable_total NUMERIC(20, 4)  NOT NULL DEFAULT 0,
    deductions_total        NUMERIC(20, 4)  NOT NULL DEFAULT 0,

    net_core_capital        NUMERIC(20, 4)  GENERATED ALWAYS AS (
                                share_capital_total
                              + reserves_total
                              - dividends_payable_total
                              - deductions_total
                            ) STORED,

    last_share_update       TIMESTAMPTZ,
    last_reserves_update    TIMESTAMPTZ,
    last_dividends_update   TIMESTAMPTZ,
    last_deductions_update  TIMESTAMPTZ,
    last_computed_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    event_count             BIGINT          NOT NULL DEFAULT 0,

    PRIMARY KEY (fsp_id, sector, reporting_date)
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_capital_state_fsp       ON capital_state (fsp_id, reporting_date);
CREATE INDEX IF NOT EXISTS idx_capital_state_sector    ON capital_state (sector, reporting_date);
CREATE INDEX IF NOT EXISTS idx_capital_state_reporting ON capital_state (reporting_date);

-- Regulatory snapshot table (immutable, point-in-time)
CREATE TABLE IF NOT EXISTS capital_snapshot (
    snapshot_id             BIGSERIAL       PRIMARY KEY,
    snapshot_taken_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    snapshot_reporting_date DATE            NOT NULL,

    fsp_id                  VARCHAR(100)    NOT NULL,
    sector                  VARCHAR(100)    NOT NULL,
    reporting_date          DATE            NOT NULL,

    share_capital_total     NUMERIC(20, 4)  NOT NULL,
    reserves_total          NUMERIC(20, 4)  NOT NULL,
    dividends_payable_total NUMERIC(20, 4)  NOT NULL,
    deductions_total        NUMERIC(20, 4)  NOT NULL,
    net_core_capital        NUMERIC(20, 4)  NOT NULL,

    last_share_update       TIMESTAMPTZ,
    last_reserves_update    TIMESTAMPTZ,
    last_dividends_update   TIMESTAMPTZ,
    last_deductions_update  TIMESTAMPTZ,
    last_computed_at        TIMESTAMPTZ,

    event_count             BIGINT
);

CREATE INDEX IF NOT EXISTS idx_snapshot_reporting_date ON capital_snapshot (snapshot_reporting_date);
CREATE INDEX IF NOT EXISTS idx_snapshot_fsp            ON capital_snapshot (fsp_id, snapshot_reporting_date);

-- Raw event audit log (optional, for debugging / late arrival analysis)
CREATE TABLE IF NOT EXISTS capital_event_log (
    event_id        BIGSERIAL       PRIMARY KEY,
    received_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_type     VARCHAR(20)     NOT NULL,  -- SHARE | RESERVE | DIVIDEND | DEDUCTION
    fsp_id          VARCHAR(100)    NOT NULL,
    sector          VARCHAR(100)    NOT NULL,
    reporting_date  DATE            NOT NULL,
    transaction_date DATE,
    amount          NUMERIC(20, 4)  NOT NULL,
    raw_payload     JSONB
);

CREATE INDEX IF NOT EXISTS idx_event_log_fsp     ON capital_event_log (fsp_id, reporting_date);
CREATE INDEX IF NOT EXISTS idx_event_log_type    ON capital_event_log (source_type, received_at);
