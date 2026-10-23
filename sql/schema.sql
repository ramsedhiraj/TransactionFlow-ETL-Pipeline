-- =============================================================================
-- TransactionFlow DDL Schema (PostgreSQL 14+)
-- High-Volume Transaction ETL & Risk Monitoring Pipeline
-- =============================================================================

-- Enable UUID extension if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- 1. Pipeline Runs: Operational audit and lineage table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    source_type VARCHAR(32) NOT NULL,
    source_name VARCHAR(256) NOT NULL,
    status VARCHAR(32) NOT NULL,
    records_extracted BIGINT NOT NULL DEFAULT 0,
    records_accepted BIGINT NOT NULL DEFAULT 0,
    records_rejected BIGINT NOT NULL DEFAULT 0,
    duplicate_records BIGINT NOT NULL DEFAULT 0,
    risk_flagged_records BIGINT NOT NULL DEFAULT 0,
    records_loaded BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    duration_seconds NUMERIC(10, 3),
    error_message TEXT,
    reconciliation_status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

-- -----------------------------------------------------------------------------
-- 2. Transactions: Canonical financial transaction table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    source_record_id VARCHAR(64) NOT NULL,
    source_system VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    transaction_amount NUMERIC(15, 2) NOT NULL CHECK (transaction_amount > 0),
    currency VARCHAR(3) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    transaction_status VARCHAR(32) NOT NULL,
    merchant_category VARCHAR(32),
    ingestion_id VARCHAR(64) NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    pipeline_run_id VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

-- Targeted indexes for query performance vs write throughput trade-off:
-- 1. Composite customer index enables sub-millisecond customer velocity and statement queries.
CREATE INDEX IF NOT EXISTS idx_transactions_customer_ts 
    ON transactions(customer_id, transaction_timestamp DESC);

-- 2. Date-range index for partition pruning or daily financial reporting.
CREATE INDEX IF NOT EXISTS idx_transactions_ts 
    ON transactions(transaction_timestamp DESC);

-- 3. Currency index for multi-currency reconciliation.
CREATE INDEX IF NOT EXISTS idx_transactions_currency 
    ON transactions(currency);

-- 4. Pipeline run lineage index for audit lookups.
CREATE INDEX IF NOT EXISTS idx_transactions_pipeline_run 
    ON transactions(pipeline_run_id);

-- -----------------------------------------------------------------------------
-- 3. Quarantined Transactions: Dead-letter queue / quarantine store
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantined_transactions (
    quarantine_id VARCHAR(64) PRIMARY KEY,
    pipeline_run_id VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    source_system VARCHAR(64) NOT NULL,
    source_record_id VARCHAR(64),
    raw_record TEXT NOT NULL,
    rejection_reason VARCHAR(128) NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_quarantine_run_id 
    ON quarantined_transactions(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_reason 
    ON quarantined_transactions(rejection_reason);

-- -----------------------------------------------------------------------------
-- 4. Transaction Risk Flags: Heuristic risk monitoring indicators
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaction_risk_flags (
    flag_id VARCHAR(64) PRIMARY KEY,
    pipeline_run_id VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    transaction_id VARCHAR(64) NOT NULL,
    rule_name VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    details TEXT NOT NULL,
    flagged_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_flags_rule 
    ON transaction_risk_flags(rule_name);
CREATE INDEX IF NOT EXISTS idx_risk_flags_tx_id 
    ON transaction_risk_flags(transaction_id);
CREATE INDEX IF NOT EXISTS idx_risk_flags_run_id 
    ON transaction_risk_flags(pipeline_run_id);

-- -----------------------------------------------------------------------------
-- 5. Ingestion Checkpoints: High-water mark state for incremental sources
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    source_system VARCHAR(64) PRIMARY KEY,
    last_processed_timestamp TIMESTAMPTZ NOT NULL,
    last_record_id VARCHAR(64),
    updated_at TIMESTAMPTZ NOT NULL
);
