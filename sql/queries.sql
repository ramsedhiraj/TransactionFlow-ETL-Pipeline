-- =============================================================================
-- TransactionFlow Analytical & Verification Queries
-- Used for operational monitoring, reconciliation, and audit reports
-- =============================================================================

-- 1. Daily Transaction Volume and Total Processed Amount
SELECT 
    DATE_TRUNC('day', transaction_timestamp) AS transaction_date,
    currency,
    COUNT(*) AS total_transactions,
    ROUND(SUM(transaction_amount), 2) AS total_volume,
    ROUND(AVG(transaction_amount), 2) AS avg_transaction_amount,
    ROUND(MIN(transaction_amount), 2) AS min_amount,
    ROUND(MAX(transaction_amount), 2) AS max_amount
FROM transactions
GROUP BY 1, 2
ORDER BY 1 DESC, 2 ASC;

-- 2. Total Transaction Volume Grouped by Currency
SELECT 
    currency,
    COUNT(*) AS total_records,
    ROUND(SUM(transaction_amount), 2) AS total_settlement_volume,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM transactions
GROUP BY currency
ORDER BY total_settlement_volume DESC;

-- 3. Transaction Status Breakdown
SELECT 
    transaction_status,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM transactions
GROUP BY transaction_status
ORDER BY record_count DESC;

-- 4. Heuristic Risk Flags Breakdown by Rule Type and Severity
SELECT 
    rule_name,
    severity,
    COUNT(*) AS total_flags,
    COUNT(DISTINCT transaction_id) AS unique_transactions_flagged,
    MIN(flagged_at) AS earliest_detection,
    MAX(flagged_at) AS latest_detection
FROM transaction_risk_flags
GROUP BY rule_name, severity
ORDER BY total_flags DESC;

-- 5. Quarantined Records Breakdown by Rejection Reason
SELECT 
    rejection_reason,
    source_system,
    COUNT(*) AS rejection_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_rejections
FROM quarantined_transactions
GROUP BY rejection_reason, source_system
ORDER BY rejection_count DESC;

-- 6. Pipeline Execution Summary and Reconciliation State
SELECT 
    run_id,
    source_type,
    source_name,
    status,
    records_extracted,
    records_accepted,
    records_rejected,
    duplicate_records,
    records_loaded,
    risk_flagged_records,
    reconciliation_status,
    duration_seconds,
    ROUND(records_extracted / NULLIF(duration_seconds, 0), 1) AS throughput_rows_sec,
    started_at,
    completed_at
FROM pipeline_runs
ORDER BY started_at DESC;

-- 7. High-Volume Customer Velocity Audit Sample
SELECT 
    customer_id,
    COUNT(*) AS tx_count,
    ROUND(SUM(transaction_amount), 2) AS total_spent,
    MIN(transaction_timestamp) AS first_tx,
    MAX(transaction_timestamp) AS last_tx
FROM transactions
GROUP BY customer_id
HAVING COUNT(*) > 5
ORDER BY tx_count DESC
LIMIT 20;
