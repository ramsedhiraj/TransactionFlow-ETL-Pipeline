"""
Unit and integration tests for database operations, transactions, and rollback behavior.
"""

import sqlalchemy as sa

from src.models import (
    PipelineRunRecord,
)


def test_database_table_creation(temp_db):
    """Verify all tables and indexes exist."""
    inspector = sa.inspect(temp_db.engine)
    table_names = inspector.get_table_names()

    assert "pipeline_runs" in table_names
    assert "transactions" in table_names
    assert "quarantined_transactions" in table_names
    assert "transaction_risk_flags" in table_names
    assert "ingestion_checkpoints" in table_names


def test_pipeline_run_lifecycle(temp_db):
    run_rec = PipelineRunRecord(
        run_id="RUN-TEST-LIFECYCLE",
        source_type="CSV",
        source_name="test.csv",
        status="RUNNING",
    )
    temp_db.record_run_start(run_rec)

    # Update on completion
    run_rec.status = "SUCCESS"
    run_rec.records_extracted = 500
    run_rec.records_accepted = 490
    run_rec.records_rejected = 10
    run_rec.records_loaded = 490
    run_rec.reconciliation_status = "BALANCED"
    run_rec.duration_seconds = 1.25

    temp_db.record_run_end(run_rec)

    with temp_db.engine.connect() as conn:
        res = conn.execute(
            sa.text("SELECT status, records_loaded FROM pipeline_runs WHERE run_id = 'RUN-TEST-LIFECYCLE'")
        ).fetchone()
        assert res[0] == "SUCCESS"
        assert res[1] == 490


def test_transaction_batch_load_and_idempotency(temp_db, sample_validated_transaction):
    # Initial run record
    run_rec = PipelineRunRecord(
        run_id=sample_validated_transaction.pipeline_run_id,
        source_type="CSV",
        source_name="test.csv",
        status="RUNNING",
    )
    temp_db.record_run_start(run_rec)

    # Load transaction
    loaded = temp_db.load_transaction_batch([sample_validated_transaction])
    assert loaded == 1
    assert temp_db.count_transactions() == 1

    # Rerun with same transaction_id (idempotent write)
    loaded_rerun = temp_db.load_transaction_batch([sample_validated_transaction])
    assert loaded_rerun == 1
    assert temp_db.count_transactions() == 1  # count must remain 1


def test_transaction_rollback_on_failure(temp_db):
    """Verify that database transaction rolls back changes when an error is raised."""
    run_rec = PipelineRunRecord(run_id="RUN-ROLLBACK", source_type="CSV", source_name="t.csv", status="RUNNING")
    temp_db.record_run_start(run_rec)

    try:
        with temp_db.transaction() as conn:
            conn.execute(
                sa.text("""
                INSERT INTO transactions (
                    transaction_id, source_record_id, source_system, customer_id,
                    transaction_timestamp, transaction_amount, currency,
                    transaction_type, transaction_status, ingestion_id,
                    ingestion_timestamp, pipeline_run_id
                ) VALUES (
                    'TXN-FAIL-01', 'SRC-1', 'SYS', 'CUST-1',
                    '2026-01-01 10:00:00', 100.0, 'USD',
                    'PURCHASE', 'COMPLETED', 'ING-1',
                    '2026-01-01 10:00:00', 'RUN-ROLLBACK'
                )
            """)
            )
            # Trigger intentional exception
            raise RuntimeError("Simulated mid-transaction failure")
    except RuntimeError:
        pass

    # Verify transaction was rolled back
    assert temp_db.count_transactions() == 0
