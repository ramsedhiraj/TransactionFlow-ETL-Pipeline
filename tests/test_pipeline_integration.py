"""
End-to-end integration tests for TransactionFlow pipeline.
"""

import tempfile
from pathlib import Path

from starlette.testclient import TestClient

from mock_api.app import app
from scripts.generate_transactions import generate_to_csv
from src.pipeline import run_pipeline


def test_pipeline_integration_csv_run(temp_db):
    """
    Test end-to-end pipeline run from CSV:
    generates 500 records with controlled errors and duplicates,
    runs the pipeline, verifies reconciliation and DB counts.
    """
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
        csv_path = Path(f.name)

    try:
        generate_to_csv(
            output_path=csv_path,
            total_records=500,
            batch_size=100,
            seed=42,
            error_rate=0.04,  # ~20 errors
            duplicate_rate=0.02,  # ~10 duplicates
        )

        run_record = run_pipeline(
            source_type="CSV",
            source_target=str(csv_path),
            batch_size=100,
            db_manager=temp_db,
        )

        assert run_record.status == "SUCCESS"
        assert run_record.reconciliation_status == "BALANCED"
        assert run_record.records_extracted == 500
        assert run_record.records_rejected > 0
        assert run_record.records_loaded == run_record.records_accepted
        assert run_record.records_extracted == (
            run_record.records_accepted + run_record.records_rejected + run_record.duplicate_records
        )

        # Verify database contents
        tx_count = temp_db.count_transactions(pipeline_run_id=run_record.run_id)
        quarantine_count = temp_db.count_quarantined(pipeline_run_id=run_record.run_id)

        assert tx_count == run_record.records_loaded
        assert quarantine_count == (run_record.records_rejected + run_record.duplicate_records)

    finally:
        csv_path.unlink(missing_ok=True)


def test_pipeline_integration_api_run(temp_db):
    """
    Test end-to-end pipeline run from mock REST API source.
    """
    client = TestClient(app)

    run_record = run_pipeline(
        source_type="REST_API",
        source_target="http://testserver",
        api_page_size=50,
        api_max_pages=3,
        api_client=client,
        db_manager=temp_db,
    )

    assert run_record.status == "SUCCESS"
    assert run_record.reconciliation_status == "BALANCED"
    assert run_record.records_extracted == 150
    assert run_record.records_loaded > 0

    tx_count = temp_db.count_transactions(pipeline_run_id=run_record.run_id)
    assert tx_count == run_record.records_loaded
