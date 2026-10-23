"""
Unit tests for pipeline count reconciliation and integrity accounting.
"""

from src.reconciliation import reconcile_pipeline_counts


def test_reconcile_balanced():
    # 100 extracted = 90 accepted + 8 rejected + 2 duplicates
    # 90 loaded = 90 accepted, DB verified count = 90
    is_balanced, status, report = reconcile_pipeline_counts(
        records_extracted=100,
        records_accepted=90,
        records_rejected=8,
        duplicate_records=2,
        records_loaded=90,
        db_loaded_verified=90,
    )

    assert is_balanced is True
    assert status == "BALANCED"
    assert report["discrepancy"] == 0


def test_reconcile_unbalanced_extraction():
    # 100 extracted, but accepted (85) + rejected (8) + dups (2) = 95 -> 5 rows missing!
    is_balanced, status, report = reconcile_pipeline_counts(
        records_extracted=100,
        records_accepted=85,
        records_rejected=8,
        duplicate_records=2,
        records_loaded=85,
        db_loaded_verified=85,
    )

    assert is_balanced is False
    assert status == "UNBALANCED_EXTRACTION"
    assert report["discrepancy"] == 5


def test_reconcile_unbalanced_load():
    # Extracted accounts correctly, but records_loaded (80) != records_accepted (90)
    is_balanced, status, report = reconcile_pipeline_counts(
        records_extracted=100,
        records_accepted=90,
        records_rejected=8,
        duplicate_records=2,
        records_loaded=80,
        db_loaded_verified=80,
    )

    assert is_balanced is False
    assert status == "UNBALANCED_LOAD"
