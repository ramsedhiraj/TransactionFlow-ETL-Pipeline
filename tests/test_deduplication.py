"""
Unit tests for business key deduplication.
"""

from datetime import datetime
from decimal import Decimal

from src.deduplication import Deduplicator
from src.models import RejectionReasonEnum, ValidatedTransaction


def make_tx(tx_id: str, cust_id: str, amt: str, ts: str, tx_type: str = "PURCHASE") -> ValidatedTransaction:
    return ValidatedTransaction(
        transaction_id=tx_id,
        source_record_id=f"SRC-{tx_id}",
        source_system="CORE_SYS",
        customer_id=cust_id,
        transaction_timestamp=datetime.fromisoformat(ts.replace("Z", "+00:00")),
        transaction_amount=Decimal(amt),
        currency="USD",
        transaction_type=tx_type,
        transaction_status="COMPLETED",
        pipeline_run_id="RUN-DEDUP",
    )


def test_deduplicator_primary_key():
    dedup = Deduplicator()
    tx1 = make_tx("TXN-001", "CUST-1", "100.00", "2026-01-01T10:00:00Z")
    tx2 = make_tx("TXN-001", "CUST-1", "100.00", "2026-01-01T10:00:00Z")  # Duplicate ID

    unique, duplicate = dedup.identify_duplicates([tx1, tx2], pipeline_run_id="RUN-01")

    assert len(unique) == 1
    assert len(duplicate) == 1
    assert duplicate[0].rejection_reason == RejectionReasonEnum.DUPLICATE_IN_BATCH.value


def test_deduplicator_composite_key():
    dedup = Deduplicator(check_composite_key=True)
    # Different transaction_ids, but identical customer, timestamp, amount, and type
    tx1 = make_tx("TXN-101", "CUST-5", "250.00", "2026-01-01T10:00:00Z", "PURCHASE")
    tx2 = make_tx("TXN-102", "CUST-5", "250.00", "2026-01-01T10:00:00Z", "PURCHASE")

    unique, duplicate = dedup.identify_duplicates([tx1, tx2], pipeline_run_id="RUN-01")

    assert len(unique) == 1
    assert len(duplicate) == 1
    assert duplicate[0].rejection_reason == "DUPLICATE_COMPOSITE_KEY"


def test_deduplicator_across_batches():
    dedup = Deduplicator()
    batch_1 = [make_tx("TXN-201", "CUST-1", "50.00", "2026-01-01T10:00:00Z")]
    batch_2 = [make_tx("TXN-201", "CUST-1", "50.00", "2026-01-01T10:00:00Z")]

    unique_1, dups_1 = dedup.identify_duplicates(batch_1, "RUN-01")
    assert len(unique_1) == 1
    assert len(dups_1) == 0

    unique_2, dups_2 = dedup.identify_duplicates(batch_2, "RUN-01")
    assert len(unique_2) == 0
    assert len(dups_2) == 1
