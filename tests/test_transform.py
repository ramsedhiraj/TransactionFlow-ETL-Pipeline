"""
Unit tests for data transformations, timestamp normalization, and amount parsing.
"""

from datetime import datetime, timezone
from decimal import Decimal

from src.transform import (
    normalize_amount,
    normalize_record,
    normalize_string,
    normalize_timestamp,
)


def test_normalize_string():
    assert normalize_string("  hello world  ") == "hello world"
    assert normalize_string("  usd  ", uppercase=True) == "USD"
    assert normalize_string(None) is None


def test_normalize_timestamp():
    # ISO string with Z
    dt1 = normalize_timestamp("2026-05-15T14:30:00Z")
    assert dt1 == datetime(2026, 5, 15, 14, 30, 0, tzinfo=timezone.utc)

    # Space separated format
    dt2 = normalize_timestamp("2026-05-15 14:30:00")
    assert dt2 == datetime(2026, 5, 15, 14, 30, 0, tzinfo=timezone.utc)

    # Date only format
    dt3 = normalize_timestamp("2026-05-15")
    assert dt3 == datetime(2026, 5, 15, 0, 0, 0, tzinfo=timezone.utc)

    # Invalid timestamp
    assert normalize_timestamp("not-a-timestamp") is None
    assert normalize_timestamp("") is None
    assert normalize_timestamp(None) is None


def test_normalize_amount():
    assert normalize_amount("123.456") == Decimal("123.46")
    assert normalize_amount("$1,250.50") == Decimal("1250.50")
    assert normalize_amount(100) == Decimal("100.00")
    assert normalize_amount(99.9) == Decimal("99.90")
    assert normalize_amount("invalid_amount") is None
    assert normalize_amount(None) is None


def test_normalize_record():
    raw = {
        "transaction_id": " txn-123 ",
        "currency": " usd ",
        "transaction_type": " purchase ",
        "transaction_status": " completed ",
        "source_system": " my_source ",
    }
    norm = normalize_record(raw)
    assert norm["transaction_id"] == "txn-123"
    assert norm["currency"] == "USD"
    assert norm["transaction_type"] == "PURCHASE"
    assert norm["transaction_status"] == "COMPLETED"
    assert norm["source_system"] == "MY_SOURCE"
