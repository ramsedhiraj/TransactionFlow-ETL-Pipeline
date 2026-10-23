"""
TransactionFlow Test Configuration & Fixtures
Provides deterministic test records, in-memory databases, and mock fixtures.
"""

import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.database import DatabaseManager
from src.models import ValidatedTransaction


@pytest.fixture
def sample_valid_record():
    return {
        "transaction_id": "TXN-20260101-000000001",
        "source_record_id": "SRC-000000001",
        "source_system": "CORE_BANKING_CSV",
        "customer_id": "CUST-00000042",
        "transaction_timestamp": "2026-01-01T12:00:00Z",
        "transaction_amount": "150.75",
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "transaction_status": "COMPLETED",
        "merchant_category": "GROCERY",
    }


@pytest.fixture
def sample_invalid_records():
    return [
        # Missing transaction_id
        {
            "transaction_id": "",
            "customer_id": "CUST-00000001",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "50.00",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Missing customer_id
        {
            "transaction_id": "TXN-002",
            "customer_id": "",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "50.00",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Negative amount
        {
            "transaction_id": "TXN-003",
            "customer_id": "CUST-00000003",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "-50.00",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Zero amount
        {
            "transaction_id": "TXN-004",
            "customer_id": "CUST-00000004",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "0.00",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Invalid amount format
        {
            "transaction_id": "TXN-005",
            "customer_id": "CUST-00000005",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "NOT_A_NUMBER",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Invalid timestamp
        {
            "transaction_id": "TXN-006",
            "customer_id": "CUST-00000006",
            "transaction_timestamp": "INVALID-DATE-TIME",
            "transaction_amount": "100.00",
            "currency": "USD",
            "transaction_type": "PURCHASE",
        },
        # Unsupported currency
        {
            "transaction_id": "TXN-007",
            "customer_id": "CUST-00000007",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "100.00",
            "currency": "XYZ",
            "transaction_type": "PURCHASE",
        },
        # Unsupported type
        {
            "transaction_id": "TXN-008",
            "customer_id": "CUST-00000008",
            "transaction_timestamp": "2026-01-01T12:00:00Z",
            "transaction_amount": "100.00",
            "currency": "USD",
            "transaction_type": "INVALID_TYPE",
        },
    ]


@pytest.fixture
def temp_db():
    """Provides a fresh isolated SQLite database for unit and integration tests."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        temp_path = f.name

    db_url = f"sqlite:///{temp_path}"
    manager = DatabaseManager(db_url=db_url)
    manager.create_tables()

    yield manager

    manager.engine.dispose()
    if os.path.exists(temp_path):
        try:
            os.remove(temp_path)
        except Exception:
            pass


@pytest.fixture
def sample_validated_transaction():
    return ValidatedTransaction(
        transaction_id="TXN-TEST-001",
        source_record_id="SRC-TEST-001",
        source_system="TEST_SYS",
        customer_id="CUST-001",
        transaction_timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        transaction_amount=Decimal("150.00"),
        currency="USD",
        transaction_type="PURCHASE",
        transaction_status="COMPLETED",
        merchant_category="RETAIL",
        pipeline_run_id="RUN-TEST-001",
    )
