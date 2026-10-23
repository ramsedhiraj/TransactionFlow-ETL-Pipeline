"""
Unit tests for schema validation, data quality checks, and quarantine routing.
"""

import json
from decimal import Decimal

from src.models import RejectionReasonEnum
from src.validation import (
    sanitize_raw_record,
    validate_schema,
    validate_single_record,
    validate_transaction_records,
)


def test_validate_schema_success(sample_valid_record):
    is_valid, err = validate_schema(sample_valid_record)
    assert is_valid is True
    assert err is None


def test_validate_schema_missing_column():
    record = {"transaction_id": "TXN-1"}
    is_valid, err = validate_schema(record)
    assert is_valid is False
    assert "Missing required column" in err


def test_validate_single_record_valid(sample_valid_record):
    valid, quarantined = validate_single_record(sample_valid_record, pipeline_run_id="RUN-01")
    assert valid is not None
    assert quarantined is None
    assert valid.transaction_id == "TXN-20260101-000000001"
    assert valid.transaction_amount == Decimal("150.75")
    assert valid.currency == "USD"


def test_validate_invalid_records(sample_invalid_records):
    expected_reasons = [
        RejectionReasonEnum.MISSING_TRANSACTION_ID.value,
        RejectionReasonEnum.MISSING_CUSTOMER_ID.value,
        RejectionReasonEnum.NEGATIVE_OR_ZERO_AMOUNT.value,
        RejectionReasonEnum.NEGATIVE_OR_ZERO_AMOUNT.value,
        RejectionReasonEnum.INVALID_AMOUNT_FORMAT.value,
        RejectionReasonEnum.INVALID_TIMESTAMP.value,
        RejectionReasonEnum.UNSUPPORTED_CURRENCY.value,
        RejectionReasonEnum.UNSUPPORTED_TRANSACTION_TYPE.value,
    ]

    for raw, expected_reason in zip(sample_invalid_records, expected_reasons):
        valid, quarantined = validate_single_record(raw, pipeline_run_id="RUN-01")
        assert valid is None, f"Expected record to be invalid: {raw}"
        assert quarantined is not None
        assert quarantined.rejection_reason == expected_reason


def test_sanitize_raw_record():
    raw = {
        "transaction_id": "TXN-1",
        "card_number": "4111111111111234",
        "cvv": "999",
        "customer_id": "CUST-1",
        "_internal_line": 42,
    }
    sanitized_str = sanitize_raw_record(raw)
    parsed = json.loads(sanitized_str)

    assert parsed["card_number"] == "********"
    assert parsed["cvv"] == "********"
    assert parsed["customer_id"] == "CUST-1"
    assert "_internal_line" not in parsed


def test_validate_batch_records(sample_valid_record, sample_invalid_records):
    batch = [sample_valid_record] + sample_invalid_records
    accepted, quarantined = validate_transaction_records(batch, pipeline_run_id="RUN-TEST")

    assert len(accepted) == 1
    assert len(quarantined) == len(sample_invalid_records)
    assert accepted[0].transaction_id == sample_valid_record["transaction_id"]
