"""
Unit tests for CSV extraction and chunked streaming.
"""

import tempfile
from pathlib import Path

import pytest

from src.extract import extract_csv_batches


def test_extract_csv_batches_normal():
    csv_content = """transaction_id,customer_id,transaction_timestamp,transaction_amount,currency,transaction_type
TXN-1,CUST-1,2026-01-01T10:00:00Z,100.00,USD,PURCHASE
TXN-2,CUST-2,2026-01-01T10:05:00Z,200.00,EUR,TRANSFER
TXN-3,CUST-3,2026-01-01T10:10:00Z,300.00,GBP,WITHDRAWAL
TXN-4,CUST-4,2026-01-01T10:15:00Z,400.00,CAD,PURCHASE
TXN-5,CUST-5,2026-01-01T10:20:00Z,500.00,USD,PAYMENT
"""
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        batches = list(extract_csv_batches(temp_path, batch_size=2))
        assert len(batches) == 3
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2
        assert len(batches[2]) == 1

        assert batches[0][0]["transaction_id"] == "TXN-1"
        assert batches[0][0]["customer_id"] == "CUST-1"
        assert batches[0][0]["_source_line_num"] == 2
        assert batches[2][0]["transaction_id"] == "TXN-5"
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_extract_csv_batches_file_not_found():
    with pytest.raises(FileNotFoundError):
        list(extract_csv_batches("non_existent_file_path_123.csv"))


def test_extract_csv_empty_file():
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as f:
        f.write("")
        temp_path = f.name

    try:
        batches = list(extract_csv_batches(temp_path))
        assert len(batches) == 0
    finally:
        Path(temp_path).unlink(missing_ok=True)
