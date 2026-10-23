"""
Unit tests for REST API pagination, rate limiting, and retry backoff.
"""

from unittest.mock import MagicMock

import httpx

from mock_api.app import app
from src.extract import extract_api_pages


def test_api_extraction_normal():
    # Use FastAPI TestClient directly as an httpx client
    from starlette.testclient import TestClient

    client = TestClient(app)

    # Ingest 2 pages of 5 items
    batches = list(
        extract_api_pages(
            base_url="http://testserver",
            page_size=5,
            max_pages=2,
            client=client,
        )
    )

    assert len(batches) == 2
    items_page_1, chk_1 = batches[0]
    items_page_2, chk_2 = batches[1]

    assert len(items_page_1) == 5
    assert len(items_page_2) == 5
    assert items_page_1[0]["transaction_id"] != items_page_2[0]["transaction_id"]
    assert chk_1 is not None


def test_api_extraction_retry_on_503(monkeypatch):
    """Verify that transient 503 errors trigger retries with exponential backoff."""
    mock_client = MagicMock(spec=httpx.Client)

    # First call returns 503, second call succeeds with 200
    mock_resp_fail = MagicMock()
    mock_resp_fail.status_code = 503

    mock_resp_success = MagicMock()
    mock_resp_success.status_code = 200
    mock_resp_success.json.return_value = {
        "items": [{"transaction_id": "TXN-RETRY-01"}],
        "has_next": False,
        "next_page": None,
    }

    mock_client.get.side_effect = [mock_resp_fail, mock_resp_success]

    # Monkeypatch time.sleep to avoid waiting in tests
    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda s: sleep_calls.append(s))

    batches = list(
        extract_api_pages(
            base_url="http://mock-api.com",
            max_retries=3,
            backoff_factor=1.0,
            client=mock_client,
        )
    )

    assert len(batches) == 1
    assert len(sleep_calls) == 1
    assert batches[0][0][0]["transaction_id"] == "TXN-RETRY-01"


def test_api_extraction_retry_on_429_rate_limit(monkeypatch):
    """Verify 429 rate limit honors Retry-After header."""
    mock_client = MagicMock(spec=httpx.Client)

    mock_resp_rate_limit = MagicMock()
    mock_resp_rate_limit.status_code = 429
    mock_resp_rate_limit.headers = {"Retry-After": "2"}

    mock_resp_ok = MagicMock()
    mock_resp_ok.status_code = 200
    mock_resp_ok.json.return_value = {
        "items": [{"transaction_id": "TXN-RATE-LIMIT-OK"}],
        "has_next": False,
        "next_page": None,
    }

    mock_client.get.side_effect = [mock_resp_rate_limit, mock_resp_ok]

    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda s: sleep_calls.append(s))

    batches = list(
        extract_api_pages(
            base_url="http://mock-api.com",
            client=mock_client,
        )
    )

    assert len(batches) == 1
    assert sleep_calls == [2.0]
    assert batches[0][0][0]["transaction_id"] == "TXN-RATE-LIMIT-OK"
