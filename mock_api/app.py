"""
Mock Financial Transaction REST API
Provides a realistic, paginated REST API endpoint simulating a financial gateway.
Includes pagination, incremental timestamp filtering, transient error injection,
and rate limiting simulation for robust ETL client testing.

NOTE: This is a synthetic mock server for pipeline testing and integration validation.
It does not connect to real banking systems or store real personal data.
"""

import math
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel

app = FastAPI(
    title="TransactionFlow Mock Gateway API",
    description="Simulated paginated financial transaction provider for ETL testing.",
    version="1.0.0",
)

CURRENCIES = ["USD", "USD", "EUR", "GBP", "CAD"]
TYPES = ["PURCHASE", "TRANSFER", "WITHDRAWAL", "PAYMENT", "REFUND"]
STATUSES = ["COMPLETED", "COMPLETED", "PENDING", "FAILED"]
CATEGORIES = ["RETAIL", "GROCERY", "TRAVEL", "DINING", "ATM", "ENTERTAINMENT"]


class TransactionItem(BaseModel):
    transaction_id: str
    source_record_id: str
    source_system: str
    customer_id: str
    transaction_timestamp: str
    transaction_amount: str
    currency: str
    transaction_type: str
    transaction_status: str
    merchant_category: str


class PaginatedResponse(BaseModel):
    items: List[TransactionItem]
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool
    next_page: Optional[int]
    checkpoint_timestamp: Optional[str]


# Simulated state for rate limiting tests
_request_counter: int = 0


@app.get("/health", tags=["Health"])
def health():
    """Healthcheck endpoint for Docker Compose and readiness probes."""
    return {"status": "healthy", "service": "mock-transaction-api"}


@app.get(
    "/api/v1/transactions",
    response_model=PaginatedResponse,
    tags=["Transactions"],
    summary="Get paginated synthetic transactions",
)
def get_transactions(
    page: int = Query(default=1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(default=100, ge=1, le=1000, description="Items per page"),
    total_records: int = Query(default=5000, ge=1, le=100000, description="Total synthetic records in dataset"),
    since_timestamp: Optional[str] = Query(default=None, description="ISO-8601 timestamp for incremental ingestion"),
    seed: int = Query(default=42, description="Random seed for deterministic responses"),
    simulate_transient_error: bool = Query(default=False, description="Simulate a 503 transient server error"),
    simulate_rate_limit: bool = Query(default=False, description="Simulate a 429 Too Many Requests response"),
):
    """
    Returns a deterministic paginated page of transaction records.
    Supports incremental checkpointing, error simulation, and rate limit testing.
    """
    global _request_counter
    _request_counter += 1

    # Simulate 429 Rate Limiting
    if simulate_rate_limit and (_request_counter % 3 == 0):
        return Response(
            content='{"error": "Rate limit exceeded. Please back off."}',
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            media_type="application/json",
            headers={"Retry-After": "1"},
        )

    # Simulate 503 Transient Server Error
    if simulate_transient_error and (_request_counter % 2 == 0):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Temporary upstream gateway timeout. Retryable.",
        )

    total_pages = math.ceil(total_records / page_size)
    if page > total_pages and total_records > 0:
        return PaginatedResponse(
            items=[],
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            has_next=False,
            next_page=None,
            checkpoint_timestamp=None,
        )

    rng = random.Random(seed + page * 101)
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    if since_timestamp:
        try:
            base_time = datetime.fromisoformat(since_timestamp.replace("Z", "+00:00"))
        except Exception:
            pass

    start_idx = (page - 1) * page_size + 1
    end_idx = min(start_idx + page_size, total_records + 1)

    items: List[TransactionItem] = []
    latest_ts = base_time

    for idx in range(start_idx, end_idx):
        cust_id = f"CUST-{rng.randint(1, 5000):08d}"
        tx_time = base_time + timedelta(seconds=idx * 15 + rng.randint(0, 5))
        latest_ts = max(latest_ts, tx_time)

        amt_raw = math.exp(rng.gauss(3.8, 1.1))
        amt = round(min(max(amt_raw, 2.00), 25000.00), 2)
        tx_type = rng.choice(TYPES)

        items.append(
            TransactionItem(
                transaction_id=f"TXN-API-{tx_time.strftime('%Y%m%d')}-{idx:08d}",
                source_record_id=f"API-REC-{idx:08d}",
                source_system="PAYMENT_GATEWAY_API",
                customer_id=cust_id,
                transaction_timestamp=tx_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                transaction_amount=f"{amt:.2f}",
                currency=rng.choice(CURRENCIES),
                transaction_type=tx_type,
                transaction_status=rng.choice(STATUSES),
                merchant_category=rng.choice(CATEGORIES),
            )
        )

    has_next = page < total_pages
    next_page = page + 1 if has_next else None

    return PaginatedResponse(
        items=items,
        page=page,
        page_size=page_size,
        total_records=total_records,
        total_pages=total_pages,
        has_next=has_next,
        next_page=next_page,
        checkpoint_timestamp=latest_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
