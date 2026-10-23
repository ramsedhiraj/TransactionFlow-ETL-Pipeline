"""
TransactionFlow Domain Models & Schema Definitions
Defines enums, pydantic models, and serialization structures for raw,
validated, quarantined, and risk-flagged financial transaction records.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CurrencyEnum(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CAD = "CAD"
    AUD = "AUD"
    JPY = "JPY"
    CHF = "CHF"


class TransactionTypeEnum(str, Enum):
    PURCHASE = "PURCHASE"
    TRANSFER = "TRANSFER"
    WITHDRAWAL = "WITHDRAWAL"
    DEPOSIT = "DEPOSIT"
    REFUND = "REFUND"
    PAYMENT = "PAYMENT"


class TransactionStatusEnum(str, Enum):
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class MerchantCategoryEnum(str, Enum):
    RETAIL = "RETAIL"
    GROCERY = "GROCERY"
    ENTERTAINMENT = "ENTERTAINMENT"
    UTILITIES = "UTILITIES"
    GAMBLING = "GAMBLING"
    TRAVEL = "TRAVEL"
    DINING = "DINING"
    HEALTHCARE = "HEALTHCARE"
    ATM = "ATM"
    OTHER = "OTHER"


class RejectionReasonEnum(str, Enum):
    MISSING_TRANSACTION_ID = "MISSING_TRANSACTION_ID"
    MISSING_CUSTOMER_ID = "MISSING_CUSTOMER_ID"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    NEGATIVE_OR_ZERO_AMOUNT = "NEGATIVE_OR_ZERO_AMOUNT"
    INVALID_AMOUNT_FORMAT = "INVALID_AMOUNT_FORMAT"
    UNSUPPORTED_CURRENCY = "UNSUPPORTED_CURRENCY"
    UNSUPPORTED_TRANSACTION_TYPE = "UNSUPPORTED_TRANSACTION_TYPE"
    UNSUPPORTED_STATUS = "UNSUPPORTED_STATUS"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    DUPLICATE_IN_BATCH = "DUPLICATE_IN_BATCH"
    DUPLICATE_IN_DATABASE = "DUPLICATE_IN_DATABASE"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"


class RiskSeverityEnum(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class PipelineStatusEnum(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


class ValidatedTransaction(BaseModel):
    """
    Canonical normalized financial transaction model ready for persistence.

    Identifier hierarchy:
    - transaction_id: Canonical unique financial event identifier.
    - source_record_id: Identifier given by originating provider or file row.
    - ingestion_id: Lineage UUID generated for this ingestion event.
    - pipeline_run_id: Execution run UUID for auditability.
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    transaction_id: str = Field(..., min_length=1, max_length=64)
    source_record_id: str = Field(..., min_length=1, max_length=64)
    source_system: str = Field(..., min_length=1, max_length=64)
    customer_id: str = Field(..., min_length=1, max_length=64)
    transaction_timestamp: datetime
    transaction_amount: Decimal = Field(..., decimal_places=2)
    currency: str = Field(..., min_length=3, max_length=3)
    transaction_type: str = Field(..., min_length=1, max_length=32)
    transaction_status: str = Field(..., min_length=1, max_length=32)
    merchant_category: Optional[str] = Field(default=None, max_length=32)
    ingestion_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    pipeline_run_id: str = Field(..., min_length=1, max_length=64)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("transaction_type")
    @classmethod
    def normalize_type(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("transaction_status")
    @classmethod
    def normalize_status(cls, v: str) -> str:
        return v.strip().upper()


class QuarantinedRecord(BaseModel):
    """Quarantine record capturing rejected rows with rejection reasons."""

    model_config = ConfigDict(from_attributes=True)

    quarantine_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_run_id: str = Field(..., min_length=1, max_length=64)
    source_system: str = Field(default="UNKNOWN", max_length=64)
    source_record_id: Optional[str] = Field(default=None, max_length=64)
    raw_record: str = Field(..., description="Sanitized JSON representation of the rejected raw row")
    rejection_reason: str = Field(..., min_length=1, max_length=128)
    rejected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RiskFlagRecord(BaseModel):
    """Flagged suspicious transaction indicator."""

    model_config = ConfigDict(from_attributes=True)

    flag_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_run_id: str = Field(..., min_length=1, max_length=64)
    transaction_id: str = Field(..., min_length=1, max_length=64)
    rule_name: str = Field(..., min_length=1, max_length=64)
    severity: str = Field(default="MEDIUM", max_length=16)
    details: str = Field(default="{}", description="JSON string containing rule violation context")
    flagged_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PipelineRunRecord(BaseModel):
    """Metadata record representing a single pipeline batch run."""

    model_config = ConfigDict(from_attributes=True)

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_type: str = Field(..., max_length=32)
    source_name: str = Field(..., max_length=256)
    status: str = Field(default=PipelineStatusEnum.PENDING.value, max_length=32)
    records_extracted: int = 0
    records_accepted: int = 0
    records_rejected: int = 0
    duplicate_records: int = 0
    risk_flagged_records: int = 0
    records_loaded: int = 0
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    reconciliation_status: str = Field(default="PENDING", max_length=32)
