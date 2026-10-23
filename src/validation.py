"""
TransactionFlow Validation Module
Enforces strict schema validation, type integrity, business rule constraints,
and routes rejected rows to structured quarantine objects with documented rejection reasons.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from src.models import (
    CurrencyEnum,
    MerchantCategoryEnum,
    QuarantinedRecord,
    RejectionReasonEnum,
    TransactionStatusEnum,
    TransactionTypeEnum,
    ValidatedTransaction,
)
from src.transform import normalize_amount, normalize_timestamp

logger = logging.getLogger("transactionflow.validation")

# Pre-compiled lookup sets for fast O(1) membership checks
VALID_CURRENCIES: Set[str] = {c.value for c in CurrencyEnum}
VALID_TYPES: Set[str] = {t.value for t in TransactionTypeEnum}
VALID_STATUSES: Set[str] = {s.value for s in TransactionStatusEnum}
VALID_CATEGORIES: Set[str] = {m.value for m in MerchantCategoryEnum}

REQUIRED_COLUMNS: List[str] = [
    "transaction_id",
    "customer_id",
    "transaction_timestamp",
    "transaction_amount",
    "currency",
    "transaction_type",
]

SENSITIVE_FIELD_NAMES: Set[str] = {"card_number", "pan", "cvv", "pin", "password", "ssn", "secret"}


def sanitize_raw_record(raw: Dict[str, Any]) -> str:
    """Mask sensitive fields and return JSON string representation."""
    sanitized = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue  # omit internal pipeline helper fields
        if k.lower() in SENSITIVE_FIELD_NAMES:
            sanitized[k] = "********"
        else:
            sanitized[k] = v
    try:
        return json.dumps(sanitized, default=str)
    except Exception:
        return str(sanitized)


def validate_schema(record: Dict[str, Any], required_fields: Optional[List[str]] = None) -> Tuple[bool, Optional[str]]:
    """
    Validate presence of required schema fields in record dictionary.
    """
    fields_to_check = required_fields or REQUIRED_COLUMNS
    for field in fields_to_check:
        val = record.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            return False, f"Missing required column: '{field}'"
    return True, None


def validate_single_record(
    raw_record: Dict[str, Any],
    pipeline_run_id: str,
) -> Tuple[Optional[ValidatedTransaction], Optional[QuarantinedRecord]]:
    """
    Validate a single transaction record.
    Returns (ValidatedTransaction, None) if valid, or (None, QuarantinedRecord) if rejected.
    """
    # Extract and clean string fields directly without dictionary cloning
    tx_id_raw = raw_record.get("transaction_id")
    tx_id = tx_id_raw.strip() if isinstance(tx_id_raw, str) else None

    source_sys_raw = raw_record.get("source_system")
    source_sys = source_sys_raw.strip().upper() if isinstance(source_sys_raw, str) else "UNKNOWN"

    src_rec_id_raw = raw_record.get("source_record_id")
    src_rec_id = src_rec_id_raw.strip() if isinstance(src_rec_id_raw, str) else None

    # 1. Required column presence
    if not tx_id:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.MISSING_TRANSACTION_ID.value,
        )

    cust_id_raw = raw_record.get("customer_id")
    cust_id = cust_id_raw.strip() if isinstance(cust_id_raw, str) else None
    if not cust_id:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.MISSING_CUSTOMER_ID.value,
        )

    # 2. Timestamp validation
    parsed_ts = normalize_timestamp(raw_record.get("transaction_timestamp"))
    if parsed_ts is None:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.INVALID_TIMESTAMP.value,
        )

    # 3. Amount validation
    amount_parsed = normalize_amount(raw_record.get("transaction_amount"))
    if amount_parsed is None:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.INVALID_AMOUNT_FORMAT.value,
        )

    if amount_parsed <= Decimal("0.00"):
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.NEGATIVE_OR_ZERO_AMOUNT.value,
        )

    # 4. Currency validation
    curr_raw = raw_record.get("currency")
    curr = curr_raw.strip().upper() if isinstance(curr_raw, str) else None
    if not curr or curr not in VALID_CURRENCIES:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.UNSUPPORTED_CURRENCY.value,
        )

    # 5. Transaction type validation
    tx_type_raw = raw_record.get("transaction_type")
    tx_type = tx_type_raw.strip().upper() if isinstance(tx_type_raw, str) else None
    if not tx_type or tx_type not in VALID_TYPES:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.UNSUPPORTED_TRANSACTION_TYPE.value,
        )

    # 6. Status validation
    status_raw = raw_record.get("transaction_status")
    status = status_raw.strip().upper() if isinstance(status_raw, str) else "COMPLETED"
    if status not in VALID_STATUSES:
        return None, QuarantinedRecord(
            pipeline_run_id=pipeline_run_id,
            source_system=source_sys,
            source_record_id=src_rec_id,
            raw_record=sanitize_raw_record(raw_record),
            rejection_reason=RejectionReasonEnum.UNSUPPORTED_STATUS.value,
        )

    m_cat_raw = raw_record.get("merchant_category")
    m_cat = m_cat_raw.strip().upper() if isinstance(m_cat_raw, str) else None

    # Fast-path model construction without validator re-execution
    validated = ValidatedTransaction.model_construct(
        transaction_id=tx_id,
        source_record_id=src_rec_id or tx_id,
        source_system=source_sys,
        customer_id=cust_id,
        transaction_timestamp=parsed_ts,
        transaction_amount=amount_parsed,
        currency=curr,
        transaction_type=tx_type,
        transaction_status=status,
        merchant_category=m_cat,
        ingestion_id=uuid.uuid4().hex,
        ingestion_timestamp=datetime.now(timezone.utc),
        pipeline_run_id=pipeline_run_id,
    )
    return validated, None


def validate_transaction_records(
    raw_records: List[Dict[str, Any]],
    pipeline_run_id: str,
) -> Tuple[List[ValidatedTransaction], List[QuarantinedRecord]]:
    """
    Validate a batch of raw transaction records.
    Splits the batch into accepted ValidatedTransactions and rejected QuarantinedRecords.
    """
    accepted: List[ValidatedTransaction] = []
    quarantined: List[QuarantinedRecord] = []

    for raw in raw_records:
        valid_rec, reject_rec = validate_single_record(raw, pipeline_run_id)
        if valid_rec is not None:
            accepted.append(valid_rec)
        elif reject_rec is not None:
            quarantined.append(reject_rec)

    return accepted, quarantined
