"""
TransactionFlow Transformation & Normalization Module
Standardizes transaction fields, normalizes timestamps to UTC, trims whitespace,
and enforces canonical case formatting.
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional


def normalize_string(value: Optional[Any], uppercase: bool = False) -> Optional[str]:
    """Clean string, trim whitespace, and optionally convert to uppercase."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned.upper() if uppercase else cleaned


def normalize_timestamp(value: Any) -> Optional[datetime]:
    """
    Parse and normalize a timestamp to timezone-aware UTC datetime.
    Supports ISO formats, standard date strings, and datetime objects.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if not isinstance(value, str):
        return None

    val_str = value.strip()
    if not val_str:
        return None

    # Handle Z suffix
    if val_str.endswith("Z"):
        val_str = val_str[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(val_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except ValueError:
        # Fallback to common alternative patterns
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val_str, fmt).replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue

    return None


def normalize_amount(value: Any) -> Optional[Decimal]:
    """Parse numeric amount and quantize to 2 decimal places."""
    if value is None:
        return None

    try:
        if isinstance(value, (int, float)):
            val_dec = Decimal(str(value))
        elif isinstance(value, str):
            cleaned = value.replace(",", "").replace("$", "").replace("€", "").replace("£", "").strip()
            val_dec = Decimal(cleaned)
        elif isinstance(value, Decimal):
            val_dec = value
        else:
            return None

        return val_dec.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize raw record dictionary fields to standardized types and formatting.
    """
    normalized = dict(raw)

    if "transaction_id" in raw:
        normalized["transaction_id"] = normalize_string(raw.get("transaction_id"))
    if "source_record_id" in raw:
        normalized["source_record_id"] = normalize_string(raw.get("source_record_id"))
    if "source_system" in raw:
        normalized["source_system"] = normalize_string(raw.get("source_system"), uppercase=True) or "UNKNOWN"
    if "customer_id" in raw:
        normalized["customer_id"] = normalize_string(raw.get("customer_id"))
    if "currency" in raw:
        normalized["currency"] = normalize_string(raw.get("currency"), uppercase=True)
    if "transaction_type" in raw:
        normalized["transaction_type"] = normalize_string(raw.get("transaction_type"), uppercase=True)
    if "transaction_status" in raw:
        normalized["transaction_status"] = (
            normalize_string(raw.get("transaction_status"), uppercase=True) or "COMPLETED"
        )
    if "merchant_category" in raw:
        normalized["merchant_category"] = normalize_string(raw.get("merchant_category"), uppercase=True)

    return normalized


def normalize_transactions(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a batch of transaction dictionaries."""
    return [normalize_record(r) for r in records]
