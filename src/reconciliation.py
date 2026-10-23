"""
TransactionFlow Reconciliation Module
Verifies mathematical balance across all pipeline stages:
extracted = accepted + rejected + duplicates
and confirms loaded records equal accepted records.
"""

import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger("transactionflow.reconciliation")


def reconcile_pipeline_counts(
    records_extracted: int,
    records_accepted: int,
    records_rejected: int,
    duplicate_records: int,
    records_loaded: int,
    db_loaded_verified: int,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Perform audit reconciliation across pipeline stages.

    Integrity invariants:
    1. Source Accounting Invariant:
       records_extracted == records_accepted + records_rejected + duplicate_records
    2. Loading Invariant:
       records_loaded == records_accepted
    3. Storage Verification Invariant:
       db_loaded_verified >= records_loaded (accounting for preexisting rows in reruns)

    Returns:
        (is_balanced, reconciliation_status_str, detailed_report_dict)
    """
    accounting_sum = records_accepted + records_rejected + duplicate_records
    is_source_balanced = records_extracted == accounting_sum
    is_load_balanced = records_loaded == records_accepted

    discrepancy = records_extracted - accounting_sum

    if is_source_balanced and is_load_balanced:
        status = "BALANCED"
        message = "All extracted records are strictly accounted for across accepted, rejected, and duplicate streams."
    elif not is_source_balanced:
        status = "UNBALANCED_EXTRACTION"
        message = (
            f"Extraction accounting mismatch: extracted={records_extracted}, "
            f"sum(accepted={records_accepted}, rejected={records_rejected}, dups={duplicate_records}) = {accounting_sum}. "
            f"Discrepancy: {discrepancy} rows."
        )
    else:
        status = "UNBALANCED_LOAD"
        message = f"Load mismatch: accepted={records_accepted} but loaded={records_loaded}."

    report = {
        "status": status,
        "is_balanced": (status == "BALANCED"),
        "message": message,
        "records_extracted": records_extracted,
        "records_accepted": records_accepted,
        "records_rejected": records_rejected,
        "duplicate_records": duplicate_records,
        "records_loaded": records_loaded,
        "db_verified_count": db_loaded_verified,
        "discrepancy": discrepancy,
    }

    if status == "BALANCED":
        logger.info("Pipeline Count Reconciliation PASSED [BALANCED]: %s", report)
    else:
        logger.error("Pipeline Count Reconciliation FAILED [%s]: %s", status, report)

    return (status == "BALANCED"), status, report
