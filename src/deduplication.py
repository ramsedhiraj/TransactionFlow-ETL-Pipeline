"""
TransactionFlow Deduplication Module
Enforces business key uniqueness within the batch pipeline run and prevents duplicate ingestion.
"""

import json
import logging
from typing import List, Set, Tuple

from src.models import QuarantinedRecord, RejectionReasonEnum, ValidatedTransaction

logger = logging.getLogger("transactionflow.deduplication")


class Deduplicator:
    """
    Maintains seen business keys across streamed batches to detect and quarantine
    duplicates within a pipeline run.
    """

    def __init__(self, check_composite_key: bool = True):
        self.seen_transaction_ids: Set[str] = set()
        self.seen_composite_keys: Set[Tuple[str, str, str, str]] = set()
        self.check_composite_key = check_composite_key

    def identify_duplicates(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> Tuple[List[ValidatedTransaction], List[QuarantinedRecord]]:
        """
        Deduplicates a batch of validated transactions against previously seen keys.

        Args:
            records: Batch of ValidatedTransaction objects.
            pipeline_run_id: Current pipeline run identifier.

        Returns:
            Tuple of (unique_records, duplicate_quarantined_records).
        """
        unique_records: List[ValidatedTransaction] = []
        duplicate_records: List[QuarantinedRecord] = []

        for rec in records:
            # 1. Primary business key: transaction_id
            if rec.transaction_id in self.seen_transaction_ids:
                dup_q = QuarantinedRecord(
                    pipeline_run_id=pipeline_run_id,
                    source_system=rec.source_system,
                    source_record_id=rec.source_record_id,
                    raw_record=json.dumps(
                        {
                            "transaction_id": rec.transaction_id,
                            "customer_id": rec.customer_id,
                            "amount": str(rec.transaction_amount),
                            "timestamp": rec.transaction_timestamp.isoformat(),
                        }
                    ),
                    rejection_reason=RejectionReasonEnum.DUPLICATE_IN_BATCH.value,
                )
                duplicate_records.append(dup_q)
                continue

            # 2. Composite business key: (customer_id, timestamp, amount, type)
            if self.check_composite_key:
                comp_key = (
                    rec.customer_id,
                    rec.transaction_timestamp.isoformat(),
                    str(rec.transaction_amount),
                    rec.transaction_type,
                )
                if comp_key in self.seen_composite_keys:
                    dup_q = QuarantinedRecord(
                        pipeline_run_id=pipeline_run_id,
                        source_system=rec.source_system,
                        source_record_id=rec.source_record_id,
                        raw_record=json.dumps(
                            {
                                "transaction_id": rec.transaction_id,
                                "customer_id": rec.customer_id,
                                "amount": str(rec.transaction_amount),
                                "timestamp": rec.transaction_timestamp.isoformat(),
                                "type": rec.transaction_type,
                            }
                        ),
                        rejection_reason="DUPLICATE_COMPOSITE_KEY",
                    )
                    duplicate_records.append(dup_q)
                    continue
                self.seen_composite_keys.add(comp_key)

            self.seen_transaction_ids.add(rec.transaction_id)
            unique_records.append(rec)

        return unique_records, duplicate_records

    def clear(self):
        """Reset deduplication state."""
        self.seen_transaction_ids.clear()
        self.seen_composite_keys.clear()
