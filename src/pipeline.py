"""
TransactionFlow Main Pipeline Orchestrator
Coordinates streaming extraction, validation, deduplication, heuristic risk flagging,
quarantine routing, bulk database loading, and audit reconciliation.
"""

import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from src.config import settings
from src.database import DatabaseManager
from src.deduplication import Deduplicator
from src.extract import extract_api_pages, extract_csv_batches
from src.models import (
    PipelineRunRecord,
    PipelineStatusEnum,
)
from src.reconciliation import reconcile_pipeline_counts
from src.risk_rules import RiskEngine
from src.validation import validate_transaction_records

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("transactionflow.pipeline")


def run_pipeline(
    source_type: str,
    source_target: str,
    batch_size: int = 10000,
    db_manager: Optional[DatabaseManager] = None,
    risk_engine: Optional[RiskEngine] = None,
    api_page_size: int = 500,
    api_max_pages: Optional[int] = None,
    api_checkpoint: Optional[str] = None,
    api_client: Optional[Any] = None,
) -> PipelineRunRecord:
    """
    Execute end-to-end TransactionFlow ETL pipeline.

    Args:
        source_type: "CSV" or "REST_API"
        source_target: File path (for CSV) or Base URL (for REST_API)
        batch_size: In-flight streaming chunk size
        db_manager: Optional DatabaseManager instance
        risk_engine: Optional RiskEngine instance
        api_page_size: Records per API page
        api_max_pages: Optional page limit for API ingestion
        api_checkpoint: Checkpoint timestamp for incremental API runs
        api_client: Optional httpx.Client for testing/mocking

    Returns:
        Completed PipelineRunRecord with full execution metrics and reconciliation.
    """
    run_id = f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
    start_time = time.monotonic()
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 70)
    logger.info("STARTING PIPELINE RUN: %s", run_id)
    logger.info("Source Type: %s | Source Target: %s | Batch Size: %d", source_type, source_target, batch_size)
    logger.info("=" * 70)

    # Initialize components
    db = db_manager or DatabaseManager()
    db.create_tables()

    deduplicator = Deduplicator(check_composite_key=True)
    engine = risk_engine or RiskEngine()

    run_record = PipelineRunRecord(
        run_id=run_id,
        source_type=source_type.upper(),
        source_name=str(source_target),
        status=PipelineStatusEnum.RUNNING.value,
        started_at=started_at,
    )
    db.record_run_start(run_record)

    total_extracted = 0
    total_accepted = 0
    total_rejected = 0
    total_duplicates = 0
    total_risk_flags = 0
    total_loaded = 0

    try:
        # Choose extraction source
        if source_type.upper() == "CSV":
            extractor = (
                (batch, None)
                for batch in extract_csv_batches(
                    file_path=source_target,
                    batch_size=batch_size,
                )
            )
        elif source_type.upper() in ("REST_API", "API"):
            extractor = extract_api_pages(
                base_url=source_target,
                page_size=api_page_size,
                max_pages=api_max_pages,
                checkpoint_timestamp=api_checkpoint,
                client=api_client,
            )
        else:
            raise ValueError(f"Unsupported source_type: '{source_type}'. Expected 'CSV' or 'REST_API'.")

        batch_idx = 0
        for raw_batch, checkpoint in extractor:
            batch_idx += 1
            batch_count = len(raw_batch)
            total_extracted += batch_count

            # 1. Schema & Data Quality Validation
            valid_recs, quarantined_recs = validate_transaction_records(
                raw_records=raw_batch,
                pipeline_run_id=run_id,
            )
            total_rejected += len(quarantined_recs)

            # 2. In-flight Business Key Deduplication
            unique_recs, duplicate_quarantined = deduplicator.identify_duplicates(
                records=valid_recs,
                pipeline_run_id=run_id,
            )
            total_duplicates += len(duplicate_quarantined)

            # 3. Route All Quarantine Records (Schema Rejections + In-batch Duplicates)
            all_quarantine = quarantined_recs + duplicate_quarantined
            if all_quarantine:
                db.load_quarantine_records(all_quarantine)

            # 4. Suspicious Transaction Risk Flagging
            if unique_recs:
                flags = engine.flag_suspicious_transactions(
                    records=unique_recs,
                    pipeline_run_id=run_id,
                )
                if flags:
                    db.load_risk_flags(flags)
                    total_risk_flags += len(flags)

                # 5. High-Throughput Idempotent Batch Loading
                loaded_count = db.load_transaction_batch(unique_recs)
                total_accepted += len(unique_recs)
                total_loaded += loaded_count

            if batch_idx % 10 == 0 or total_extracted >= 100000:
                elapsed = time.monotonic() - start_time
                logger.info(
                    "Progress [Batch #%d]: Extracted=%d | Accepted=%d | Rejected=%d | Dups=%d | Loaded=%d (%.0f rows/s)",
                    batch_idx,
                    total_extracted,
                    total_accepted,
                    total_rejected,
                    total_duplicates,
                    total_loaded,
                    total_extracted / elapsed if elapsed > 0 else 0,
                )

        # 6. Post-Run Database Row Count Verification
        db_verified_count = db.count_transactions(pipeline_run_id=run_id)

        # 7. Audit Count Reconciliation
        is_balanced, recon_status, recon_report = reconcile_pipeline_counts(
            records_extracted=total_extracted,
            records_accepted=total_accepted,
            records_rejected=total_rejected,
            duplicate_records=total_duplicates,
            records_loaded=total_loaded,
            db_loaded_verified=db_verified_count,
        )

        duration = time.monotonic() - start_time
        final_status = PipelineStatusEnum.SUCCESS.value if is_balanced else PipelineStatusEnum.PARTIAL_SUCCESS.value

        run_record.status = final_status
        run_record.records_extracted = total_extracted
        run_record.records_accepted = total_accepted
        run_record.records_rejected = total_rejected
        run_record.duplicate_records = total_duplicates
        run_record.risk_flagged_records = total_risk_flags
        run_record.records_loaded = total_loaded
        run_record.completed_at = datetime.now(timezone.utc)
        run_record.duration_seconds = round(duration, 3)
        run_record.reconciliation_status = recon_status
        run_record.error_message = None if is_balanced else recon_report.get("message")

        db.record_run_end(run_record)

        logger.info("=" * 70)
        logger.info("PIPELINE RUN COMPLETED: %s [%s]", run_id, final_status)
        logger.info(
            "Extracted: %d | Accepted: %d | Rejected: %d | Duplicates: %d | Loaded: %d | Risk Flags: %d",
            total_extracted,
            total_accepted,
            total_rejected,
            total_duplicates,
            total_loaded,
            total_risk_flags,
        )
        logger.info(
            "Duration: %.2fs | Throughput: %.1f rows/sec | Reconciliation: %s",
            duration,
            total_extracted / duration if duration > 0 else 0,
            recon_status,
        )
        logger.info("=" * 70)

        return run_record

    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.exception("Pipeline run %s encountered unhandled error: %s", run_id, exc)

        run_record.status = PipelineStatusEnum.FAILED.value
        run_record.records_extracted = total_extracted
        run_record.records_accepted = total_accepted
        run_record.records_rejected = total_rejected
        run_record.duplicate_records = total_duplicates
        run_record.records_loaded = total_loaded
        run_record.risk_flagged_records = total_risk_flags
        run_record.completed_at = datetime.now(timezone.utc)
        run_record.duration_seconds = round(duration, 3)
        run_record.error_message = f"{type(exc).__name__}: {str(exc)}"
        run_record.reconciliation_status = "FAILED"

        try:
            db.record_run_end(run_record)
        except Exception as db_err:
            logger.error("Failed to update pipeline run failure status: %s", db_err)

        raise
