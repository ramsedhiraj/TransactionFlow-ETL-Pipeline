#!/usr/bin/env python3
"""
TransactionFlow CLI Runner
Command-line interface to execute the TransactionFlow ETL and risk monitoring pipeline.
"""

import argparse
import json
import sys
from pathlib import Path

from src.config import settings
from src.database import DatabaseManager
from src.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="TransactionFlow: High-Volume Transaction ETL & Risk Monitoring Pipeline",
    )
    parser.add_argument(
        "--source-type",
        type=str,
        default="CSV",
        choices=["CSV", "API", "REST_API"],
        help="Input source type (CSV file or paginated REST API)",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="data/transactions_100k.csv",
        help="Source path (CSV file) or Base URL (for API)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.DEFAULT_BATCH_SIZE,
        help="In-flight batch size for streaming extraction & database loads",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="SQLAlchemy database connection string (overrides .env)",
    )
    parser.add_argument(
        "--api-pages",
        type=int,
        default=None,
        help="Maximum pages to ingest if source-type is API",
    )
    parser.add_argument(
        "--api-page-size",
        type=int,
        default=500,
        help="Page size for REST API queries",
    )
    parser.add_argument(
        "--summary-json",
        type=str,
        default=None,
        help="Optional path to write JSON execution summary",
    )

    args = parser.parse_args()

    db_manager = DatabaseManager(db_url=args.db_url) if args.db_url else DatabaseManager()

    try:
        run_record = run_pipeline(
            source_type=args.source_type,
            source_target=args.source,
            batch_size=args.batch_size,
            db_manager=db_manager,
            api_page_size=args.api_page_size,
            api_max_pages=args.api_pages,
        )

        # Print formatted summary table
        print("\n" + "=" * 65)
        print("          TRANSACTIONFLOW PIPELINE EXECUTION SUMMARY")
        print("=" * 65)
        print(f"Run ID:                 {run_record.run_id}")
        print(f"Status:                 {run_record.status}")
        print(f"Source:                 {run_record.source_type} ({run_record.source_name})")
        print(f"Records Extracted:      {run_record.records_extracted:,}")
        print(f"Records Accepted:       {run_record.records_accepted:,}")
        print(f"Records Rejected:       {run_record.records_rejected:,}")
        print(f"Duplicate Records:      {run_record.duplicate_records:,}")
        print(f"Records Loaded:         {run_record.records_loaded:,}")
        print(f"Risk Flags Raised:      {run_record.risk_flagged_records:,}")
        print(f"Reconciliation:         {run_record.reconciliation_status}")
        print(f"Execution Duration:     {run_record.duration_seconds:.3f} seconds")
        if run_record.duration_seconds and run_record.duration_seconds > 0:
            rate = run_record.records_extracted / run_record.duration_seconds
            print(f"Processing Throughput:  {rate:,.1f} rows/second")
        print("=" * 65 + "\n")

        if args.summary_json:
            out_file = Path(args.summary_json).resolve()
            out_file.parent.mkdir(parents=True, exist_ok=True)
            summary_dict = run_record.model_dump(mode="json")
            out_file.write_text(json.dumps(summary_dict, indent=2), encoding="utf-8")
            print(f"Execution summary written to: {out_file}")

        if run_record.status == "FAILED":
            sys.exit(1)

    except Exception as exc:
        print(f"\n[FATAL] Pipeline execution failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
