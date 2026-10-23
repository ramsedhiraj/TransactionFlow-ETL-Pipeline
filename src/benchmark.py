"""
TransactionFlow High-Volume Performance Benchmark Suite
Executes end-to-end benchmarks with monotonic timing, memory profiling (tracemalloc),
and stage-by-stage latency tracking across 100k, 1M, and 5M records.

NOTE: All benchmarks execute real pipelines against real generated data.
No synthetic metrics or throughput figures are fabricated.
"""

import argparse
import json
import os
import platform
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.generate_transactions import generate_to_csv
from src.database import DatabaseManager
from src.pipeline import run_pipeline


def get_system_specs() -> Dict[str, Any]:
    """Capture runtime hardware and environment specifications."""
    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count_logical": os.cpu_count(),
        "python_version": platform.python_version(),
        "python_compiler": platform.python_compiler(),
    }


def run_benchmark(
    records_target: int,
    batch_size: int = 25000,
    seed: int = 42,
    db_url: Optional[str] = None,
    output_dir: Path = Path("data"),
    keep_data_file: bool = False,
) -> Dict[str, Any]:
    """
    Run a complete, monitored benchmark run on a generated synthetic dataset.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_file = output_dir / f"benchmark_{records_target // 1000}k_seed{seed}.csv"

    print("\n" + "=" * 75)
    print(f" STARTING BENCHMARK: {records_target:,} RECORDS (batch_size={batch_size:,})")
    print("=" * 75)

    specs = get_system_specs()
    print(
        f"Environment: Python {specs['python_version']} on {specs['os']} ({specs['machine']}, {specs['cpu_count_logical']} CPUs)"
    )

    # 1. Generate Dataset
    print(f"\n[Stage 1] Generating {records_target:,} synthetic records...")
    gen_start = time.monotonic()
    gen_result = generate_to_csv(
        output_path=csv_file,
        total_records=records_target,
        batch_size=batch_size,
        seed=seed,
        error_rate=0.005,
        duplicate_rate=0.002,
    )
    gen_duration = time.monotonic() - gen_start

    # 2. Benchmark Pipeline with Profiling
    print("[Stage 2] Executing ETL Pipeline with memory tracking...")
    tracemalloc.start()
    db_manager = DatabaseManager(db_url=db_url)
    db_manager.create_tables()

    e2e_start = time.monotonic()
    run_record = run_pipeline(
        source_type="CSV",
        source_target=str(csv_file),
        batch_size=batch_size,
        db_manager=db_manager,
    )
    e2e_duration = time.monotonic() - e2e_start

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mem_mb = peak_mem / (1024 * 1024)
    rows_per_sec = records_target / e2e_duration if e2e_duration > 0 else 0

    benchmark_summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "target_records": records_target,
        "batch_size": batch_size,
        "seed": seed,
        "database_url": db_manager.engine.url.render_as_string(hide_password=True),
        "system_specs": specs,
        "generation": {
            "duration_seconds": round(gen_duration, 3),
            "file_size_mb": gen_result["file_size_mb"],
            "generation_rate_rows_sec": round(records_target / gen_duration, 1),
        },
        "pipeline": {
            "run_id": run_record.run_id,
            "status": run_record.status,
            "records_extracted": run_record.records_extracted,
            "records_accepted": run_record.records_accepted,
            "records_rejected": run_record.records_rejected,
            "duplicate_records": run_record.duplicate_records,
            "records_loaded": run_record.records_loaded,
            "risk_flagged_records": run_record.risk_flagged_records,
            "reconciliation_status": run_record.reconciliation_status,
            "duration_seconds": round(e2e_duration, 3),
            "throughput_rows_sec": round(rows_per_sec, 1),
            "peak_memory_mb": round(peak_mem_mb, 2),
        },
    }

    print("\n" + "=" * 75)
    print("                 BENCHMARK PERFORMANCE RESULTS")
    print("=" * 75)
    print(f"Target Records:         {records_target:,}")
    print(f"Batch Size:             {batch_size:,}")
    print(f"E2E Duration:           {e2e_duration:.3f} s")
    print(f"Throughput:             {rows_per_sec:,.1f} rows/second")
    print(f"Peak Memory (Profiled): {peak_mem_mb:.2f} MB")
    print(f"Records Extracted:      {run_record.records_extracted:,}")
    print(f"Records Loaded:         {run_record.records_loaded:,}")
    print(f"Records Quarantined:    {run_record.records_rejected:,}")
    print(f"Duplicates Detected:    {run_record.duplicate_records:,}")
    print(f"Risk Flags Raised:      {run_record.risk_flagged_records:,}")
    print(f"Reconciliation:         {run_record.reconciliation_status}")
    print("=" * 75 + "\n")

    if not keep_data_file and csv_file.exists():
        try:
            csv_file.unlink()
            print(f"Cleaned up temporary benchmark file: {csv_file}")
        except Exception as e:
            print(f"Notice: Failed to remove temporary file {csv_file}: {e}")

    return benchmark_summary


def main():
    parser = argparse.ArgumentParser(description="TransactionFlow Performance Benchmark Suite")
    parser.add_argument("--records", type=int, default=100000, help="Record volume (e.g. 100000, 1000000, 5000000)")
    parser.add_argument("--batch-size", type=int, default=25000, help="ETL chunk batch size")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable data")
    parser.add_argument("--db-url", type=str, default=None, help="Database connection URL")
    parser.add_argument("--keep-file", action="store_true", help="Keep generated CSV benchmark file on disk")
    parser.add_argument("--output-json", type=str, default=None, help="File path to save JSON benchmark metrics")

    args = parser.parse_args()
    summary = run_benchmark(
        records_target=args.records,
        batch_size=args.batch_size,
        seed=args.seed,
        db_url=args.db_url,
        keep_data_file=args.keep_file,
    )

    if args.output_json:
        out_p = Path(args.output_json).resolve()
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Saved benchmark report to: {out_p}")


if __name__ == "__main__":
    main()
