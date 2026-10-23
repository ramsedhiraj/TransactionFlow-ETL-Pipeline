# TransactionFlow Data Directory

This directory holds generated synthetic transactions, quarantine files, and checkpoints for local runs.

## Dataset Strategy
- **Fictional Synthetic Data**: All generated transaction records are synthetic and deterministically generated for benchmarking and stress testing. They do not represent real banking or customer data.
- **Ignored in Git**: Large CSV and Parquet files generated during benchmarks are excluded by `.gitignore` to prevent repository bloat.
- **Generation**: To regenerate benchmark datasets:
  ```bash
  python scripts/generate_transactions.py --records 100000 --output data/transactions_100k.csv --seed 42
  python scripts/generate_transactions.py --records 1000000 --output data/transactions_1m.csv --seed 42
  ```
