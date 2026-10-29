# AUCA E-commerce Big Data Analytics

Local implementation of the Oct 2026 brief. The supplied `dataset_generator.py` is a notebook JSON export with a Python code cell, so `run_assignment_generator.py` extracts and runs that cell at the chosen scale. Data is synthetic.

## Components

- `mongodb_analytics.py`: imports JSON collections into MongoDB and runs aggregation pipelines.
- `hbase_setup.py`, `hbase_loader.py`, `hbase_query.py`: create HBase tables, load user events and product/day counters, and execute a user-prefix scan.
- `spark_analytics.py`: Spark batch analytics and optional direct integration across MongoDB and HBase.
- `spark_full_scale_summary.py`: bounded Spark summaries over the full two-million-session dataset.
- `spark_streaming_simulation.py`: bounded Spark Structured Streaming simulation from a generated event stream.
- `TECHNICAL_REPORT.pdf`: technical report.

## Reproduce

1. The `data_assignment/` dataset is a reduced sample (1,000 users, 10,000 sessions, 2,500 transactions) used to populate local MongoDB and HBase. Generate the brief's full scale (10,000 users, 5,000 products, 25 categories, 500,000 transactions, 2,000,000 sessions) in bounded files with:

   ```powershell
   python run_assignment_generator.py --out data_full --users 10000 --products 5000 --categories 25 --sessions 2000000 --transactions 500000
   ```

   The runner writes 25,000-session and 10,000-transaction chunks plus `dataset_manifest.json`.

2. Install packages with `pip install -r requirements.txt`. Spark requires a compatible Java and Python runtime. Set `PYSPARK_PYTHON` to your Python executable on Windows if Spark cannot launch `python3`.

3. Import and query MongoDB (default `mongodb://localhost:27017`):

   ```powershell
   python mongodb_analytics.py --data data_assignment
   ```

4. Run Spark's file-based batch analytics:

   ```powershell
   spark-submit --master 'local[*]' spark_analytics.py --data data_assignment --out output
   ```

   For the full dataset, run the bounded full-scale session, purchase, and category summaries after generation:

   ```powershell
   spark-submit --master 'local[2]' spark_full_scale_summary.py --data data_full --schema-sample data_assignment --out output_full_scale_summary
   ```

   The script reuses schemas inferred from the small sample, processes the full chunks, and writes compact user-level Parquet results plus `full_scale_summary.json`.

5. Run the Spark join with MongoDB profiles/orders and HBase user events directly. Both services must be running (MongoDB URI can be set in `MONGODB_URI`; HBase Thrift defaults to localhost:9090):

   ```powershell
   spark-submit --master 'local[*]' spark_analytics.py --source-mode stores --out output
   ```

6. Run the synthetic Spark microbatch example:

   ```powershell
   spark-submit --master 'local[*]' spark_streaming_simulation.py --duration-seconds 40 --rows-per-second 10 --out output/streaming_results.json
   ```

7. Start/check local HBase, create the tables, load assignment events and product/day aggregates, then query a specific user:

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_hbase_local.ps1
   ```

   The default user is `user_000042`. Results are in `output/hbase_results.json` and `output/hbase_user_query.json`; HBase logs are in `hbase_runtime/logs`.

The full-scale dataset is generated locally and intentionally excluded from Git because it is multi-gigabyte; `results/full_dataset_manifest.json` records its scale. MongoDB and HBase loads and direct-store integration use the assignment-scale sample to keep local services practical. The `results/` directory contains small verified result snapshots and charts; Parquet outputs, HBase runtime files, Python caches and local service configuration remain local.
