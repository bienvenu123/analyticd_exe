"""Short, deterministic simulated ecommerce event stream using Spark Structured Streaming."""
import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='output/streaming_results.json')
    ap.add_argument('--duration-seconds', type=int, default=20)
    ap.add_argument('--rows-per-second', type=int, default=30)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out) or '.', exist_ok=True)
    spark = SparkSession.builder.appName('AUCA-Simulated-Streaming').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    rate = spark.readStream.format('rate').option('rowsPerSecond', args.rows_per_second).load()
    events = (rate.withColumn('user_id', F.format_string('user_%06d', F.pmod(F.col('value'), F.lit(1000))))
        .withColumn('product_id', F.format_string('product_%05d', F.pmod(F.col('value') * 17, F.lit(5000))))
        .withColumn('page_type', F.when(F.col('value') % 10 < 6, 'product_detail')
                    .when(F.col('value') % 10 < 9, 'cart').otherwise('checkout'))
        .withColumn('category_id', F.format_string('category_%03d', F.pmod(F.col('value') * 7, F.lit(25)))))
    windowed = (events.withWatermark('timestamp', '10 seconds')
        .groupBy(F.window('timestamp', '5 seconds'), 'page_type', 'category_id')
        .agg(F.count('*').alias('event_count'), F.approx_count_distinct('user_id').alias('approx_unique_users')))
    batches = []
    def consume(batch_df, batch_id):
        rows = [r.asDict(recursive=True) for r in batch_df.collect()]
        for row in rows:
            row['window']['start'] = row['window']['start'].isoformat()
            row['window']['end'] = row['window']['end'].isoformat()
        batches.append({'batch_id': batch_id, 'groups': rows})
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump({'source': 'Spark rate stream (synthetic)', 'duration_seconds': args.duration_seconds,
                       'rows_per_second': args.rows_per_second, 'microbatches': batches}, f, indent=2)
    query = windowed.writeStream.outputMode('update').foreachBatch(consume).trigger(processingTime='2 seconds').start()
    deadline = time.time() + args.duration_seconds
    try:
        while query.isActive and time.time() < deadline:
            time.sleep(0.5)
    finally:
        query.stop()
        spark.stop()
    print(f'Wrote {len(batches)} microbatches to {args.out}')


if __name__ == '__main__':
    main()
