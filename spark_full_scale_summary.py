"""Memory-bounded full-scale summaries for the assignment-generated JSON chunks."""
import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def read_array(spark, pattern, schema=None):
    reader = spark.read.option('multiLine', True)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.json(pattern)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', default='data_full')
    ap.add_argument('--schema-sample', default='data_assignment')
    ap.add_argument('--out', default='output_full_scale_summary')
    args = ap.parse_args()
    data = Path(args.data)
    sample = Path(args.schema_sample)
    for path in (data / 'users.json', data / 'products.json', data / 'categories.json'):
        if not path.exists():
            ap.error(f'Missing generated data file: {path}')
    if not list(data.glob('sessions_*.json')) or not list(data.glob('transactions*.json')):
        ap.error(f'Expected chunked session and transaction JSON files under {data}')

    spark = SparkSession.builder.appName('AUCA-Full-Scale-Summary').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions', '8')
    try:
        # Infer nested JSON schemas once from the small sample; apply them to all full-scale chunks.
        user_schema = read_array(spark, str(sample / 'users.json')).schema
        product_schema = read_array(spark, str(sample / 'products.json')).schema
        session_schema = read_array(spark, str(sample / 'sessions_*.json')).schema
        tx_schema = read_array(spark, str(sample / 'transactions*.json')).schema

        users = read_array(spark, str(data / 'users.json'), user_schema).select('user_id')
        products = read_array(spark, str(data / 'products.json'), product_schema).select('product_id', 'category_id')
        sessions = read_array(spark, str(data / 'sessions_*.json'), session_schema)
        tx = read_array(spark, str(data / 'transactions*.json'), tx_schema)
        os.makedirs(args.out, exist_ok=True)

        # One bounded row per user, computed from all two million session documents.
        engagement = sessions.groupBy('user_id').agg(
            F.count('*').alias('sessions'),
            F.avg('duration_seconds').alias('avg_session_seconds'),
            F.sum(F.when(F.col('conversion_status') == 'converted', 1).otherwise(0)).alias('converted_sessions'))
        engagement.write.mode('overwrite').parquet(os.path.join(args.out, 'user_engagement'))
        engagement_totals = engagement.agg(
            F.sum('sessions').alias('sessions'),
            F.sum('converted_sessions').alias('converted_sessions'),
            F.count('*').alias('users_with_sessions'),
            F.avg('avg_session_seconds').alias('mean_user_session_seconds')).first().asDict()

        fulfilled = tx.filter(F.col('status').isin('completed', 'shipped', 'delivered'))
        purchases = fulfilled.groupBy('user_id').agg(
            F.countDistinct('transaction_id').alias('orders'),
            F.sum('total').alias('revenue'))
        purchases.write.mode('overwrite').parquet(os.path.join(args.out, 'user_purchases'))
        purchase_totals = purchases.agg(
            F.sum('orders').alias('fulfilled_transactions'),
            F.sum('revenue').alias('fulfilled_revenue'),
            F.count('*').alias('purchasing_users')).first().asDict()

        items = (fulfilled.select('transaction_id', F.explode('items').alias('item'))
                 .select('transaction_id', 'item.product_id', 'item.quantity', 'item.subtotal'))
        category_revenue = (items.join(products, 'product_id')
            .groupBy('category_id')
            .agg(F.sum('subtotal').alias('item_revenue'), F.sum('quantity').alias('units'),
                 F.countDistinct('transaction_id').alias('orders'))
            .orderBy(F.desc('item_revenue')))
        category_revenue.write.mode('overwrite').parquet(os.path.join(args.out, 'category_revenue'))
        category_count = category_revenue.count()
        summary = {
            'source': 'full-scale generated JSON chunks',
            'input_manifest': json.loads((data / 'dataset_manifest.json').read_text(encoding='utf-8')),
            'spark_users': users.count(),
            'session_metrics': engagement_totals,
            'purchase_metrics': purchase_totals,
            'category_rows': category_count,
        }
        # Normalize Spark's returned numeric types for portable JSON.
        def portable(value):
            if hasattr(value, 'item'):
                return value.item()
            return value
        summary = {key: ({k: portable(v) for k, v in value.items()} if isinstance(value, dict) else value)
                   for key, value in summary.items()}
        with open(os.path.join(args.out, 'full_scale_summary.json'), 'w', encoding='utf-8') as stream:
            json.dump(summary, stream, indent=2, default=str)
        print(json.dumps(summary, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
