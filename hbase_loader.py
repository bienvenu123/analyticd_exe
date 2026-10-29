"""Load session events and product/day aggregates through HBase Thrift."""
import argparse
import glob
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import happybase


def reverse_ms(iso):
    dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return 9999999999999 - int(dt.timestamp() * 1000)


def enc(value):
    return str(value).encode('utf-8')


def day_key(iso):
    return datetime.fromisoformat(iso.replace('Z', '+00:00')).strftime('%Y%m%d')


def read_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', default='data')
    ap.add_argument('--host', default='localhost')
    ap.add_argument('--port', type=int, default=9090)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--results', default='output/hbase_results.json')
    args = ap.parse_args()

    conn = happybase.Connection(args.host, port=args.port, timeout=30000)
    events = conn.table('user_events')
    product_daily = conn.table('product_daily')
    row_count = 0
    session_count = 0
    user_ids = set()
    metrics = defaultdict(lambda: {'views': 0, 'cart_adds': 0, 'purchases': 0, 'revenue_cents': 0})

    with events.batch(batch_size=args.batch_size) as batch:
        for path in sorted(glob.glob(str(Path(args.data) / 'sessions_*.json'))):
            for session in read_json(path):
                session_count += 1
                user_ids.add(session['user_id'])
                session_start = session.get('start_time', '')
                day = day_key(session_start) if session_start else ''
                for i, event in enumerate(session.get('page_views') or []):
                    row = (f"{session['user_id']}|{reverse_ms(event['timestamp']):013d}|"
                           f"{session['session_id']}|{i:04d}").encode()
                    batch.put(row, {
                        b's:start_time': enc(session_start),
                        b's:duration_seconds': enc(session.get('duration_seconds', 0)),
                        b's:device': enc(json.dumps(session.get('device_profile', {}))),
                        b's:referrer': enc(session.get('referrer', '')),
                        b's:conversion': enc(session.get('conversion_status', '')),
                        b'e:time': enc(event.get('timestamp', '')),
                        b'e:type': enc(event.get('page_type', '')),
                        b'e:product_id': enc(event.get('product_id') or ''),
                        b'e:category_id': enc(event.get('category_id') or ''),
                        b'e:duration': enc(event.get('view_duration', 0)),
                    })
                    row_count += 1
                    if day and event.get('product_id') and event.get('page_type') == 'product_detail':
                        metrics[(event['product_id'], day)]['views'] += 1

                if day:
                    for product_id, cart_item in (session.get('cart_contents') or {}).items():
                        metrics[(product_id, day)]['cart_adds'] += int(cart_item.get('quantity', 0))

    transaction_paths = sorted(glob.glob(str(Path(args.data) / 'transactions*.json')))
    for path in transaction_paths:
        for transaction in read_json(path):
            if transaction.get('status') not in ('completed', 'shipped', 'delivered'):
                continue
            timestamp = transaction.get('timestamp')
            if not timestamp:
                continue
            day = day_key(timestamp)
            for item in transaction.get('items') or []:
                product_id = item.get('product_id')
                if not product_id:
                    continue
                bucket = metrics[(product_id, day)]
                quantity = int(item.get('quantity', 0))
                bucket['purchases'] += quantity
                bucket['revenue_cents'] += int(round(float(item.get('subtotal', 0)) * 100))

    with product_daily.batch(batch_size=args.batch_size) as batch:
        for (product_id, day), values in metrics.items():
            row = f'{product_id}|{day}'.encode()
            batch.put(row, {f'm:{name}'.encode(): enc(value) for name, value in values.items()})

    actual_event_rows = sum(1 for _ in events.scan(columns=[b'e:time']))
    actual_product_days = sum(1 for _ in product_daily.scan(columns=[b'm:views']))
    conn.close()

    result = {
        'host': args.host,
        'port': args.port,
        'sessions_processed': session_count,
        'distinct_users': len(user_ids),
        'page_view_rows_loaded': row_count,
        'user_events_rows_after_load': actual_event_rows,
        'product_daily_rows_loaded': len(metrics),
        'product_daily_rows_after_load': actual_product_days,
        'product_page_views': sum(v['views'] for v in metrics.values()),
        'cart_add_units': sum(v['cart_adds'] for v in metrics.values()),
        'purchased_units': sum(v['purchases'] for v in metrics.values()),
        'item_revenue_cents': sum(v['revenue_cents'] for v in metrics.values()),
        'row_keys': {
            'user_events': 'user_id|reverse_epoch_ms|session_id|event_index',
            'product_daily': 'product_id|yyyyMMdd',
        },
    }
    results_path = Path(args.results)
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(f"Loaded {row_count:,} page-view rows and {len(metrics):,} product/day rows; results: {results_path}")


if __name__ == '__main__':
    main()
