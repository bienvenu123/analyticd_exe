"""Fetch recent event rows for one HBase user key prefix."""
import argparse
import json
from pathlib import Path

import happybase


def prefix_successor(prefix):
    value = bytearray(prefix)
    for index in range(len(value) - 1, -1, -1):
        if value[index] != 255:
            value[index] += 1
            return bytes(value[:index + 1])
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--user-id', required=True)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=9090)
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--out', default='output/hbase_user_query.json')
    args = parser.parse_args()

    prefix = f'{args.user_id}|'.encode('utf-8')
    conn = happybase.Connection(args.host, port=args.port, timeout=30000)
    table = conn.table('user_events')
    rows = []
    for row_key, columns in table.scan(row_start=prefix, row_stop=prefix_successor(prefix)):
        parts = row_key.decode('utf-8').split('|')
        rows.append({
            'user_id': parts[0],
            'reverse_epoch_ms': parts[1],
            'session_id': parts[2],
            'event_index': int(parts[3]),
            'event_time': columns.get(b'e:time', b'').decode('utf-8'),
            'page_type': columns.get(b'e:type', b'').decode('utf-8'),
            'product_id': columns.get(b'e:product_id', b'').decode('utf-8') or None,
            'category_id': columns.get(b'e:category_id', b'').decode('utf-8') or None,
            'view_duration': int(columns.get(b'e:duration', b'0')),
            'conversion': columns.get(b's:conversion', b'').decode('utf-8'),
        })
        if len(rows) >= args.limit:
            break
    conn.close()

    result = {
        'user_id': args.user_id,
        'rows_returned': len(rows),
        'query': 'user_id prefix scan; reverse timestamp orders recent events first',
        'events': rows,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(f"Retrieved {len(rows)} event rows for {args.user_id}; saved {out}")


if __name__ == '__main__':
    main()
