"""Create the project tables through the HBase Thrift API."""
import argparse
import happybase


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=9090)
    args = parser.parse_args()

    connection = happybase.Connection(args.host, port=args.port, timeout=30000)
    existing = set(connection.tables())
    if b'user_events' not in existing:
        connection.create_table('user_events', {
            's': {'max_versions': 1},
            'e': {'max_versions': 1},
        })
    if b'product_daily' not in existing:
        connection.create_table('product_daily', {
            'm': {'max_versions': 1},
        })
    connection.close()
    print('HBase tables ready: user_events, product_daily')


if __name__ == '__main__':
    main()
