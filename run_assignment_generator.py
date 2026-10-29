"""Run the code cell embedded in the supplied notebook-JSON generator.

The generator logic is kept intact apart from its scale constants and bounded
output buffers, so the original defaults can run without retaining millions of
session objects in memory.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent


def patch_source(source, args):
    replacements = {
        'NUM_USERS': args.users,
        'NUM_PRODUCTS': args.products,
        'NUM_CATEGORIES': args.categories,
        'NUM_TRANSACTIONS': args.transactions,
        'NUM_SESSIONS': args.sessions,
    }
    for name, value in replacements.items():
        source, n = re.subn(rf'^{name}\s*=\s*\d+', f'{name} = {value}', source,
                            count=1, flags=re.M)
        if n != 1:
            raise ValueError(f'Expected one {name} assignment in supplied notebook cell')

    state_anchor = 'iteration = 0\n'
    if state_anchor not in source:
        raise ValueError('Could not find generator iteration state')
    source = source.replace(state_anchor, state_anchor + (
        f'SESSION_CHUNK_SIZE = {args.session_chunk_size}\n'
        f'TRANSACTION_CHUNK_SIZE = {args.transaction_chunk_size}\n'
        'session_file_index = 0\ntransaction_file_index = 0\n'), 1)

    progress_anchor = '''    if iteration % 10000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")
'''
    flush_code = progress_anchor + '''
    if len(sessions) >= SESSION_CHUNK_SIZE:
        with open(f"sessions_{session_file_index}.json", "w") as f:
            json.dump(sessions, f)
        sessions.clear()
        session_file_index += 1

    if len(transactions) >= TRANSACTION_CHUNK_SIZE:
        with open(f"transactions_{transaction_file_index}.json", "w") as f:
            json.dump(transactions, f)
        transactions.clear()
        transaction_file_index += 1
'''
    if progress_anchor not in source:
        raise ValueError('Could not find generator progress block')
    source = source.replace(progress_anchor, flush_code, 1)

    tx_export = '''# Save transactions
with open("transactions.json", "w") as f:
    json.dump(transactions, f, default=json_serializer)
'''
    tx_replacement = '''# Save the final transaction buffer. Transaction chunks are read together by the project loaders.
if transactions:
    with open(f"transactions_{transaction_file_index}.json", "w") as f:
        json.dump(transactions, f, default=json_serializer)
    transaction_file_index += 1
'''
    if tx_export not in source:
        raise ValueError('Could not find transaction export block')
    source = source.replace(tx_export, tx_replacement, 1)

    session_export = '''# Save sessions in chunks
CHUNK_SIZE = 100000
for i in range(0, len(sessions), CHUNK_SIZE):
    chunk = sessions[i:i+CHUNK_SIZE]
    with open(f"sessions_{i//CHUNK_SIZE}.json", "w") as f:
        json.dump(chunk, f, default=json_serializer)
'''
    session_replacement = '''# Save the final session buffer.
if sessions:
    with open(f"sessions_{session_file_index}.json", "w") as f:
        json.dump(sessions, f, default=json_serializer)
    session_file_index += 1

with open("dataset_manifest.json", "w", encoding="utf-8") as f:
    json.dump({"users": len(users), "products": len(products),
               "categories": len(categories), "sessions": session_counter,
               "transactions": transaction_counter,
               "session_chunk_size": SESSION_CHUNK_SIZE,
               "transaction_chunk_size": TRANSACTION_CHUNK_SIZE}, f, indent=2)
'''
    if session_export not in source:
        raise ValueError('Could not find session export block')
    source = source.replace(session_export, session_replacement, 1)
    source = source.replace('- Sessions: {len(sessions):,} (target: {NUM_SESSIONS:,})',
                            '- Sessions: {session_counter:,} (target: {NUM_SESSIONS:,})')
    source = source.replace('- Transactions: {len(transactions):,} (target: {NUM_TRANSACTIONS:,})',
                            '- Transactions: {transaction_counter:,} (target: {NUM_TRANSACTIONS:,})')
    return source


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--out', default='data_assignment')
    ap.add_argument('--users', type=int, default=1000)
    ap.add_argument('--products', type=int, default=500)
    ap.add_argument('--categories', type=int, default=15)
    ap.add_argument('--sessions', type=int, default=10000)
    ap.add_argument('--transactions', type=int, default=2500)
    ap.add_argument('--session-chunk-size', type=int, default=25000)
    ap.add_argument('--transaction-chunk-size', type=int, default=10000)
    ap.add_argument('--overwrite', action='store_true')
    args = ap.parse_args()
    if min(args.users, args.products, args.categories, args.sessions,
           args.transactions, args.session_chunk_size, args.transaction_chunk_size) <= 0:
        ap.error('all scales and chunk sizes must be positive')

    payload = json.loads((ROOT / 'dataset_generator.py').read_text(encoding='utf-8'))
    source = patch_source(''.join(payload['cells'][0]['source']), args)
    output = (ROOT / args.out).resolve()
    if output.exists() and any(output.iterdir()) and not args.overwrite:
        ap.error(f'{output} already contains files; choose another --out or pass --overwrite')
    output.mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for pattern in ('users.json', 'products.json', 'categories.json',
                        'transactions*.json', 'sessions_*.json', 'dataset_manifest.json'):
            for old_file in output.glob(pattern):
                if old_file.is_file():
                    old_file.unlink()

    with tempfile.NamedTemporaryFile('w', suffix='.py', encoding='utf-8', delete=False) as f:
        temp = Path(f.name)
        f.write(source)
    try:
        env = os.environ.copy()
        vendor = ROOT / '.tmp_faker'
        if vendor.exists():
            env['PYTHONPATH'] = os.pathsep.join(filter(None, [str(vendor.resolve()), env.get('PYTHONPATH', '')]))
        print(f'Generating in {output}: {args.users:,} users, {args.products:,} products, '
              f'{args.sessions:,} sessions, {args.transactions:,} transactions target.', flush=True)
        subprocess.run([sys.executable, str(temp)], cwd=output, env=env, check=True)
    finally:
        temp.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
