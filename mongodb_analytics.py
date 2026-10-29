"""MongoDB JSON loader and aggregation examples for the AUCA project."""
import argparse, glob, json, os
from pathlib import Path
from pymongo import MongoClient, ASCENDING, DESCENDING

DB = "auca_ecommerce"

def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def load(data_dir, limit=None):
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    db = client[DB]
    db.sessions.delete_many({})
    db.transactions.delete_many({})
    specs = [("users", "users.json"), ("products", "products.json"),
             ("categories", "categories.json")]
    for coll, filename in specs:
        path = Path(data_dir) / filename
        if path.exists():
            rows = read_json(path)
            if limit: rows = rows[:limit]
            if rows:
                db[coll].delete_many({})
                db[coll].insert_many(rows, ordered=False)
    for path in sorted(Path(data_dir).glob("transactions*.json")):
        rows = read_json(path)
        if limit:
            rows = rows[:limit]
        if rows:
            db.transactions.insert_many(rows, ordered=False)
    for path in sorted(glob.glob(str(Path(data_dir) / "sessions_*.json"))):
        rows = read_json(path)
        if limit: rows = rows[:limit]
        if rows:
            db.sessions.insert_many(rows, ordered=False)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING), ("is_active", ASCENDING)])
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("timestamp", ASCENDING), ("status", ASCENDING)])
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    client.close()

def pipelines(db):
    # Unwind transaction items, enrich from product catalog, then rank by units and revenue.
    popularity = [{"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
      {"$unwind": "$items"}, {"$group": {"_id": "$items.product_id", "units": {"$sum": "$items.quantity"},
       "item_revenue": {"$sum": "$items.subtotal"}, "orders": {"$addToSet": "$transaction_id"}}},
      {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product_id", "as": "product"}},
      {"$unwind": {"path": "$product", "preserveNullAndEmptyArrays": True}},
      {"$project": {"_id": 0, "product_id": "$_id", "name": "$product.name", "category_id": "$product.category_id",
       "units": 1, "item_revenue": 1, "order_count": {"$size": "$orders"}}},
      {"$sort": {"units": -1}}, {"$limit": 20}]
    # Category revenue after item and product joins; transaction status is explicit to avoid counting failed orders.
    category = [{"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
      {"$unwind": "$items"}, {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "product_id", "as": "p"}},
      {"$unwind": "$p"}, {"$group": {"_id": "$p.category_id", "revenue": {"$sum": "$items.subtotal"},
       "units": {"$sum": "$items.quantity"}, "transactions": {"$addToSet": "$transaction_id"}}},
      {"$project": {"_id": 0, "category_id": "$_id", "revenue": 1, "units": 1, "orders": {"$size": "$transactions"}}},
      {"$sort": {"revenue": -1}}]
    return popularity, category

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--data", default="data"); ap.add_argument("--limit", type=int)
    ap.add_argument("--no-load", action="store_true"); ap.add_argument("--out", default="output/mongodb_results.json"); args = ap.parse_args()
    if not args.no_load: load(args.data, args.limit)
    client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017")); db = client[DB]
    results = {name: list(db.transactions.aggregate(pipeline, allowDiskUse=True))
               for name, pipeline in zip(("top_products", "category_revenue"), pipelines(db))}
    results["collection_counts"] = {name: db[name].count_documents({}) for name in ("users", "products", "categories", "sessions", "transactions")}
    output = Path(args.out); output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    for name in ("top_products", "category_revenue"):
        print(name, json.dumps(results[name][:5], default=str))
    print("collection_counts", json.dumps(results["collection_counts"]))
    print("saved", output.resolve())
    client.close()
if __name__ == "__main__": main()
