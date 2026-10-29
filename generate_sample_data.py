"""Create a deterministic, small development dataset without external services."""
import argparse, json, random
from datetime import datetime, timedelta
from pathlib import Path

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--out',default='data'); ap.add_argument('--users',type=int,default=120); ap.add_argument('--sessions',type=int,default=800); a=ap.parse_args()
    rng=random.Random(42); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    cats=[{'category_id':f'cat_{i:03d}','name':f'Category {i+1}','subcategories':[{'subcategory_id':f'sub_{i:03d}_00','name':f'Group {i+1}','profit_margin':round(.15+i*.01,2)}]} for i in range(8)]
    products=[{'product_id':f'prod_{i:05d}','name':f'Product {i+1}','category_id':f'cat_{i%8:03d}','subcategory_id':f'sub_{i%8:03d}_00','base_price':round(rng.uniform(8,180),2),'current_stock':1000,'is_active':True,'price_history':[],'creation_date':'2025-01-01T00:00:00'} for i in range(40)]
    users=[{'user_id':f'user_{i:06d}','geo_data':{'city':f'City {i%12+1}','state':f'S{i%8+1}','country':'US'},'registration_date':(datetime(2025,1,1)+timedelta(days=rng.randrange(240))).isoformat(),'last_active':'2025-10-01T00:00:00'} for i in range(a.users)]
    sessions=[]; tx=[]
    for i in range(a.sessions):
        u=rng.choice(users); start=datetime(2025,10,1)+timedelta(minutes=rng.randrange(90*24*60)); pid=f'prod_{rng.randrange(40):05d}'; cv=rng.random()<.24; sid=f'sess_{i:010d}'; cart={pid:{'quantity':1,'price':next(p['base_price'] for p in products if p['product_id']==pid)}} if cv or rng.random()<.15 else {}
        views=[{'timestamp':start.isoformat(),'page_type':'home','product_id':None,'category_id':None,'view_duration':35},{'timestamp':(start+timedelta(seconds=50)).isoformat(),'page_type':'product_detail','product_id':pid,'category_id':next(p['category_id'] for p in products if p['product_id']==pid),'view_duration':80}]
        if cart: views.append({'timestamp':(start+timedelta(seconds=140)).isoformat(),'page_type':'cart','product_id':None,'category_id':None,'view_duration':24})
        sessions.append({'session_id':sid,'user_id':u['user_id'],'start_time':start.isoformat(),'end_time':(start+timedelta(minutes=5)).isoformat(),'duration_seconds':300,'geo_data':u['geo_data'],'device_profile':{'type':rng.choice(['mobile','desktop']),'os':'Android','browser':'Chrome'},'viewed_products':[pid],'page_views':views,'cart_contents':cart,'conversion_status':'converted' if cv else ('abandoned' if cart else 'browsed'),'referrer':rng.choice(['direct','email','social','search_engine'])})
        if cv:
            price=cart[pid]['price']; tx.append({'transaction_id':f'txn_{len(tx):010d}','session_id':sid,'user_id':u['user_id'],'timestamp':(start+timedelta(minutes=5)).isoformat(),'items':[{'product_id':pid,'quantity':1,'unit_price':price,'subtotal':price}],'subtotal':price,'discount':0,'total':price,'payment_method':'credit_card','status':'completed'})
    def save(name,obj): (out/name).write_text(json.dumps(obj),encoding='utf-8')
    save('users.json',users); save('products.json',products); save('categories.json',cats); save('transactions.json',tx)
    for idx in range(0,len(sessions),200): save(f'sessions_{idx//200}.json',sessions[idx:idx+200])
    print(f'Wrote {len(users)} users, {len(products)} products, {len(sessions)} sessions, {len(tx)} transactions to {out}')
if __name__=='__main__': main()
