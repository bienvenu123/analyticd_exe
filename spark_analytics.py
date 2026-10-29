"""Spark batch analytics: normalization, cohorts, co-purchase, funnel, CLV and plots."""
import argparse, glob, json, os
from pyspark.sql import SparkSession, functions as F, types as T

def json_df(spark, path):
    return spark.read.option('multiLine', True).json(path)

def run_store_integration(spark, args):
    """Build user metrics directly from MongoDB profiles/orders and HBase events."""
    import happybase
    from pymongo import MongoClient
    mongo = MongoClient(args.mongo_uri)
    db = mongo['auca_ecommerce']
    profile_rows = [(r['user_id'], str(r.get('registration_date') or ''),
                     str((r.get('geo_data') or {}).get('country') or ''))
                    for r in db.users.find({}, {'_id': 0, 'user_id': 1, 'registration_date': 1, 'geo_data.country': 1})]
    purchase_rows = [(r['_id'], int(r['orders']), float(r['revenue'] or 0), str(r.get('last_purchase') or ''))
                     for r in db.transactions.aggregate([
        {'$match': {'status': {'$in': ['completed', 'shipped', 'delivered']}}},
        {'$group': {'_id': '$user_id', 'orders': {'$addToSet': '$transaction_id'},
                    'revenue': {'$sum': '$total'}, 'last_purchase': {'$max': '$timestamp'}}},
        {'$project': {'orders': {'$size': '$orders'}, 'revenue': 1, 'last_purchase': 1}}])]
    mongo.close()
    hbase = happybase.Connection(args.hbase_host, port=args.hbase_port, timeout=30000)
    by_session = {}
    for key, cells in hbase.table('user_events').scan(columns=[b'e:duration', b's:conversion']):
        parts = key.decode().split('|', 3)
        if len(parts) < 3: continue
        uid, sid = parts[0], parts[2]
        rec = by_session.setdefault((uid, sid), [0, False])
        rec[0] += int(cells.get(b'e:duration', b'0') or b'0')
        rec[1] = rec[1] or cells.get(b's:conversion', b'').decode() == 'converted'
    hbase.close()
    engagement = {}
    for (uid, _), (seconds, converted) in by_session.items():
        rec = engagement.setdefault(uid, [0, 0, 0])
        rec[0] += 1; rec[1] += seconds; rec[2] += int(converted)
    e_rows=[(uid,v[0],float(v[1])/v[0] if v[0] else 0.0,v[2]) for uid,v in engagement.items()]
    # Spark 3.5's bundled cloudpickle cannot serialize local Python collections on
    # Python 3.14; stage direct store extracts as JSON and let Spark read them natively.
    extract_dir=os.path.join(args.out,'store_source_extracts'); os.makedirs(extract_dir,exist_ok=True)
    def stage(name, rows, fields):
        path=os.path.join(extract_dir,name+'.json')
        with open(path,'w',encoding='utf-8') as f:
            for values in rows:
                f.write(json.dumps(dict(zip(fields,values)))+'\n')
        return spark.read.json(path)
    p_df=stage('mongo_profiles',profile_rows,['user_id','registration_date','country'])
    o_df=stage('mongo_purchases',purchase_rows,['user_id','orders','revenue','last_purchase'])
    e_df=stage('hbase_engagement',e_rows,['user_id','sessions','avg_session_seconds','converted_sessions'])
    result=p_df.join(e_df,'user_id','left').join(o_df,'user_id','left').fillna({'sessions':0,'avg_session_seconds':0,'converted_sessions':0,'orders':0,'revenue':0})
    result=result.withColumn('revenue_per_session',F.when(F.col('sessions')>0,F.col('revenue')/F.col('sessions')).otherwise(0))
    out=os.path.join(args.out,'integrated_store_user_metrics'); result.write.mode('overwrite').parquet(out)
    result.createOrReplaceTempView('store_users')
    segments=spark.sql("SELECT CASE WHEN orders>=5 THEN 'frequent' WHEN orders>=2 THEN 'repeat' WHEN orders=1 THEN 'one-time' ELSE 'no purchase' END purchase_frequency, COUNT(*) users, ROUND(AVG(revenue),2) avg_revenue, ROUND(AVG(sessions),2) avg_sessions FROM store_users GROUP BY 1 ORDER BY avg_revenue DESC")
    segments.write.mode('overwrite').option('header',True).csv(os.path.join(args.out,'store_user_segments'))
    manifest={'source':'MongoDB + HBase via native clients','mongo_profiles':len(profile_rows),'mongo_purchasing_users':len(purchase_rows),'hbase_user_sessions':len(by_session),'integrated_users':result.count()}
    with open(os.path.join(args.out,'store_integration_manifest.json'),'w',encoding='utf-8') as f: json.dump(manifest,f,indent=2)
    print(json.dumps(manifest,indent=2))

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--data',default='data'); ap.add_argument('--out',default='output'); ap.add_argument('--source-mode',choices=['files','stores'],default='files'); ap.add_argument('--mongo-uri',default=os.getenv('MONGODB_URI','mongodb://localhost:27017')); ap.add_argument('--hbase-host',default='localhost'); ap.add_argument('--hbase-port',type=int,default=9090); a=ap.parse_args()
    os.makedirs(a.out,exist_ok=True)
    spark=SparkSession.builder.appName('AUCA-Ecommerce-Analytics').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions', max(8, spark.sparkContext.defaultParallelism * 2))
    if a.source_mode == 'stores':
        run_store_integration(spark,a); spark.stop(); return
    d=a.data
    users=json_df(spark,os.path.join(d,'users.json')).dropDuplicates(['user_id'])
    products=json_df(spark,os.path.join(d,'products.json')).dropDuplicates(['product_id'])
    tx=json_df(spark,os.path.join(d,'transactions*.json')).dropDuplicates(['transaction_id'])
    sessions=json_df(spark,os.path.join(d,'sessions_*.json')).dropDuplicates(['session_id'])
    # Normalize IDs, dates, null collections and monetary values once at ingestion.
    tx=tx.withColumn('event_ts',F.to_timestamp('timestamp')).filter(F.col('transaction_id').isNotNull())
    sessions=sessions.withColumn('event_ts',F.to_timestamp('start_time'))
    tx.createOrReplaceTempView('transactions'); users.createOrReplaceTempView('users'); products.createOrReplaceTempView('products'); sessions.createOrReplaceTempView('sessions')

    # Item table and purchase cohorts. Filter to fulfilled revenue transactions.
    fulfilled=tx.filter(F.col('status').isin('completed','shipped','delivered'))
    items=fulfilled.select('transaction_id','user_id','event_ts','total','discount',F.explode('items').alias('item')).select('transaction_id','user_id','event_ts','total','item.*')
    items.cache()
    cohort=users.select('user_id',F.date_trunc('month',F.to_timestamp('registration_date')).alias('cohort_month')).join(items.groupBy('user_id',F.date_trunc('month','event_ts').alias('purchase_month')).agg(F.sum('subtotal').alias('spend'),F.countDistinct('transaction_id').alias('orders')),'user_id')
    cohort=cohort.withColumn('months_since_registration',F.floor(F.months_between('purchase_month','cohort_month'))).filter(F.col('months_since_registration')>=0)
    cohort.write.mode('overwrite').parquet(os.path.join(a.out,'cohort_spend'))

    # Pairwise co-purchase, deduplicated per order before self-join.
    distinct=items.select('transaction_id','product_id').dropDuplicates()
    pairs=distinct.alias('x').join(distinct.alias('y'),(F.col('x.transaction_id')==F.col('y.transaction_id'))&(F.col('x.product_id')<F.col('y.product_id'))).groupBy(F.col('x.product_id').alias('product_x'),F.col('y.product_id').alias('product_y')).agg(F.count('*').alias('orders_together')).filter(F.col('orders_together')>=2).orderBy(F.desc('orders_together'))
    pairs.write.mode('overwrite').parquet(os.path.join(a.out,'co_purchase'))

    # Spark SQL example: category revenue and transaction count from nested line items.
    items.createOrReplaceTempView('items')
    category_sql=spark.sql("""SELECT p.category_id, ROUND(SUM(i.subtotal),2) revenue, SUM(i.quantity) units, COUNT(DISTINCT i.transaction_id) orders FROM items i JOIN products p ON i.product_id=p.product_id GROUP BY p.category_id ORDER BY revenue DESC""")
    category_sql.write.mode('overwrite').option('header',True).csv(os.path.join(a.out,'category_revenue'))

    # Integrated funnel/CLV: user profiles + session engagement + fulfilled transaction history.
    engagement=sessions.groupBy('user_id').agg(F.countDistinct('session_id').alias('sessions'),F.avg('duration_seconds').alias('avg_session_seconds'),F.sum(F.when(F.col('conversion_status')=='converted',1).otherwise(0)).alias('converted_sessions'))
    purchases=fulfilled.groupBy('user_id').agg(F.countDistinct('transaction_id').alias('orders'),F.sum('total').alias('revenue'),F.max('event_ts').alias('last_purchase'))
    integrated=users.select('user_id','registration_date',F.col('geo_data.country').alias('country')).join(engagement,'user_id','left').join(purchases,'user_id','left').fillna({'sessions':0,'converted_sessions':0,'orders':0,'revenue':0})
    integrated=integrated.withColumn('revenue_per_session',F.when(F.col('sessions')>0,F.col('revenue')/F.col('sessions')).otherwise(0)).withColumn('purchase_frequency',F.when(F.col('orders')>=5,'frequent').when(F.col('orders')>=2,'repeat').when(F.col('orders')==1,'one-time').otherwise('no purchase'))
    integrated.write.mode('overwrite').parquet(os.path.join(a.out,'integrated_user_metrics'))
    integrated.createOrReplaceTempView('integrated_users')
    spark.sql("SELECT purchase_frequency, COUNT(*) users, ROUND(AVG(revenue),2) avg_revenue, ROUND(AVG(sessions),2) avg_sessions FROM integrated_users GROUP BY purchase_frequency ORDER BY avg_revenue DESC").write.mode('overwrite').option('header',True).csv(os.path.join(a.out,'user_segments'))

    # Static plots from distributed aggregates. This small final collection is bounded.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    cat=category_sql.limit(15).toPandas()
    if len(cat):
        plt.figure(figsize=(10,5)); plt.bar(cat.category_id,cat.revenue,color='#3973ac'); plt.xticks(rotation=45,ha='right'); plt.ylabel('Item revenue'); plt.title('Fulfilled revenue by category'); plt.tight_layout(); plt.savefig(os.path.join(a.out,'revenue_by_category.png'),dpi=160); plt.close()
    seg=integrated.groupBy('purchase_frequency').agg(F.count('*').alias('users')).orderBy('purchase_frequency').toPandas()
    if len(seg):
        plt.figure(figsize=(7,4)); plt.bar(seg.purchase_frequency,seg.users,color='#39a88a'); plt.ylabel('Users'); plt.title('Customer purchase frequency'); plt.tight_layout(); plt.savefig(os.path.join(a.out,'customer_segments.png'),dpi=160); plt.close()
    top=items.groupBy('product_id').agg(F.sum('quantity').alias('units')).orderBy(F.desc('units')).limit(10).toPandas()
    if len(top):
        plt.figure(figsize=(9,4)); plt.bar(top.product_id,top.units,color='#e08e45'); plt.xticks(rotation=45,ha='right'); plt.ylabel('Units'); plt.title('Top selling products'); plt.tight_layout(); plt.savefig(os.path.join(a.out,'top_products.png'),dpi=160); plt.close()
    funnel=sessions.agg(F.count('*').alias('sessions'),F.sum(F.when(F.size('viewed_products')>0,1).otherwise(0)).alias('product_view'),F.sum(F.when(F.expr("exists(page_views, x -> x.page_type = 'cart')"),1).otherwise(0)).alias('cart'),F.sum(F.when(F.col('conversion_status')=='converted',1).otherwise(0)).alias('converted')).toPandas()
    if len(funnel):
        row=funnel.iloc[0]; stages=['Sessions','Product views','Cart','Converted']; keys=['sessions','product_view','cart','converted']; vals=[int(row[k] or 0) for k in keys]
        plt.figure(figsize=(7,4)); plt.bar(stages,vals,color=['#527da8','#638fb5','#81a978','#df9c52']); plt.ylabel('Sessions'); plt.title('Browsing and purchase funnel'); plt.tight_layout(); plt.savefig(os.path.join(a.out,'conversion_funnel.png'),dpi=160); plt.close()
    items.unpersist(); spark.stop()
if __name__=='__main__': main()
