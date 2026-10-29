"""Interactive local dashboard for the AUCA analytics project."""

import json
import os
from datetime import date, timedelta
from pathlib import Path

import happybase
import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

from mongodb_analytics import DB

ROOT = Path(__file__).resolve().parent
STATUSES = ["completed", "shipped", "delivered"]
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_THRIFT_PORT", "9090"))


def read_spark_summary():
    candidates = (
        ROOT / "output_full_scale_check" / "full_scale_summary.json",
        ROOT / "output_full_scale_summary" / "full_scale_summary.json",
        ROOT / "results" / "full_scale_spark_summary.json",
    )
    for path in candidates:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return {}


@st.cache_data(ttl=60, show_spinner=False)
def get_filter_values(uri):
    """Fetch light-weight bounds and selector values from MongoDB."""
    with MongoClient(uri, serverSelectionTimeoutMS=2500) as client:
        client.admin.command("ping")
        db = client[DB]
        bounds = next(db.transactions.aggregate([
            {"$group": {"_id": None, "min": {"$min": "$timestamp"}, "max": {"$max": "$timestamp"}}}
        ]), {})
        categories = sorted(value for value in db.products.distinct("category_id") if value)
        users = sorted(value for value in db.users.distinct("user_id") if value)
        products = sorted(value for value in db.products.distinct("product_id") if value)
        return bounds.get("min"), bounds.get("max"), categories, users, products


@st.cache_data(ttl=10, show_spinner=False)
def get_commerce_data(uri, start_text, end_text, statuses, categories):
    """Live, filtered transaction aggregations returned as display-ready rows."""
    start_stamp = f"{start_text}T00:00:00"
    end_stamp = f"{end_text}T23:59:59.999"
    match = {
        "status": {"$in": list(statuses)},
        "timestamp": {"$gte": start_stamp, "$lte": end_stamp},
    }
    with MongoClient(uri, serverSelectionTimeoutMS=2500) as client:
        client.admin.command("ping")
        db = client[DB]
        base = [
            {"$match": match},
            {"$unwind": "$items"},
            {"$lookup": {
                "from": "products", "localField": "items.product_id",
                "foreignField": "product_id", "as": "product",
            }},
            {"$unwind": {"path": "$product", "preserveNullAndEmptyArrays": True}},
        ]
        if categories:
            base.append({"$match": {"product.category_id": {"$in": list(categories)}}})
        projection = {"$project": {
            "transaction_id": 1, "timestamp": 1, "payment_method": 1,
            "product_id": "$items.product_id", "product_name": "$product.name",
            "category_id": "$product.category_id", "units": "$items.quantity",
            "item_revenue": "$items.subtotal",
        }}
        rows = list(db.transactions.aggregate(base + [projection]))
        # Aggregations below work on the already-filtered item records, so category
        # selection changes totals and charts consistently.
        frame = pd.DataFrame(rows)
        if frame.empty:
            return {"items": [], "daily": [], "categories": [], "products": [], "payments": [],
                    "orders": 0, "units": 0, "revenue": 0.0}

        frame["item_revenue"] = pd.to_numeric(frame["item_revenue"], errors="coerce").fillna(0)
        frame["units"] = pd.to_numeric(frame["units"], errors="coerce").fillna(0)
        frame["day"] = frame["timestamp"].astype(str).str.slice(0, 10)
        summary = {
            "items": rows,
            "daily": frame.groupby("day", as_index=False).agg(revenue=("item_revenue", "sum"), units=("units", "sum")).to_dict("records"),
            "categories": frame.groupby("category_id", as_index=False, dropna=False).agg(revenue=("item_revenue", "sum"), units=("units", "sum")).sort_values("revenue", ascending=False).to_dict("records"),
            "products": frame.groupby(["product_id", "product_name"], as_index=False, dropna=False).agg(revenue=("item_revenue", "sum"), units=("units", "sum")).sort_values("revenue", ascending=False).head(15).to_dict("records"),
            "payments": frame.groupby("payment_method", as_index=False, dropna=False).agg(orders=("transaction_id", "nunique"), revenue=("item_revenue", "sum")).sort_values("revenue", ascending=False).to_dict("records"),
            "orders": int(frame["transaction_id"].nunique()),
            "units": int(frame["units"].sum()),
            "revenue": float(frame["item_revenue"].sum()),
        }
        return summary


@st.cache_data(ttl=10, show_spinner=False)
def get_customer_summary(uri, user_id):
    with MongoClient(uri, serverSelectionTimeoutMS=2500) as client:
        client.admin.command("ping")
        db = client[DB]
        user = db.users.find_one({"user_id": user_id}, {"_id": 0}) or {}
        sessions = db.sessions.count_documents({"user_id": user_id})
        orders = list(db.transactions.aggregate([
            {"$match": {"user_id": user_id, "status": {"$in": STATUSES}}},
            {"$group": {"_id": None, "orders": {"$sum": 1}, "revenue": {"$sum": "$total"}}},
        ]))
        return user, sessions, (orders[0] if orders else {"orders": 0, "revenue": 0})


def read_hbase_user(user_id, limit):
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=4000)
    try:
        table = conn.table("user_events")
        prefix = f"{user_id}|".encode("utf-8")
        rows = []
        for key, cells in table.scan(
            row_start=prefix,
            row_stop=prefix_successor(prefix),
            limit=limit,
        ):
            parts = key.decode("utf-8").split("|")
            rows.append({
                "time": cells.get(b"e:time", b"").decode("utf-8"),
                "page": cells.get(b"e:type", b"").decode("utf-8"),
                "product": cells.get(b"e:product_id", b"").decode("utf-8") or "—",
                "category": cells.get(b"e:category_id", b"").decode("utf-8") or "—",
                "session": parts[2] if len(parts) > 2 else "",
                "duration_sec": int(cells.get(b"e:duration", b"0") or b"0"),
                "conversion": cells.get(b"s:conversion", b"").decode("utf-8"),
            })
        return rows
    finally:
        conn.close()


def prefix_successor(prefix):
    key = bytearray(prefix)
    for index in range(len(key) - 1, -1, -1):
        if key[index] != 255:
            key[index] += 1
            return bytes(key[: index + 1])
    return None


def plot_style(fig, height=330):
    fig.update_layout(
        template="plotly_dark", height=height, margin=dict(l=12, r=12, t=38, b=12),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, Segoe UI, sans-serif", color="#dce5f2"),
        title_font=dict(size=15, color="#eff5ff"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


st.set_page_config(
    page_title="AUCA | Commerce Intelligence",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');
:root { --ink:#e8eef9; --muted:#91a0b8; --panel:#101b2b; --line:#24344a; --aqua:#51dbc1; --blue:#6c9cff; }
.stApp { background: radial-gradient(ellipse at 8% 0%, #182941 0%, #0a1220 44%, #080e18 100%); color:var(--ink) !important; }
.stApp, [data-testid="stAppViewContainer"], [data-testid="stSidebar"] { color:#e8eef9 !important; }
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p, [data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
[data-testid="stAppViewContainer"] [data-testid="stWidgetLabel"] p { color:#dce6f5 !important; }
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p { color:#9aabc2 !important; }
[data-testid="stSidebar"] [data-baseweb="select"] > div { background:#101d2d !important; border-color:#344b65 !important; }
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="select"] input { color:#f1f6ff !important; }
[data-testid="stSidebar"] [data-baseweb="tag"] { background:#125e58 !important; border:1px solid #2caa96 !important; }
[data-testid="stSidebar"] [data-baseweb="tag"] span { color:#edfffb !important; }
[data-testid="stSidebar"] [data-baseweb="tag"] svg { fill:#cffff5 !important; }
[data-testid="stSidebar"] [data-testid="stSlider"] div { color:#e8eef9; }
[data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] p,
[data-testid="stAppViewContainer"] [data-testid="stWidgetLabel"] p { color:#cbd7e8; }
[data-testid="stAlert"] p { color:#e8eef9 !important; }
html, body, [class*="css"] { font-family:'DM Sans',sans-serif; font-size:17px; }
h1,h2,h3 { font-family:'Manrope',sans-serif; letter-spacing:-.035em; }
h1 { font-size:2.45rem !important; font-weight:800 !important; }
h2 { font-size:1.8rem !important; }
h3 { font-size:1.4rem !important; }
[data-testid="stSidebar"] { font-size:1.05rem; }
[data-testid="stSidebar"] label, [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p { font-size:1rem !important; }
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p { font-size:.9rem !important; }
[data-testid="stSidebar"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-baseweb="select"] input,
[data-testid="stSidebar"] [data-baseweb="tag"] span { font-size:.95rem !important; }
[data-testid="stSidebar"] button { font-size:1rem !important; }
[data-testid="stTabs"] button { font-size:1.05rem !important; padding-top:.7rem !important; padding-bottom:.7rem !important; }
[data-testid="stSidebar"] { background:#0b1421; border-right:1px solid #1e2c40; }
[data-testid="stMetric"] { background:linear-gradient(145deg,rgba(24,39,59,.92),rgba(15,26,41,.84)); border:1px solid #263950; padding:17px 19px; border-radius:15px; box-shadow:0 12px 32px rgba(0,0,0,.14); }
[data-testid="stMetricLabel"] { color:#9baac0 !important; font-size:.95rem !important; }
[data-testid="stMetricValue"] { color:#f4f7ff !important; font-family:'Manrope',sans-serif; font-size:1.8rem !important; }
[data-testid="stTabs"] button { border-radius:9px 9px 0 0; }
[data-testid="stDataFrame"] { border:1px solid #263950; border-radius:12px; overflow:hidden; }
.hero { padding:22px 26px; border-radius:20px; margin:2px 0 20px; background:linear-gradient(110deg,rgba(28,50,74,.96),rgba(16,30,48,.78)); border:1px solid #2b405b; box-shadow:0 18px 45px rgba(0,0,0,.18); }
.eyebrow { color:#63e4cb; font-size:.86rem; font-weight:700; letter-spacing:.16em; text-transform:uppercase; }
.hero p { color:#a9b7cb; margin:.45rem 0 0; }
.status-dot { color:#54dfad; font-weight:700; }
div[data-testid="stVerticalBlockBorderWrapper"] { border-color:#263950 !important; background:rgba(16,27,43,.5); border-radius:16px; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("<div class='eyebrow'>AUCA · ANALYTICS LAB</div>", unsafe_allow_html=True)
    st.markdown("## Commerce intelligence")
    st.caption("Live local data · MongoDB + HBase")
    st.divider()
    auto_refresh = st.toggle("Auto refresh", value=True)
    refresh_seconds = st.select_slider("Refresh interval", options=[5, 10, 15, 30, 60], value=10, format_func=lambda x: f"{x} sec")
    if st.button("↻ Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    try:
        min_stamp, max_stamp, category_values, user_values, product_values = get_filter_values(MONGO_URI)
        min_date = date.fromisoformat(min_stamp[:10]) if min_stamp else date.today() - timedelta(days=30)
        max_date = date.fromisoformat(max_stamp[:10]) if max_stamp else date.today()
        chosen_range = st.date_input(
            "Transaction date range", value=(min_date, max_date), min_value=min_date, max_value=max_date,
        )
        status_values = st.multiselect("Order statuses", STATUSES, default=STATUSES)
        chosen_categories = st.multiselect("Categories", category_values, default=category_values)
        filters_ready = True
    except Exception as exc:
        st.error("MongoDB is offline or not loaded.")
        st.caption(str(exc))
        min_date = max_date = date.today()
        chosen_range = (min_date, max_date)
        status_values = STATUSES
        category_values = []
        user_values = []
        product_values = []
        chosen_categories = []
        filters_ready = False

    st.divider()
    st.caption(f"MongoDB · `{MONGO_URI}`")
    st.caption(f"HBase Thrift · `{HBASE_HOST}:{HBASE_PORT}`")

if len(chosen_range) == 2:
    start_date, end_date = chosen_range
else:
    start_date = end_date = chosen_range[0]

st.markdown("""
<div class="hero">
  <div class="eyebrow">E-COMMERCE · LIVE OPERATIONS</div>
  <h1>Commerce at a glance</h1>
  <p>Explore sales, customer activity, and HBase events with live filters.</p>
</div>
""", unsafe_allow_html=True)

run_every = f"{refresh_seconds}s" if auto_refresh else None


@st.fragment(run_every=run_every)
def dashboard_body():
    summary = read_spark_summary()
    spark_sessions = summary.get("session_metrics", {})
    spark_purchases = summary.get("purchase_metrics", {})

    try:
        commerce = get_commerce_data(
            MONGO_URI,
            start_date.isoformat(),
            end_date.isoformat(),
            tuple(status_values),
            tuple(chosen_categories),
        ) if filters_ready else {}
        mongo_online = filters_ready
    except Exception as exc:
        commerce = {}
        mongo_online = False
        st.error(f"MongoDB query failed: {exc}")

    overview, sales, customers, activity, explorer = st.tabs(
        ["Overview", "Sales explorer", "Customer 360", "HBase activity", "Data explorer"]
    )

    with overview:
        st.caption(f"Filtered MongoDB results · {start_date:%d %b %Y} — {end_date:%d %b %Y}")
        if mongo_online:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Orders", f"{commerce.get('orders', 0):,}")
            m2.metric("Items sold", f"{commerce.get('units', 0):,}")
            m3.metric("Item revenue", f"{commerce.get('revenue', 0):,.2f}")
            m4.metric("Spark sessions", f"{spark_sessions.get('sessions', 0):,}",
                      help="Full-scale batch summary from the last completed Spark run")

            left, right = st.columns([1.5, 1])
            with left:
                daily = pd.DataFrame(commerce.get("daily", []))
                if not daily.empty:
                    fig = px.area(daily.sort_values("day"), x="day", y="revenue", title="Revenue trend",
                                  color_discrete_sequence=["#51dbc1"])
                    fig.update_traces(line=dict(width=2.5), fillcolor="rgba(81,219,193,.14)")
                    st.plotly_chart(plot_style(fig), width="stretch", key="overview_trend")
                else:
                    st.info("No sales match the selected filters.")
            with right:
                categories = pd.DataFrame(commerce.get("categories", []))
                if not categories.empty:
                    fig = px.pie(categories.head(8), names="category_id", values="revenue",
                                 hole=.66, title="Revenue mix", color_discrete_sequence=px.colors.qualitative.Set2)
                    fig.update_traces(textposition="inside", textinfo="percent")
                    st.plotly_chart(plot_style(fig), width="stretch", key="overview_mix")

            p1, p2 = st.columns(2)
            with p1:
                st.markdown("#### Best selling products")
                st.dataframe(pd.DataFrame(commerce.get("products", [])).head(8), hide_index=True,
                             width="stretch", height=320)
            with p2:
                st.markdown("#### Order payment mix")
                payments = pd.DataFrame(commerce.get("payments", []))
                if not payments.empty:
                    fig = px.bar(payments, x="payment_method", y="orders", color="revenue",
                                 title="Orders by payment method", color_continuous_scale="Teal")
                    st.plotly_chart(plot_style(fig, 320), width="stretch", key="payment_mix")
        else:
            st.warning("Connect and load MongoDB to see live commerce analytics.")

        st.markdown("#### Full-scale Spark batch")
        if summary:
            s1, s2, s3 = st.columns(3)
            s1.metric("Users", f"{summary.get('spark_users', 0):,}")
            s2.metric("Converted sessions", f"{spark_sessions.get('converted_sessions', 0):,}")
            s3.metric("Fulfilled revenue", f"{spark_purchases.get('fulfilled_revenue', 0):,.2f}")
            st.caption("Spark figures refresh from the latest summary file; rerun Spark to recalculate them.")
        else:
            st.info("Run the full-scale Spark summary to populate its metrics.")

    with sales:
        st.markdown("### Sales explorer")
        st.caption("Change dates, statuses, and categories in the sidebar. Click chart legends to isolate series.")
        daily = pd.DataFrame(commerce.get("daily", []))
        categories = pd.DataFrame(commerce.get("categories", []))
        products = pd.DataFrame(commerce.get("products", []))
        payments = pd.DataFrame(commerce.get("payments", []))
        c1, c2 = st.columns(2)
        with c1:
            if not daily.empty:
                measure = st.radio("Trend metric", ["revenue", "units"], horizontal=True, key="trend_measure")
                fig = px.line(daily.sort_values("day"), x="day", y=measure, markers=True,
                              title=f"Daily {measure}", color_discrete_sequence=["#6c9cff"])
                st.plotly_chart(plot_style(fig, 390), width="stretch", key="sales_line")
            else:
                st.info("No data for the current filters.")
        with c2:
            if not categories.empty:
                fig = px.bar(categories.sort_values("revenue"), x="revenue", y="category_id", orientation="h",
                             title="Category performance", color="units", color_continuous_scale="Teal")
                st.plotly_chart(plot_style(fig, 390), width="stretch", key="category_bar")
        if not products.empty:
            st.markdown("#### Product ranking")
            st.dataframe(products, hide_index=True, width="stretch")
        if not payments.empty:
            st.markdown("#### Payment breakdown")
            st.dataframe(payments, hide_index=True, width="stretch")

    with customers:
        st.markdown("### Customer 360")
        if user_values:
            selected_user = st.selectbox("Select a customer", user_values, key="customer_picker")
            try:
                profile, session_count, order_summary = get_customer_summary(MONGO_URI, selected_user)
                c1, c2, c3 = st.columns(3)
                c1.metric("Sessions", f"{session_count:,}")
                c2.metric("Fulfilled orders", f"{order_summary.get('orders', 0):,}")
                c3.metric("Customer revenue", f"{order_summary.get('revenue', 0):,.2f}")
                with st.expander("Profile details", expanded=False):
                    st.json(profile)
                st.markdown("#### Recent HBase events")
                events = read_hbase_user(selected_user, 20)
                st.dataframe(pd.DataFrame(events), hide_index=True, width="stretch")
            except Exception as exc:
                st.error(f"Could not load this customer from MongoDB/HBase: {exc}")
        else:
            st.info("Connect MongoDB to load customer IDs.")

    with activity:
        st.markdown("### HBase event explorer")
        st.caption("This tab performs a live prefix scan against `user_events`.")
        h1, h2 = st.columns([1, 3])
        with h1:
            if user_values:
                event_user = st.selectbox("User", user_values, key="event_user")
            else:
                event_user = st.text_input("User ID", "user_000042", key="event_user_text")
            event_limit = st.slider("Rows to fetch", 5, 100, 20, 5, key="event_limit")
        with h2:
            if event_user:
                try:
                    events = pd.DataFrame(read_hbase_user(event_user, event_limit))
                    st.metric("Events returned", len(events))
                    if not events.empty:
                        event_counts = events.groupby("page", as_index=False).size().rename(columns={"size": "events"})
                        fig = px.bar(event_counts, x="page", y="events", color="page",
                                     title=f"Recent activity · {event_user}", color_discrete_sequence=px.colors.qualitative.Bold)
                        st.plotly_chart(plot_style(fig, 300), width="stretch", key="event_chart")
                        st.dataframe(events, hide_index=True, width="stretch")
                    else:
                        st.info("No events found for this user.")
                except Exception as exc:
                    st.error(f"HBase is unavailable: {exc}")

    with explorer:
        st.markdown("### MongoDB collection explorer")
        collection_name = st.selectbox("Collection", ["users", "products", "categories", "sessions", "transactions"], key="collection_picker")
        row_limit = st.slider("Maximum rows", 10, 200, 50, 10, key="collection_limit")
        search_user = st.text_input("Optional user ID filter", key="collection_user_filter")
        query = {"user_id": search_user} if search_user and collection_name in ("sessions", "transactions") else {}
        try:
            with MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500) as client:
                client.admin.command("ping")
                docs = list(client[DB][collection_name].find(query, {"_id": 0}).limit(row_limit))
            st.caption(f"Showing up to {row_limit} rows from `{DB}.{collection_name}`")
            st.dataframe(pd.DataFrame(docs), hide_index=True, width="stretch", height=520)
        except Exception as exc:
            st.error(f"MongoDB explorer error: {exc}")


dashboard_body()
