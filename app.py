import re
from datetime import date
from typing import Tuple

import altair as alt
import pandas as pd
import streamlit as st


# =========================
# CONFIG
# =========================
APP_TITLE = "三角洲行动 交易行价格分析工具"
TIME_COL = "commit_time"

# Your GitHub repo that stores daily snapshots under /data/YYYY-MM-DD.csv
GITHUB_REPO = "ol0w0lo/delta-force-price-tracker"
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/data"


# =========================
# DATA LOADING (from GitHub)
# =========================
@st.cache_data(ttl=60)
def load_github_data(last_n_days: int = 7) -> pd.DataFrame:
    """
    Load last N daily CSV files from GitHub:
      data/YYYY-MM-DD.csv
    Concats them into one DataFrame.
    """
    dfs = []
    today = pd.Timestamp.utcnow().date()

    for i in range(last_n_days):
        day = (today - pd.Timedelta(days=i)).isoformat()
        url = f"{RAW_BASE}/{day}.csv"
        try:
            df = pd.read_csv(url)
            dfs.append(df)
        except Exception:
            # file may not exist yet
            pass

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Normalize required columns
    if "name" in df.columns:
        df["name"] = df["name"].astype(str)

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df[TIME_COL] = pd.to_datetime(df.get(TIME_COL), errors="coerce", utc=True)

    # Drop invalid rows
    df = df.dropna(subset=["name", "price", TIME_COL])

    # Remove corrupted rows where "name" looks like a date/time
    # (protects UI if any bad data slips in)
    bad_name = df["name"].astype(str).str.match(r"^20\d{2}-\d{2}-\d{2}")
    df = df.loc[~bad_name].copy()

    return df


def safe_date_range(df: pd.DataFrame) -> Tuple[date, date]:
    t = df[TIME_COL].dropna()
    # exclude absurd early values
    t = t[t >= pd.Timestamp("2000-01-01", tz="UTC")]
    if t.empty:
        today = pd.Timestamp.utcnow().date()
        return today, today
    return t.min().date(), t.max().date()


def format_price(x) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


# =========================
# UI
# =========================
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

st.write(
    "本页面直接从 GitHub 仓库读取 `/data/YYYY-MM-DD.csv` 的价格快照（由 GitHub Actions 每 10 分钟更新）。\n\n"
    f"当前仓库：`{GITHUB_REPO}`"
)

# Sidebar controls
st.sidebar.header("数据来源")
last_n_days = st.sidebar.slider("加载最近 N 天", min_value=1, max_value=90, value=30, step=1)

if st.sidebar.button("刷新数据（清缓存）"):
    load_github_data.clear()
st.sidebar.caption("提示：GitHub Actions 每 10 分钟更新一次；你也可以手动点上面的刷新。")

# Load data
df = load_github_data(last_n_days=last_n_days)

if df.empty:
    st.warning(
        "没有读取到任何数据。\n\n"
        "请检查：\n"
        "1) 你的 GitHub Actions 是否已经成功运行并生成 `data/YYYY-MM-DD.csv`\n"
        "2) 仓库名是否正确\n"
        "3) 分支是否是 main\n"
    )
    st.stop()

# Sidebar filters
st.sidebar.header("筛选条件")

keyword = st.sidebar.text_input("物品名称包含：", value="", placeholder="例如：钢盔 / 5.56 / 狙击")

df_names = df
if keyword.strip():
    df_names = df[df["name"].str.contains(keyword.strip(), na=False)]

if df_names.empty:
    st.error("当前关键字下没有找到任何物品记录。请更换关键字。")
    st.stop()

all_names = sorted(df_names["name"].unique())
item_name = st.sidebar.selectbox("选择具体物品：", all_names)

# Date range (default = item range)
global_min, global_max = safe_date_range(df)
tmp_item = df[df["name"] == item_name].copy()
item_min, item_max = safe_date_range(tmp_item)

item_min = max(item_min, global_min)
item_max = min(item_max, global_max)
if item_min > item_max:
    item_min, item_max = global_min, global_max

date_range = st.sidebar.date_input(
    "时间范围：",
    value=(item_min, item_max),
    min_value=global_min,
    max_value=global_max,
)

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date, end_date = global_min, date_range

agg_mode = st.sidebar.selectbox("时间粒度：", ["原始数据", "按天平均"])

# Filter item data
item_df = df[df["name"] == item_name].copy()
item_df = item_df.sort_values(TIME_COL)
mask = (item_df[TIME_COL].dt.date >= start_date) & (item_df[TIME_COL].dt.date <= end_date)
item_df = item_df[mask]

if item_df.empty:
    st.warning("该时间范围内没有记录，请调整时间范围。")
    st.stop()

# Aggregate daily
if agg_mode == "按天平均":
    item_df = (
        item_df.assign(_day=item_df[TIME_COL].dt.date)
        .groupby("_day", as_index=False)
        .agg({"price": "mean"})
    )
    item_df[TIME_COL] = pd.to_datetime(item_df["_day"], utc=True, errors="coerce")
    item_df = item_df.drop(columns=["_day"]).sort_values(TIME_COL)

# Layout
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader(f"价格走势：{item_name}")

    chart = (
        alt.Chart(item_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{TIME_COL}:T", title="时间"),
            y=alt.Y("price:Q", title="价格"),
            tooltip=[
                alt.Tooltip(f"{TIME_COL}:T", title="时间"),
                alt.Tooltip("price:Q", title="价格"),
            ],
        )
        .interactive()
    )
    st.altair_chart(chart, width="stretch")

with col_right:
    st.subheader("统计信息")

    item_sorted = item_df.sort_values(TIME_COL)
    earliest_price = float(item_sorted.iloc[0]["price"])
    latest_price = float(item_sorted.iloc[-1]["price"])

    price_min = item_df["price"].min()
    price_max = item_df["price"].max()
    price_mean = item_df["price"].mean()
    price_std = item_df["price"].std()

    st.metric("当前最新价格", format_price(latest_price))
    st.metric("时间段内最低价", format_price(price_min))
    st.metric("时间段内最高价", format_price(price_max))
    st.metric("时间段内平均价", f"{price_mean:,.1f}")
    st.metric("价格波动（标准差）", f"{price_std:,.1f}" if pd.notna(price_std) else "N/A")

    if earliest_price != 0:
        pct = (latest_price / earliest_price - 1) * 100
        st.metric("时间段涨跌幅 (%)", f"{pct:,.1f}")
    else:
        st.metric("时间段涨跌幅 (%)", "N/A")

    st.caption("提示：用『按天平均』看长期趋势；用『原始数据』看当天波动。")

st.subheader("最近记录（原始数据）")
df_recent = df[df["name"] == item_name].sort_values(TIME_COL, ascending=False).head(50)
st.dataframe(df_recent.reset_index(drop=True))
