import streamlit as st
import pandas as pd
import pyodbc
import numpy as np
import plotly.graph_objects as go

# ---------------------------------------------------------
# SQL CONNECTION
# ---------------------------------------------------------
@st.cache_data(ttl=600)
def load_sql_data():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=stock_project;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    return {
        "sw": pd.read_sql("SELECT * FROM dbo.simply_wallstreet_facts", conn),
        "sf": pd.read_sql("SELECT * FROM dbo.snowflake_scores", conn),
        "stock": pd.read_sql(
            "SELECT * FROM dbo.stock_data",
            conn,
            parse_dates=["trade_date"],
        ),
        "news": pd.read_sql("SELECT * FROM dbo.google_news", conn),
        "fg": pd.read_sql("SELECT * FROM dbo.fear_and_greed_index", conn)
    }

sql = load_sql_data()

# ---------------------------------------------------------
# DATA PREP
# ---------------------------------------------------------
sw = sql["sw"].copy()
sf = sql["sf"].copy()
stock = sql["stock"].copy()
news_df = sql["news"].copy()
fg = sql["fg"].copy()

# rename dividend column
sw = sw.rename(columns={"current_dividend": "dividend_current"})

# filter to most recent per source_file or ticker
sw = sw.loc[sw.groupby("source_file")["date"].transform("max") == sw["date"]]
sf = sf.loc[sf.groupby("tickers")["date"].transform("max") == sf["date"]]

# uppercase tickers
sw["ticker"] = sw["source_file"].astype(str).str.upper()
sf["ticker"] = sf["tickers"].astype(str).str.upper()
stock["ticker"] = stock["tickers"].astype(str).str.upper()
dates = stock["trade_date"].sort_values().unique()
IND = "extended_data_industry_averages_name"

# =========================================================
# TOP KPI DISPLAYS (ADDED — SAFE / ADDITIVE)
# =========================================================

ticker_labels = {
    "^VIX": "VIX Index",
    "^GSPC": "SP-500",
    "GC=F": "Gold",
    "CL=F": "Crude Oil",
    "^HSI": "Hang Seng Index",
    "^N225": "Nikkei-225",
    "^STOXX": "STOXX Europe 600",
    "HG=F": "Copper",
    "SI=F": "Silver",
    "BTC=F": "Bitcoin",
    "ETH=F": "Ethereum",
    "^DJI": "Dow Jones",
    "ALI=F": "Aluminum",
    "ZW=F": "Wheat",
    "ZC=F": "Corn",
    "FEAR_GREED": "Fear & Greed Index",
}

# Commodity tickers (add $ prefix)
COMMODITY_TICKERS = {
    "GC=F",   # Gold
    "CL=F",   # Crude Oil
    "HG=F",   # Copper
    "SI=F",   # Silver
    "ALI=F",  # Aluminum
    "ZW=F",   # Wheat
    "ZC=F",   # Corn
    "BTC=F",  # Bitcoin
    "ETH=F",  # Ethereum
}

def latest_stock_value(ticker):
    df = stock[stock["ticker"] == ticker]
    if df.empty:
        return None
    return df.loc[df["trade_date"].idxmax(), "close_price"]

def latest_fear_greed():
    return fg.loc[fg["date"].idxmax(), "fear_and_greed"]

def resolve_kpi(selection):
    if selection == "FEAR_GREED":
        return latest_fear_greed()
    return latest_stock_value(selection)

k1, k2, k3 = st.columns([1, 1, 1], gap="large")

kpi_options = list(ticker_labels.keys())

def render_kpi(col, default, key):
    sel = col.selectbox(
        "",
        kpi_options,
        index=kpi_options.index(default),
        format_func=lambda x: ticker_labels[x],
        key=key,
        label_visibility="collapsed",
    )

    val = resolve_kpi(sel)

    if val is None:
        display = "—"
    else:
        if sel in COMMODITY_TICKERS:
            display = f"${val:,.2f}"
        else:
            display = f"{val:,.2f}"

    col.markdown(
        f"<div style='text-align:center;font-size:40px;font-weight:bold;margin-top:2px;'>{display}</div>",
        unsafe_allow_html=True,
    )

render_kpi(k1, "^GSPC", "kpi_left")
render_kpi(k2, "^VIX", "kpi_mid")
render_kpi(k3, "FEAR_GREED", "kpi_right")

# =========================================================
# SIDEBAR FILTERS
# =========================================================
with st.sidebar.expander("Select Industry", expanded=False):
    industries = sorted(sw[IND].dropna().unique())
    selected_industries = st.multiselect("", industries, default=industries)

with st.sidebar.expander("Select MarketCap", expanded=False):
    caps = sorted(sw["value_market_cap_band"].dropna().unique())
    selected_caps = st.multiselect("", caps, default=caps)

# =========================================================
# FILTERED DATA
# =========================================================
sw_filtered = sw[
    sw[IND].isin(selected_industries) &
    sw["value_market_cap_band"].isin(selected_caps)
].copy()

stocks_filtered = sw_filtered[["ticker"]].drop_duplicates()
stocks_filtered = stocks_filtered.merge(
    sf[["ticker", "my_pick", "total"]], on="ticker", how="left"
)

# =========================================================
# PRICE % CHANGE
# =========================================================
def pct_change_n(df, n):
    df = df.sort_values("trade_date")
    if len(df) < 2:
        return np.nan

    # percent change between first and last value
    return (df["close_price"].iloc[-1] - df["close_price"].iloc[0]) / df["close_price"].iloc[0] * 100


def price_change(n):
    return (
        stock[stock["trade_date"].isin(dates[-n:])]
        .groupby("ticker")
        .apply(lambda x: pct_change_n(x, n), include_groups=False)
        .rename(f"pct_change_{n}d")
        .reset_index()
    )

price_5d = price_change(5)
price_30d = price_change(30)
price_90d = price_change(90)

stocks_filtered = stocks_filtered.merge(price_5d, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(price_30d, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(price_90d, on="ticker", how="left")

# =========================================================
# VOLUME Z-SCORES
# =========================================================
vol_1y = (
    stock[stock["trade_date"].isin(dates[-252:])]
    .groupby("ticker")["volume"]
    .agg(["mean", "std"])
    .reset_index()
)

def vol_z(n):
    avg = (
        stock[stock["trade_date"].isin(dates[-n:])]
        .groupby("ticker")["volume"]
        .mean()
        .reset_index(name=f"avg_vol_{n}d")
    )
    df = avg.merge(vol_1y, on="ticker", how="left")
    df[f"vol_z_{n}d"] = (df[f"avg_vol_{n}d"] - df["mean"]) / df["std"]
    return df[["ticker", f"vol_z_{n}d"]]

vol5 = vol_z(5)
vol30 = vol_z(30)
vol90 = vol_z(90)

stocks_filtered = stocks_filtered.merge(vol5, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(vol30, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(vol90, on="ticker", how="left")

# =========================================================
# AVG VOLUME
# =========================================================
def avg_vol(n):
    return (
        stock[stock["trade_date"].isin(dates[-n:])]
        .groupby("ticker")["volume"]
        .mean()
        .reset_index(name=f"avg_vol_{n}d")
    )

avgvol5 = avg_vol(5)
avgvol30 = avg_vol(30)
avgvol90 = avg_vol(90)

stocks_filtered = stocks_filtered.merge(avgvol5, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(avgvol30, on="ticker", how="left")
stocks_filtered = stocks_filtered.merge(avgvol90, on="ticker", how="left")

# =========================================================
# INDUSTRY METRICS
# =========================================================
def industry_metric(col):
    return (
        sw_filtered[[IND, col]]
        .dropna()
        .groupby(IND)[col]
        .mean()
        .reset_index()
    )

industry_metrics = {
    "Industry Avg Share Price": industry_metric("extended_data_industry_averages_share_price"),
    "Industry Avg Market Cap": industry_metric("extended_data_industry_averages_market_cap"),
    "Industry Avg Intrinsic Discount": industry_metric("extended_data_industry_averages_intrinsic_discount"),
    "Industry Avg PE (High)": industry_metric("extended_data_industry_averages_pe"),
    "Industry Avg PE (Low)": industry_metric("extended_data_industry_averages_pe"),
    "Industry Fut 1Y Growth (High)": industry_metric("extended_data_industry_averages_future_one_year_growth"),
    "Industry Fut 1Y Growth (Low)": industry_metric("extended_data_industry_averages_future_one_year_growth"),
    "Industry Fut 3Y Growth (High)": industry_metric("extended_data_industry_averages_future_three_year_growth"),
    "Industry Fut 3Y Growth (Low)": industry_metric("extended_data_industry_averages_future_three_year_growth"),
    "Industry PEG (High)": industry_metric("extended_data_industry_averages_peg"),
    "Industry PEG (Low)": industry_metric("extended_data_industry_averages_peg"),
}

LOW_STATS = {
    "Industry Avg PE (Low)",
    "Industry Fut 1Y Growth (Low)",
    "Industry Fut 3Y Growth (Low)",
    "Industry PEG (Low)",
}

# =========================================================
# SW FACTS METRICS
# =========================================================
sw_stats = {
    "Future 1Y Revenue Growth": ("future_revenue_growth_1y", "top"),
    "Future 3Y Revenue Growth": ("future_revenue_growth_3y", "top"),
    "EV/EBITDA to Net Debt (Lowest)": ("health_net_debt_to_ebitda", "low"),
    "Intrinsic Value (Lowest)": ("value_intrinsic_value_de", "low"),
    "Intrinsic Value Levered Beta": ("value_intrinsic_value_levered_beta", "top"),
    "Future Growth 1Y": ("future_growth_1y", "top"),
    "Future Growth 3Y": ("future_growth_3y", "top"),
    "Forward EV/EBITDA 1Y (>0.1)": ("future_forward_ev_to_ebitda_1y", "low"),
    "Future Net Income Growth 1Y": ("future_net_income_growth_1y", "top"),
    "Future Net Income Growth 3Y": ("future_net_income_growth_3y", "top"),
    "Future ROE 1Y": ("future_roe_1y", "top"),
    "Future ROE 3Y": ("future_roe_3y", "top"),
    "Future EPS Growth 1Y": ("future_earnings_per_share_growth_1y", "top"),
    "Future EPS Growth 3Y": ("future_earnings_per_share_growth_3y", "top"),
    "Future Gross Margin 1Y": ("future_gross_profit_margin_1y", "top"),
    "Forward PE 1Y (>0.1)": ("future_forward_pe_1y", "low"),
    "Insider Buying": ("insider_buying", "top"),
    "Dividend Stocks": ("dividend_current", "top"),
    "Management Rate of Return": ("health_management_rate_return", "top"),
    "Cost of Equity (Lowest)": ("value_intrinsic_value_cost_of_equity", "low"),
}

# =========================================================
# STAT SELECTOR
# =========================================================
STAT_OPTIONS = [
    "My Pick",
    "Total Snowflake Score",
    "5D % Price Change",
    "30D % Price Change",
    "90D % Price Change",
    "5D Volume Z-Score",
    "30D Volume Z-Score",
    "90D Volume Z-Score",
    "Top Volume 5D",
    "Top Volume 30D",
    "Top Volume 90D",
    *industry_metrics.keys(),
    *sw_stats.keys(),
]

selected_stat = st.sidebar.selectbox("", STAT_OPTIONS, index=1)

# =========================================================
# BUILD TOP-15 TABLE (ticker/industry first, value second)
# =========================================================
if selected_stat in sw_stats:
    col, mode = sw_stats[selected_stat]
    df = sw_filtered[[col, "source_file"]].rename(columns={"source_file": "Ticker"}).dropna()

    # Enforce ROE ≤ 100
    if selected_stat in ["Future ROE 1Y", "Future ROE 3Y"]:
        df = df[df[col] <= 100]

    if ">0.1" in selected_stat:
        df = df[df[col] > 0.1]
    df = df.sort_values(col, ascending=(mode == "low")).head(15)
    df = df[["Ticker", col]]
    df.columns = ["Ticker", selected_stat]

elif "Industry" in selected_stat:
    df = industry_metrics[selected_stat].copy()
    col = df.columns[1]
    df = df.sort_values(col, ascending=(selected_stat in LOW_STATS)).head(15)
    df = df[[IND, col]]
    df.columns = ["Industry", selected_stat]

else:
    col_map = {
        "My Pick": "my_pick",
        "Total Snowflake Score": "total",
        "5D % Price Change": "pct_change_5d",
        "30D % Price Change": "pct_change_30d",
        "90D % Price Change": "pct_change_90d",
        "5D Volume Z-Score": "vol_z_5d",
        "30D Volume Z-Score": "vol_z_30d",
        "90D Volume Z-Score": "vol_z_90d",
        "Top Volume 5D": "avg_vol_5d",
        "Top Volume 30D": "avg_vol_30d",
        "Top Volume 90D": "avg_vol_90d",
    }
    col = col_map[selected_stat]
    df = stocks_filtered[["ticker", col]].dropna()
    df = df[["ticker", col]]
    df.columns = ["Ticker", selected_stat]
    df = df.sort_values(selected_stat, ascending=False).head(15)

# =========================================================
# DISPLAY TABLE (formatted, left-aligned, centered title)
# =========================================================
def fmt(x):
    if isinstance(x, (int, float)):
        return f"{x:,.2f}"
    return x

styled = (
    df.style
      .format(fmt)
      .set_properties(**{"text-align": "left"})
      .set_table_styles([
          {"selector": "th", "props": [("text-align", "left")]},
          {"selector": "td", "props": [("text-align", "left")]}
      ])
)

st.sidebar.dataframe(
    styled,
    use_container_width=True,
    hide_index=True
)

# =========================================================
st.markdown("---")

# ---------------------------------------------------------
# --- News Section Selector + Sentiment + Articles ---
# ---------------------------------------------------------

news_sections_map = {
    "stock market": "Stock Market",
    "economy": "Economy",
    "inflation": "Inflation",
    "federal reserve": "Federal Reserve",
    "interest rates": "Interest Rates",
    "CPI": "CPI",
    "jobs report": "Jobs Report"
}

news_filtered = news_df.copy()
news_filtered["published_at"] = pd.to_datetime(news_filtered["published_at"], errors="coerce")
cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=30)

recent_news_30d_all = news_filtered[news_filtered["published_at"] >= cutoff_date]

# ---------------------------------------------------------
# HEADER ROW: TITLE (CENTER) + SELECTOR (RIGHT)
# ---------------------------------------------------------
left_spacer, title_col, selector_col = st.columns([2, 5, 2.2])

with selector_col:
    selected_section_key = st.selectbox(
        "",
        options=list(news_sections_map.keys()),
        format_func=lambda x: news_sections_map[x],
        key="news_section_selector",
        label_visibility="collapsed",
        placeholder="Select News"
    )

with title_col:
    st.markdown(
        f"""
        <div style="
            text-align:center;
            font-size:32px;
            font-weight:bold;
            line-height:2.2;
        ">
            {news_sections_map[selected_section_key]} News
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------
# FILTER NEWS FOR SELECTED SECTION
# ---------------------------------------------------------
recent_news_30d = recent_news_30d_all[
    recent_news_30d_all["query_text"] == selected_section_key
]

# ---------------------------------------------------------
# SENTIMENT SUMMARY
# ---------------------------------------------------------
if not recent_news_30d.empty:
    avg_pos = recent_news_30d["sentiment_positive"].mean()
    avg_neu = recent_news_30d["sentiment_neutral"].mean()
    avg_neg = recent_news_30d["sentiment_negative"].mean()
    count_articles = len(recent_news_30d)

    st.markdown(f"""
    <div style="text-align:center; margin-top:5px; margin-bottom:2px;">
        <div style="display:flex; justify-content:space-around; max-width:700px; margin:0 auto; font-size:30px; font-weight:bold;">
            <div style="color:green;">Positive: {avg_pos:.2f}</div>
            <div style="color:black;">Neutral: {avg_neu:.2f}</div>
            <div style="color:red;">Negative: {avg_neg:.2f}</div>
        </div>
        <div style="margin-top:5px; font-size:12px; color:grey;">
            Based on {count_articles} articles from the last 30 days
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div style="text-align:center; margin-top:15px; margin-bottom:10px; color:grey;">
        No sentiment data available for {news_sections_map[selected_section_key]} in the past 30 days.
    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------
# MOST RECENT 3 ARTICLES
# ---------------------------------------------------------
news_recent = (
    news_filtered[news_filtered["query_text"] == selected_section_key]
    .sort_values("published_at", ascending=False)
    .head(3)
)

for _, row in news_recent.iterrows():
    link = row["link_url"] if pd.notna(row["link_url"]) else "#"
    source = row["source_name"] if pd.notna(row["source_name"]) else "N/A"
    title = row["title_text"]
    published = row["published_at"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["published_at"]) else ""
    sentiment_label = row["sentiment_label"]

    color = "green" if sentiment_label.lower() == "positive" else \
            "red" if sentiment_label.lower() == "negative" else "black"

    st.markdown(f"""
    <div style="margin:0 auto 20px auto; max-width:750px;">
        <div style="display:flex; align-items:center; margin-bottom:15px; flex-wrap:wrap;">
            <div style="
                width:60px;
                height:60px;
                background-color:#eeeeee;
                color:black;
                font-weight:bold;
                display:flex;
                justify-content:center;
                align-items:center;
                margin-right:15px;
                border-radius:4px;
                text-align:center;
                font-size:12px;">
                {source}
            </div>
            <div style="flex:1; min-width:250px; max-width:650px;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:5px;">
                    <div style="font-size:12px; color:grey;">{published}</div>
                    <div style="font-size:18px; font-weight:bold; color:{color}; text-align:center;">{sentiment_label}</div>
                </div>
                <div style="font-size:16px; font-weight:bold;">
                    <a href="{link}" target="_blank" style="text-decoration:none; color:black;">{title}</a>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
# =========================================================
st.markdown("---")

st.markdown(
    "<h2 style='text-align: center; font-weight:bold;'>Time Comparison Chart</h2>",
    unsafe_allow_html=True
)

# --- TIME SERIES GRAPH (Multi-Selection) ---

selected_tickers = st.multiselect(
    "Select up to 3 Indices / Commodities:",
    options=list(ticker_labels.keys()),
    format_func=lambda x: ticker_labels[x],
    default=["^GSPC"]
)

if len(selected_tickers) > 3:
    st.error("Please select no more than 3 metrics.")
elif selected_tickers:
    # Determine overall min and max dates across all selected tickers
    all_dates = []
    dfs = {}
    for ticker in selected_tickers:
        if ticker == "FEAR_GREED":
            temp_df = fg[["date", "fear_and_greed"]].copy()
            temp_df.rename(columns={"date": "trade_date", "fear_and_greed": "value"}, inplace=True)
        else:
            temp_df = stock[stock["ticker"] == ticker][["trade_date", "close_price"]].copy()
            temp_df.rename(columns={"close_price": "value"}, inplace=True)
        temp_df["trade_date"] = pd.to_datetime(temp_df["trade_date"])
        temp_df.sort_values("trade_date", inplace=True)
        dfs[ticker] = temp_df
        all_dates.extend(temp_df["trade_date"].tolist())

    if all_dates:
        min_date = min(all_dates).date()
        max_date = max(all_dates).date()

        col_start, col_end = st.columns(2)
        with col_start:
            start_date = st.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
        with col_end:
            end_date = st.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)

        # Build figure
        fig = go.Figure()
        colors = ["#33ccff", "#ff9933", "#66cc66"]

        for i, ticker in enumerate(selected_tickers):
            df = dfs[ticker]
            filtered_df = df[(df["trade_date"] >= pd.Timestamp(start_date)) & 
                             (df["trade_date"] <= pd.Timestamp(end_date))]

            if not filtered_df.empty:
                fig.add_trace(go.Scatter(
                    x=filtered_df["trade_date"],
                    y=filtered_df["value"],
                    mode="lines+markers",
                    name=ticker_labels[ticker],
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=4),
                    hovertemplate="Date: %{x|%Y-%m-%d}<br>Value: %{y:,.2f}<extra></extra>"
                ))

        fig.update_layout(
            title="Selected Time Series",
            xaxis_title="Date",
            yaxis_title="Value",
            template="plotly_white",
            height=500,
            hovermode="x unified",
            margin=dict(t=40, b=40, l=40, r=40)
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data found for the selected tickers.")
else:
    st.info("Please select at least one ticker or FEAR & GREED Index.")