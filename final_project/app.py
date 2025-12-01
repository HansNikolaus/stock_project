import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pkg_resources
from datetime import datetime, timedelta
import os
from packaging import version
import pyodbc

# --- Custom CSS ---
st.markdown("""
    <style>
    .header-text { font-size: 40px; font-weight: bold; }
    .sector-text { font-size: 17px; color: grey; margin-left: 20px; }
    .info-label { font-size: 24px; font-weight: normal; color: black; }
    .info-value { font-size: 24px; font-weight: bold; color: black; }
    .change-text { font-size: 24px; font-weight: bold; margin-left: 15px; }
    .description-box {
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #444;
        color: #eeeeee;
        margin-bottom: 25px;
        font-size: 17px;
    }
    .inline-metrics {
        display: flex;
        align-items: center;
        gap: 20px;
    }
    .stExpanderContent > div {
        padding-top: 5px !important;
        padding-bottom: 5px !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- OpenAI version check ---
required_version = "1.0.0"
installed_version = pkg_resources.get_distribution("openai").version
if version.parse(installed_version) < version.parse(required_version):
    st.warning(f"⚠️ OpenAI version {installed_version} is outdated. "
               f"Please upgrade to ≥ {required_version}. Try: pip install --upgrade openai")

st.set_page_config(page_title="Stock Dashboard", page_icon="📈", layout="wide")

# ---------------------------------------------------------
# SQL CONNECTION + LOADING TABLES
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

    try:
        conn = pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        st.error(f"❌ Could not connect to SQL Server: {e}")
        return None

    tables = {
        "company_info": None,
        "fear_and_greed_index": None,
        "google_news": None,
        "insider_transactions": None,
        "ownership_breakdown": None,
        "simply_wallstreet_facts": None,
        "snowflake_scores": None,
        "stock_data": None,
        "tickers": None,
    }

    for table in tables:
        try:
            if table == "stock_data":
                df = pd.read_sql(
                    f"SELECT * FROM dbo.{table}",
                    conn,
                    parse_dates=["date"]
                )
            else:
                df = pd.read_sql(f"SELECT * FROM dbo.{table}", conn)

            tables[table] = df

        except Exception as e:
            st.warning(f"⚠️ Could not load table {table}: {e}")
            tables[table] = pd.DataFrame()

    return tables


# Load data
sql = load_sql_data()
if sql is None:
    st.stop()

# ---------------- sw_facts_df ----------------
# Keep all rows with the most recent "date" per "source_file"
df = sql["simply_wallstreet_facts"].copy()
if not df.empty and "source_file" in df and "date" in df:
    sw_facts_df = df.loc[
        df.groupby("source_file")["date"].transform("max") == df["date"]
    ]
else:
    sw_facts_df = df

# ---------------- ownership_breakdown ----------------
df = sql["ownership_breakdown"].copy()

ownership_df = pd.DataFrame()

if isinstance(df, pd.DataFrame) and not df.empty:
    if "ticker" in df.columns and "html_creation_date" in df.columns:
        ownership_df = (
            df.loc[df.groupby("ticker")["html_creation_date"]
                     .transform("max") == df["html_creation_date"]]
            .reset_index(drop=True)
        )
    else:
        ownership_df = df.reset_index(drop=True)

# ---------------- snowflake_df ----------------
df = sql["snowflake_scores"].copy()
if not df.empty and "tickers" in df and "date" in df:
    snowflake_df = df.loc[
        df.groupby("tickers")["date"].transform("max") == df["date"]
    ]
else:
    snowflake_df = df

# ---------------------------------------------------------
# OTHER TABLES (NO SPECIAL FILTERING)
# ---------------------------------------------------------

tickers_df    = sql["tickers"]
stock_df      = sql["stock_data"]
news_df       = sql["google_news"]
fear_greed_df = sql["fear_and_greed_index"]

# --- Match name with tickers column
ticker_to_name = dict(
    zip(tickers_df["tickers"].str.strip(), tickers_df["names"].str.strip())
)

# --- Sidebar: Ticker Selection ---
with st.sidebar:
    # Dropdown with ticker symbols and optional company names
    ticker_options = tickers_df.apply(
        lambda row: f"{row['tickers']} - {row['names']}", axis=1
    )
    selected_option = st.selectbox("Select a ticker:", sorted(ticker_options))

# Extract the ticker symbol safely
selected_ticker = selected_option.split(" - ")[0].strip()

# --- Match name with tickers column
ticker_to_name = dict(
    zip(tickers_df["tickers"].str.strip(), tickers_df["names"].str.strip())
)

selected_company_name = ticker_to_name.get(selected_ticker)

# Get the single row for the selected ticker
info_row = tickers_df.loc[tickers_df["tickers"].str.strip() == selected_ticker]

if not info_row.empty:
    info = info_row.iloc[0]  # just get the first (and only) row
else:
    info = {"descriptions": "No description available", 
            "country": "N/A", 
            "financial_instrument": ""}

instrument_type = info.get("financial_instrument", "").upper()

# Filter stock data for the selected ticker
price_data = stock_df[stock_df["tickers"] == selected_ticker].sort_values("trade_date")
latest = price_data.iloc[-1]

# --- Sidebar Statistics Section ---
with st.sidebar.expander("📊 Statistics", expanded=False):
    def styled_header(title, tooltip):
        return f"""
        <div style='background-color:#000000;padding:8px 12px;border-radius:6px;'>{{
            <span style='color:#ffffff;font-weight:bold;text-decoration:underline;' title='{tooltip}'>{title} 🛈</span>
        </div>
        """

    # Fetch Simply Wall St facts for this ticker
    sw_matches = sw_facts_df[sw_facts_df["source_file"] == selected_ticker]

    if sw_matches.empty:
        sw = pd.Series()   # no data for this ticker
    else:
        sw = sw_matches.iloc[0]  # ALWAYS returns a Series

        # --- Valuation Layer ---
        if instrument_type not in ["FUTURE", "INDEX"]:
            st.markdown(
                styled_header(
                    "Valuation Layer",
                    "These metrics assess how the stock is priced relative to company fundamentals like earnings, book value, and revenue."
                ),
                unsafe_allow_html=True
            )

            pe_value = pd.to_numeric(sw.get("pe", 0), errors="coerce")
            pe_value = 0 if pd.isna(pe_value) else pe_value

            st.markdown(f"PE Ratio: <strong>{pe_value:.2f}</strong> 🛈", unsafe_allow_html=True)
            st.markdown(f"PB Ratio: <strong>{sw.get('pb', 0):.2f}</strong> 🛈", unsafe_allow_html=True)
            st.markdown(f"PS Ratio: <strong>{sw.get('value_price_to_sales', 0):.2f}</strong> 🛈", unsafe_allow_html=True)

            # --- Profitability Layer ---
            st.markdown(
                styled_header(
                    "Profitability Anchor",
                    "Profitability metrics show how effectively the company turns revenue into profit and creates shareholder value."
                ),
                unsafe_allow_html=True
            )

            st.markdown(f"EPS: <strong>{sw.get('eps', 0):.2f}</strong> 🛈", unsafe_allow_html=True)
            st.markdown(f"Book Value/Share: <strong>{sw.get('health_book_value_per_share', 0):.2f}</strong> 🛈", unsafe_allow_html=True)

        # --- Market Pulse ---
        st.markdown(
            styled_header(
                "Market Pulse",
                "These indicators measure price momentum and trend behavior, providing clues about investor sentiment."
            ),
            unsafe_allow_html=True
        )

        st.markdown(f"Relative Strength Index (RSI 30): <strong>{latest.get('rsi_30', 0):.2f}</strong> 🛈", unsafe_allow_html=True)
        st.markdown(f"SMA 50 Day: <strong>{latest.get('sma_50', 0):.2f}</strong> 🛈", unsafe_allow_html=True)

        # --- Volatility Check ---
        st.markdown(
            styled_header(
                "Volatility Check",
                "Volatility gauges the magnitude of price changes—higher values signal more risk but also more opportunity."
            ),
            unsafe_allow_html=True
        )

        st.markdown(f"Std. Dev 20 Day: <strong>{latest.get('std_dev_20', 0):.2f}</strong> 🛈", unsafe_allow_html=True)

# --- Sidebar Company Description ---
if instrument_type not in ["FUTURE", "INDEX"]:
    with st.sidebar.expander("📘 Company Description", expanded=False):
        st.markdown(f"""
            <div title='Summary of what this company does and where it’s based.'>
                <strong>Headquarter Location:</strong> {info.get('country', 'N/A')}  
                <br><br>
                {info.get('descriptions', '')}
            </div>
        """, unsafe_allow_html=True)

# --- Snowflake Fallback ---
if selected_ticker in snowflake_df["tickers"].values:
    snow = snowflake_df[snowflake_df["tickers"] == selected_ticker].squeeze()
else:
    # Default zero-values for snowflake categories
    snow = pd.Series({
        "value": 0,
        "future": 0,
        "past": 0,
        "health": 0,
        "dividend": 0
    })

# --- Header ---
company_name = info.get("names", selected_ticker)

st.markdown(
    f"<div class='header-text'>{company_name} ({selected_ticker})</div>",
    unsafe_allow_html=True
)

# --- Info and Metrics Display ---
def colorize(value):
    try:
        val_float = float(value)
        color = "red" if val_float < 0 else "black"
        return f"<span class='info-value' style='color:{color}'>{val_float:,.0f}</span>"
    except:
        return f"<span class='info-value'>{value}</span>"

colL, colR = st.columns([3, 2])

# Ensure price_data is sorted and latest close is available
price_data = stock_df[stock_df["tickers"] == selected_ticker].sort_values("trade_date")
latest = price_data.iloc[-1]
recent_close = latest.get("close_price", 0)

past_week = price_data.iloc[-5]["close_price"] if len(price_data) >= 5 else recent_close
past_year = price_data.iloc[-252]["close_price"] if len(price_data) >= 252 else recent_close

change_7d = ((recent_close - past_week) / past_week * 100) if past_week else 0
change_1y = ((recent_close - past_year) / past_year * 100) if past_year else 0

color_7d = "green" if change_7d >= 0 else "red"
color_1y = "green" if change_1y >= 0 else "red"

# Pull Simply Wall St facts (valuation & fundamentals) for this ticker
sw_rows = sw_facts_df[sw_facts_df["source_file"] == selected_ticker]
sw = sw_rows.iloc[0] if not sw_rows.empty else pd.Series(dtype="float64")

currency_iso = str(sw.get("dividend_dividend_currency_iso", "")).strip().upper()

currency_symbol = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CAD": "C$",
    "AUD": "A$",
    "CHF": "€",
    "SEK": "€",
    "DKK": "€"
}.get(currency_iso, "$")

with colL:
    if instrument_type not in ["FUTURE", "INDEX"]:
        st.markdown(f"<div class='sector-text'>{info['sector']} – {info['industry']}</div>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # ---------------------------------------------------------
        # Market Cap / Net Income / Revenue Display (Dollar Symbol Before Value)
        # ---------------------------------------------------------

        # Helper function to render values with styles and negative red
        def styled_value(value, decimals=0, currency_symbol="$"):
            """
            Returns HTML-styled value using:
            - .info-value class for default styling
            - red text only if value is negative
            - optional currency symbol before value
            """
            try:
                num = float(value)
            except:
                return f"<span class='info-value'>{value}</span>"

            fmt = f"{{:,.{decimals}f}}" if decimals > 0 else "{:,.0f}"

            formatted = f"{currency_symbol}{fmt.format(abs(num))}" if num >= 0 else f"-{currency_symbol}{fmt.format(abs(num))}"

            if num < 0:
                return f"<span class='info-value' style='color:red;'>{formatted}</span>"

            return f"<span class='info-value'>{formatted}</span>"

        # Ensure scalar float values
        market_cap = float(sw.get("value_market_cap_usd", 0) or 0)
        net_income = float(sw.get("past_net_income_usd", 0) or 0) * 1_000_000
        revenue = float(sw.get("past_revenue_usd", 0) or 0) * 1_000_000

        # Display each metric
        st.markdown(
            f"<div><span class='info-label'>Market Cap:</span> {styled_value(market_cap)}</div>",
            unsafe_allow_html=True
        )

        st.markdown(
            f"<div><span class='info-label'>Net Income:</span> {styled_value(net_income)}</div>",
            unsafe_allow_html=True
        )

        st.markdown(
            f"<div><span class='info-label'>Revenue:</span> {styled_value(revenue)}</div>",
            unsafe_allow_html=True
        )

        st.markdown("<br>", unsafe_allow_html=True)

    # --- Recent Close & Price Changes ---
    st.markdown(f"""
        <div class='inline-metrics' style='margin-top: 5px'>
            <span class='info-value'>Recent Close: {currency_symbol}{recent_close:.2f}</span>
            <span class='change-text' style='color:{color_7d}'>(7D {change_7d:.1f}%)</span>
            <span class='change-text' style='color:{color_1y}'>(1Y {change_1y:.1f}%)</span>
        </div>
    """, unsafe_allow_html=True)

# --- Dividends ---
if instrument_type not in ["FUTURE", "INDEX"]:
    # Filter sw_facts_df to the selected ticker and most recent row
    sw_latest_rows = sw_facts_df[sw_facts_df["source_file"] == selected_ticker]

    if not sw_latest_rows.empty:
        sw = sw_latest_rows.iloc[0]  # most recent row for this ticker

        # Estimated Annual Dividend
        estimated_div = sw.get("dividend_current")
        estimated_div = float(estimated_div) if pd.notna(estimated_div) else 0.0

        # Ex-Dividend Date
        ex_div_date = sw.get("dividend_upcoming_dividend_date")
        ex_div_date_str = (
            pd.to_datetime(ex_div_date).strftime("%b %d, %Y")
            if pd.notna(ex_div_date)
            else "N/A"
        )

        # Dividend Amount
        div_amount = sw.get("dividend_upcoming_dividend_amount")
        div_amount = float(div_amount) if pd.notna(div_amount) else 0.0

    else:
        # Defaults if no data available
        estimated_div = 0.0
        ex_div_date_str = "N/A"
        div_amount = 0.0

    # Display the dividend info using sector-text
    st.markdown(
        f"<div class='sector-text'>Estimated Annual Dividend: {estimated_div:.2f}%</div>",
        unsafe_allow_html=True
    )
    st.markdown(
        f"<div class='sector-text'>Ex-Dividend Date: {ex_div_date_str}</div>",
        unsafe_allow_html=True
    )
    st.markdown(
        f"<div class='sector-text'>Dividend Amount: {currency_symbol}{div_amount:.2f}</div>",
        unsafe_allow_html=True
    )


with colR:
    # Ensure snowflake data is a single row
    if selected_ticker in snowflake_df["tickers"].values:
        snow_rows = snowflake_df[snowflake_df["tickers"] == selected_ticker]
        snow = snow_rows.iloc[0]
    else:
        snow = pd.Series({k: 0 for k in ["value", "future", "past", "health", "dividend"]})

    def build_snowflake_chart(data, label):
        axes = ["value", "future", "past", "health", "dividend"]
        labels = [a.title() for a in axes]
        values = [int(round(data.get(a, 0))) for a in axes]

        # Close the radar polygon
        r = values + [values[0]]
        theta = labels + [labels[0]]

        hover_descriptions = {
            "value": "Is the company undervalued compared to peers and cashflows?",
            "future": "Forecasted performance in 1–3 years?",
            "past": "Performance over the last 5 years?",
            "health": "Financial health and debt levels?",
            "dividend": "Dividend quality and reliability?"
        }

        hover_text = [
            f"<span style='font-size:13px'><b>{lbl}</b>: {val}/6<br>{hover_descriptions[key]}</span>"
            for key, lbl, val in zip(axes, labels, values)
        ]
        hover_text.append(hover_text[0])

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=r,
            theta=theta,
            fill='toself',
            line=dict(color="#00ccff", width=4),
            marker=dict(size=6, color="#00ccff"),
            hoverinfo="text",
            hovertext=hover_text
        ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0,6],
                    tickvals=[1,2,3,4,5,6],
                    ticktext=[
                        "<span style='color:white'>1</span>",
                        "<span style='color:white'>2</span>",
                        "<span style='color:white'>3</span>",
                        "<span style='color:white'>4</span>",
                        "<span style='color:white'>5</span>",
                        "<span style='color:white; font-weight:bold; text-shadow:-1px -1px 0 #000, 1px -1px 0 #000, -1px 1px 0 #000, 1px 1px 0 #000'>6</span>"
                    ],

                    tickfont=dict(size=12),
                ),
                angularaxis=dict(
                    tickvals=labels,
                    ticktext=labels,
                    tickfont=dict(size=14, color="black"),
                    direction="clockwise",
                    rotation=90
                )
            ),
            template="plotly_dark",
            showlegend=False,
            margin=dict(t=30, b=20, l=55, r=35),
            width=390,
            height=351
        )

        return fig

    fig = build_snowflake_chart(snow, selected_ticker)
    st.plotly_chart(fig, use_container_width=True)

# =========================================================
st.markdown("---")

# --- Analyst Price Target + Forward Commentary Section ---
if instrument_type not in ["FUTURE", "INDEX"]:

    sw_rows = sw_facts_df[sw_facts_df["source_file"] == selected_ticker]

    if not sw_rows.empty:
        sw_row = sw_rows.iloc[0]  # get first row as Series (scalar access)

        # Analyst price targets
        num_analysts = sw_row.get("value_price_target_analyst_count")
        target_low = sw_row.get("value_price_target_low")
        target_avg = sw_row.get("value_price_target")
        target_high = sw_row.get("value_price_target_high")
        recent_close = latest["close_price"]

        # -----------------------------
        # HEADER: "{X} Analysts' 1-Year Price Predictions"
        # -----------------------------
        if pd.notna(num_analysts):
            st.markdown(
                f"""
                <div style='text-align: center; font-size:28px; font-weight:bold; color:black; margin-top:20px;'>
                    {int(num_analysts)} Analysts' 1-Year Price Predictions
                </div>
                """,
                unsafe_allow_html=True,
            )

        # -----------------------------
        # PRICE TARGET CHART
        # -----------------------------
        if pd.notna(target_low) and pd.notna(target_avg) and pd.notna(target_high):

            price_points = {
                "Lowest Estimate": float(target_low),
                "Average Estimate": float(target_avg),
                "Highest Estimate": float(target_high),
                "Current Price": float(recent_close)
            }

            sorted_points = sorted(price_points.items(), key=lambda x: x[1])

            fig = go.Figure()

            # Line connecting points
            fig.add_trace(go.Scatter(
                x=[p[1] for p in sorted_points],
                y=[1] * len(sorted_points),
                mode="lines",
                line=dict(color="black", width=5),
                hoverinfo="skip",
                showlegend=False
            ))

            # Markers for each point
            for label, value in sorted_points:
                is_current = label == "Current Price"

                fig.add_trace(go.Scatter(
                    x=[value],
                    y=[1],
                    mode="markers+text",
                    marker=dict(
                        size=14 if is_current else 11,
                        color="#00ccff" if is_current else "black"
                    ),
                    text=[f"<b>{currency_symbol}{value:.2f}</b>" if is_current else f"{currency_symbol}{value:.2f}"],
                    textposition="top center" if is_current else "bottom center",
                    textfont=dict(size=22),
                    hovertext=label,
                    hoverinfo="text",
                    hoverlabel=dict(font=dict(size=20)),
                    showlegend=False
                ))

            fig.update_layout(
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                margin=dict(t=10, b=10, l=20, r=20),
                height=160
            )

            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No price target data available for this ticker.")

        # -----------------------------
        # Forward-looking commentary
        # -----------------------------
        commentary_columns = [
            "extended_data_statements_future_earnings_high_growth_description",
            "extended_data_statements_future_growth1y_statement_description",
            "extended_data_statements_future_net_income2y_statement_description",
            "extended_data_statements_future_projected_roebench_statement_description",
            "extended_data_statements_future_cash2y_statement_description",
            "extended_data_statements_future_revenue2y_statement_description",
            "extended_data_statements_future_revenue_high_growth_description"
        ]

        commentary_text = " ".join([str(sw_row.get(col, "")) for col in commentary_columns if pd.notna(sw_row.get(col))])
        if commentary_text:
            st.markdown(
                f"<div style='margin-top:15px; font-size:16px; color:black;'>{commentary_text}</div>",
                unsafe_allow_html=True
            )

    else:
        st.info("No analyst data available for this ticker.")
        
# =========================================================
st.markdown("---")

# --- Time Series Chart ---

# Mapping of internal metric keys to user-friendly labels
metric_label_map = {
    "open_price": "Open",
    "close_price": "Close",
    "high_price": "High",
    "low_price": "Low",
    "dividend": "Dividend",
    "split": "Split",
    "rsi_5": "RSI 5",
    "rsi_14": "RSI 14",
    "rsi_30": "RSI 30",
    "rsi_50": "RSI 50",
    "sma_10": "SMA 10",
    "sma_50": "SMA 50",
    "sma_200": "SMA 200",
    "std_dev_10": "Std Dev 10",
    "std_dev_20": "Std Dev 20",
    "std_dev_100": "Std Dev 100"
}

# Reverse mapping for internal lookup
label_to_metric_map = {v: k for k, v in metric_label_map.items()}

# --- Metric selection ---
selected_labels = st.multiselect(
    "Select up to 3 metrics:",
    options=list(metric_label_map.values()),
    default=[metric_label_map["close_price"]]
)

if len(selected_labels) > 3:
    st.error("Please select no more than 3 metrics.")
else:
    selected_metrics = [label_to_metric_map[label] for label in selected_labels]

    # Ensure trade_date is datetime
    price_data["trade_date"] = pd.to_datetime(price_data["trade_date"])

    # Date inputs with proper bounds
    min_date = price_data["trade_date"].min()
    max_date = price_data["trade_date"].max()
    col_start, col_end = st.columns(2)
    with col_start:
        start_date = st.date_input("Start Date", min_value=min_date.date(), max_value=max_date.date(), value=min_date.date())
    with col_end:
        end_date = st.date_input("End Date", min_value=min_date.date(), max_value=max_date.date(), value=max_date.date())

    # Convert selected dates to Timestamps for comparison
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    # Filter data safely
    filtered = price_data[(price_data["trade_date"] >= start_ts) & (price_data["trade_date"] <= end_ts)].copy()
    filtered.set_index("trade_date", inplace=True)

    if not filtered.empty:
        # Rename columns for display
        chart_df = filtered[selected_metrics].rename(columns=metric_label_map)
        st.line_chart(chart_df)

        # Volume chart if available
        if "volume" in filtered.columns:
            volume_fig = go.Figure(data=go.Bar(
                x=filtered.index,
                y=filtered["volume"],
                marker_color="#33ccff"
            ))
            volume_fig.update_layout(
                margin=dict(t=10, b=30),
                xaxis_title="Date",
                yaxis_title="Volume",
                template="plotly_dark",
                height=300
            )
            st.plotly_chart(volume_fig, use_container_width=True)
    else:
        st.info("No data found for the selected date range.")
# -----------------------------

# =========================================================
st.markdown("---")

st.markdown(
    "<h2 style='text-align: center; font-weight:bold;'>Sentiment Analysis</h2>",
    unsafe_allow_html=True
)

# --- Sentiment Summary (Last 30 Days) ---

if selected_company_name:
    news_filtered = news_df[news_df["query_text"] == selected_company_name].copy()
else:
    news_filtered = news_df.iloc[0:0].copy()

news_filtered["published_at"] = pd.to_datetime(
    news_filtered["published_at"], errors="coerce"
)

cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=30)
recent_news_30d = news_filtered[news_filtered["published_at"] >= cutoff_date]

# Compute averages if data exists
if not recent_news_30d.empty:
    avg_pos = recent_news_30d["sentiment_positive"].mean()
    avg_neu = recent_news_30d["sentiment_neutral"].mean()
    avg_neg = recent_news_30d["sentiment_negative"].mean()
    count_articles = len(recent_news_30d)

    st.markdown(f"""
    <div style="text-align:center; margin-top:10px; margin-bottom:2px;">
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
    st.markdown("""
    <div style="text-align:center; margin-top:20px; margin-bottom:10px; color:grey;">
        No sentiment data available for the past 30 days.
    </div>
    """, unsafe_allow_html=True)

# --- Most recent 3 articles ---
news_recent = (
    news_filtered
    .sort_values("published_at", ascending=False)
    .head(3)
)

for _, row in news_recent.iterrows():
    link = row["link_url"] if pd.notna(row["link_url"]) else "#"
    source = row["source_name"] if pd.notna(row["source_name"]) else "N/A"
    title = row["title_text"]
    published = (
        row["published_at"].strftime("%Y-%m-%d %H:%M")
        if pd.notna(row["published_at"]) else ""
    )
    sentiment_label = str(row["sentiment_label"])

    color = (
        "green" if sentiment_label.lower() == "positive"
        else "red" if sentiment_label.lower() == "negative"
        else "black"
    )

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

# ===========================Extended Statistics==============================

st.markdown("---")

st.markdown(
    "<h2 style='text-align: center; font-weight:bold;'>Extended Analysis</h2>",
    unsafe_allow_html=True
)
# --- MAIN EXPANDER: VALUE ---
with st.expander("Value", expanded=False):

    # Sub-expander: Comparisons
    with st.expander("Comparisons", expanded=False):

        # ------------------------------
        # Define colors once
        # ------------------------------
        ICE_BLUE = "#7FDBFF"
        GRAY = "#888888"
        BLACK = "#000000"

        # ------------------------------
        # Helper function to create bar charts
        # ------------------------------
        def create_bar_chart(company_val, industry_val, all_val, title,
                            company_label, industry_label, all_label):

            fig = go.Figure(
                data=[
                    go.Bar(name=company_label, x=["Company"], y=[company_val], marker_color=ICE_BLUE,
                        hoverinfo="skip", text=[f"<b>{company_val}</b>"], textposition="auto", textfont=dict(size=14)),
                    go.Bar(name=industry_label, x=["Industry"], y=[industry_val], marker_color=GRAY,
                        hoverinfo="skip", text=[f"<b>{industry_val}</b>"], textposition="auto", textfont=dict(size=14)),
                    go.Bar(name=all_label, x=["All Companies"], y=[all_val], marker_color=BLACK,
                        hoverinfo="skip", text=[f"<b>{all_val}</b>"], textposition="auto", textfont=dict(size=14)),
                ]
            )
            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=22, color="black")),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font_color="black",
                showlegend=False,
                height=350,
                margin=dict(l=20, r=20, t=60, b=20)
            )
            return fig

        # ------------------------------
        # Helper: Format large numbers as billions
        # ------------------------------
        def format_billions(x):
            try:
                return round(float(x) / 1_000_000_000, 3)
            except:
                return 0

        # ---------------------------------------------------------
        # ROW 0: VALUE SCORE (LEFT) + MARKET CAP (RIGHT)
        # ---------------------------------------------------------
        col_vs_left, col_mc_right = st.columns(2)

        value_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                            "extended_data_scores_value"].iloc[0], 3)
        value_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                            "extended_data_industry_averages_value_score"].iloc[0], 3)
        value_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                        "extended_data_industry_averages_all_value_score"].iloc[0], 3)

        with col_vs_left:
            st.plotly_chart(create_bar_chart(value_company, value_industry, value_all,
                                            "Value Score", "Company VS", "Industry VS", "All Company VS"),
                            use_container_width=True)
            st.markdown(f"""
                <div style="font-size:12px; margin-top:-10px; text-align:center;">
                    <span style="color:{ICE_BLUE}; font-weight:bold;">■</span> Company Value Score &nbsp;&nbsp;
                    <span style="color:{GRAY}; font-weight:bold;">■</span> Industry Value Score &nbsp;&nbsp;
                    <span style="color:{BLACK}; font-weight:bold;">■</span> All Company Value Score
                </div>
            """, unsafe_allow_html=True)

        mc_company = format_billions(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                    "value_market_cap"].iloc[0])
        mc_industry = format_billions(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                    "extended_data_industry_averages_market_cap"].iloc[0])
        mc_all = format_billions(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                "extended_data_industry_averages_all_market_cap"].iloc[0])

        with col_mc_right:
            st.plotly_chart(create_bar_chart(mc_company, mc_industry, mc_all,
                                            "Market Cap (Billions)", "Company MC", "Industry MC", "All Company MC"),
                            use_container_width=True)
            st.markdown(f"""
                <div style="font-size:12px; margin-top:-10px; text-align:center;">
                    <span style="color:{ICE_BLUE}; font-weight:bold;">■</span> Company Market Cap &nbsp;&nbsp;
                    <span style="color:{GRAY}; font-weight:bold;">■</span> Industry Market Cap &nbsp;&nbsp;
                    <span style="color:{BLACK}; font-weight:bold;">■</span> All Company Market Cap
                </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height:50px'></div>", unsafe_allow_html=True)

        # ---------------------------------------------------------
        # ROW 1: PE and PB
        # ---------------------------------------------------------
        col_left, col_right = st.columns(2)

        pe_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "value_pe"].iloc[0], 3)
        pe_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_pe"].iloc[0], 3)
        pe_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_pe"].iloc[0], 3)

        pb_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "value_pb"].iloc[0], 3)
        pb_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_pb"].iloc[0], 3)
        pb_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_pb"].iloc[0], 3)

        with col_left:
            st.plotly_chart(create_bar_chart(pe_company, pe_industry, pe_all,
                                            "Price to Earnings (PE)", "Company PE", "Industry Avg PE", "All Company Avg PE"),
                            use_container_width=True)

        with col_right:
            st.plotly_chart(create_bar_chart(pb_company, pb_industry, pb_all,
                                            "Price to Book (PB)", "Company PB", "Industry Avg PB", "All Company Avg PB"),
                            use_container_width=True)

        st.markdown("<div style='height:50px'></div>", unsafe_allow_html=True)

        # ---------------------------------------------------------
        # ROW 2: PEG and Intrinsic Discount
        # ---------------------------------------------------------
        col_left2, col_right2 = st.columns(2)

        peg_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "value_peg"].iloc[0], 3)
        peg_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_peg"].iloc[0], 3)
        peg_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_peg"].iloc[0], 3)

        discount_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "value_intrinsic_discount"].iloc[0], 3)
        discount_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_intrinsic_discount"].iloc[0], 3)
        discount_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_intrinsic_discount"].iloc[0], 3)

        with col_left2:
            st.plotly_chart(create_bar_chart(peg_company, peg_industry, peg_all,
                                            "Price/Earnings to Growth (PEG)", "Company PEG", "Industry Avg PEG", "All Company Avg PEG"),
                            use_container_width=True)

        with col_right2:
            st.plotly_chart(create_bar_chart(discount_company, discount_industry, discount_all,
                                            "Intrinsic Discount (%)", "Company Discount", "Industry Avg Discount", "All Company Avg Discount"),
                            use_container_width=True)

        st.markdown("<div style='height:50px'></div>", unsafe_allow_html=True)

        # ---------------------------------------------------------
        # ROW 3: ROE and ROA
        # ---------------------------------------------------------
        col_left3, col_right3 = st.columns(2)

        roe_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "roe"].iloc[0], 3)
        roe_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_roe"].iloc[0], 3)
        roe_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_roe"].iloc[0], 3)

        roa_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "roa"].iloc[0], 3)
        roa_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_roa"].iloc[0], 3)
        roa_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_roa"].iloc[0], 3)

        with col_left3:
            st.plotly_chart(create_bar_chart(roe_company, roe_industry, roe_all,
                                            "Return on Equity (ROE)", "Company ROE", "Industry Avg ROE", "All Company Avg ROE"),
                            use_container_width=True)

        with col_right3:
            st.plotly_chart(create_bar_chart(roa_company, roa_industry, roa_all,
                                            "Return on Assets (ROA)", "Company ROA", "Industry Avg ROA", "All Company Avg ROA"),
                            use_container_width=True)

        st.markdown("<div style='height:50px'></div>", unsafe_allow_html=True)

        # ---------------------------------------------------------
        # ROW 4: Levered Beta (Left) + Unlevered Beta (Right)
        # ---------------------------------------------------------
        col_left4, col_right4 = st.columns(2)

        levered_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                "value_intrinsic_value_levered_beta"].iloc[0], 3)
        levered_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                "extended_data_industry_averages_levered_beta"].iloc[0], 3)
        levered_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                            "extended_data_industry_averages_all_levered_beta"].iloc[0], 3)

        unlevered_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                "value_intrinsic_value_unlevered_beta"].iloc[0], 3)
        unlevered_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                                "extended_data_industry_averages_unlevered_beta"].iloc[0], 3)
        unlevered_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker,
                                            "extended_data_industry_averages_all_unlevered_beta"].iloc[0], 3)

        with col_left4:
            st.plotly_chart(create_bar_chart(levered_company, levered_industry, levered_all,
                                            "Levered Beta", "Company LB", "Industry LB", "All Company LB"),
                            use_container_width=True)

        with col_right4:
            st.plotly_chart(create_bar_chart(unlevered_company, unlevered_industry, unlevered_all,
                                            "Unlevered Beta", "Company ULB", "Industry ULB", "All Company ULB"),
                            use_container_width=True)

    # ---------------------------------------------------------
    # 📊 VALUE → VALUE STATISTICS EXPANDER
    # ---------------------------------------------------------
    with st.expander("Value Statistics", expanded=False):

        col_left_vs, col_right_vs = st.columns(2)

        # Helper: safely extract & round number
        def get_val(col):
            try:
                val = sw_facts_df.loc[
                    sw_facts_df["source_file"] == selected_ticker, col
                ].iloc[0]

                if isinstance(val, str):
                    return val  # text fields (e.g., Market Cap Band)

                return round(val, 3)
            except:
                return "N/A"

        # -----------------------------------------
        # COMBINED LIST OF ALL LABELS/METRICS
        # -----------------------------------------
        all_items = {
            "Capital to Revenue Ratio (3yr Avg)": "value_intrinsic_value_two_stage_fcf_capital_to_revenue_ratio_3yr_avg",
            "Cost of Equity": "value_intrinsic_value_cost_of_equity",
            "Intrinsic Value ADR/Share": "value_intrinsic_value_adr_per_share",
            "Market Cap Band": "value_market_cap_band",
            "NPV per Share": "value_npv_per_share",
            "PV 5Y": "value_intrinsic_value_pv_5y",
            "PV TV": "value_intrinsic_value_pvtv",
            "Risk Free Rate": "value_intrinsic_value_risk_free_rate",
            "Tax Rate": "value_intrinsic_value_tax_rate",
            "Two Stage FCF CAGR 5Y": "value_intrinsic_value_two_stage_fcf_growth_cagr_5y",
            "Two Stage FCF Shares Outstanding": "value_intrinsic_value_two_stage_fcf_shares_outstanding",
            "Equity Premium": "value_intrinsic_value_equity_premium",
            "EV to EBITDA": "value_ev_to_ebitda",
            "EV to Sales": "value_ev_to_sales",
            "Excess Return": "value_intrinsic_value_excess_returns_excess_return",
            "Excess Returns Book Value": "value_intrinsic_value_excess_returns_book_value",
            "Excess Returns Equity Cost": "value_intrinsic_value_excess_returns_equity_cost",
            "Excess Returns ROE Average": "value_intrinsic_value_excess_returns_return_on_equity_avg",
            "Excess Returns Stable Book Value": "value_intrinsic_value_excess_returns_stable_book_value",
            "Excess Returns Stable EPS": "value_intrinsic_value_excess_returns_stable_eps",
            "Terminal Value": "value_intrinsic_value_terminal_value",
        }

        # -----------------------------------------
        # ALPHABETIZE ALL METRICS TOGETHER
        # -----------------------------------------
        sorted_items = dict(sorted(all_items.items(), key=lambda x: x[0]))

        # -----------------------------------------
        # SPLIT INTO LEFT / RIGHT COLUMNS EVENLY
        # -----------------------------------------
        labels = list(sorted_items.keys())
        midpoint = len(labels) // 2

        left_keys = labels[:midpoint]
        right_keys = labels[midpoint:]

        # -----------------------------------------
        # RENDER LEFT COLUMN
        # -----------------------------------------
        with col_left_vs:
            for label in left_keys:
                col = sorted_items[label]
                st.markdown(
                    f"""
                    <div style='margin-bottom:12px;'>
                        <span style='font-size:20px; font-weight:bold;'>{label}:</span>
                        <span style='font-size:24px; font-weight:bold;'> {get_val(col)}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # -----------------------------------------
        # RENDER RIGHT COLUMN
        # -----------------------------------------
        with col_right_vs:
            for label in right_keys:
                col = sorted_items[label]
                st.markdown(
                    f"""
                    <div style='margin-bottom:12px;'>
                        <span style='font-size:20px; font-weight:bold;'>{label}:</span>
                        <span style='font-size:24px; font-weight:bold;'> {get_val(col)}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    # ------------------------------
    # 🔹 SECTION 5 — VALUE FORECASTS
    # ------------------------------
    with st.expander("Value Forecasts", expanded=False):

        # Define colors
        ICE_BLUE = "#7FDBFF"
        DARK_ICE_BLUE = "#3399CC"

        # Years and corresponding columns
        years = [2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035]
        forecast_cols = [
            "value_intrinsic_value_two_stage_fcf_first_stage_2026_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2027_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2028_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2029_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2030_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2031_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2032_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2033_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2034_data",
            "value_intrinsic_value_two_stage_fcf_first_stage_2035_data"
        ]
        discounted_cols = [
            "value_intrinsic_value_two_stage_fcf_first_stage_2026_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2027_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2028_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2029_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2030_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2031_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2032_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2033_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2034_discounted",
            "value_intrinsic_value_two_stage_fcf_first_stage_2035_discounted"
        ]

        # Extract values dynamically based on selected ticker and round to nearest tenth
        forecast_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1) 
            for col in forecast_cols
        ]
        discounted_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1) 
            for col in discounted_cols
        ]

        # Create bar chart
        fig_forecast = go.Figure()

        fig_forecast.add_trace(go.Bar(
            x=[str(year) for year in years],
            y=forecast_values,
            name="Free Cash Flow Forecast",
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in forecast_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))

        fig_forecast.add_trace(go.Bar(
            x=[str(year) for year in years],
            y=discounted_values,
            name="Discounted Value",
            marker_color=DARK_ICE_BLUE,
            text=[f"{v:,}" for v in discounted_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))

        # Update layout
        fig_forecast.update_layout(
            title=dict(text="Free Cash Flow Forecast vs. Discounted Value (in Millions)", 
                    x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value (in Millions)"),
            barmode='group',
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=500,
            margin=dict(l=40, r=40, t=80, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
        )

        st.plotly_chart(fig_forecast, use_container_width=True)
        
# ------------------------------
# 🔹 SECTION 6 — HEALTH
# ------------------------------

with st.expander("Health", expanded=False):

    # ------------------------------
    # Sub-expander: Health Comparisons
    # ------------------------------
    with st.expander("Health Comparisons", expanded=False):

        # ------------------------------
        # Define colors (same as Value Comparisons)
        # ------------------------------
        ICE_BLUE = "#7FDBFF"
        GRAY = "#888888"
        BLACK = "#000000"

        # ------------------------------
        # Extract values dynamically for selected ticker
        # ------------------------------
        company_val = sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_scores_health"].iloc[0]
        industry_val = sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_health_score"].iloc[0]
        all_val = sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_health_score"].iloc[0]

        # ------------------------------
        # Create bar chart
        # ------------------------------
        fig_health = go.Figure(
            data=[
                go.Bar(
                    name="Company",
                    x=["Company"],
                    y=[company_val],
                    marker_color=ICE_BLUE,
                    hoverinfo="skip",
                    text=[f"<b>{company_val}</b>"],
                    textposition="auto",
                    textfont=dict(size=14)
                ),
                go.Bar(
                    name="Industry Avg",
                    x=["Industry"],
                    y=[industry_val],
                    marker_color=GRAY,
                    hoverinfo="skip",
                    text=[f"<b>{industry_val}</b>"],
                    textposition="auto",
                    textfont=dict(size=14)
                ),
                go.Bar(
                    name="All Companies",
                    x=["All Companies"],
                    y=[all_val],
                    marker_color=BLACK,
                    hoverinfo="skip",
                    text=[f"<b>{all_val}</b>"],
                    textposition="auto",
                    textfont=dict(size=14)
                ),
            ]
        )

        fig_health.update_layout(
            title=dict(text="Health Score Comparison", x=0.5, xanchor="center", font=dict(size=22, color="black")),
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=False),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font_color="black",
            showlegend=False,
            height=350,
            margin=dict(l=20, r=20, t=60, b=20)
        )

        st.plotly_chart(fig_health, use_container_width=True)

        # ------------------------------
        # Add legend
        # ------------------------------
        st.markdown(f"""
            <div style="font-size:12px; margin-top:-10px; text-align:center;">
                <span style="color:{ICE_BLUE}; font-weight:bold;">■</span> Company &nbsp;&nbsp;
                <span style="color:{GRAY}; font-weight:bold;">■</span> Industry Average &nbsp;&nbsp;
                <span style="color:{BLACK}; font-weight:bold;">■</span> All Company Average
            </div>
        """, unsafe_allow_html=True)

    with st.expander("Health Statistics", expanded=False):

        # ------------------------------
        # Define columns
        # ------------------------------
        health_columns = [
            "health_accounts_payable",
            "health_accounts_receivable_percent",
            "health_accounts_receivable_growth_1y",
            "health_aggregate_accruals",
            "health_capex",
            "health_capex_growth_1y",
            "health_capex_growth_annual",
            "health_cash_from_investing",
            "health_cash_from_investing_1y",
            "health_cash_operating",
            "health_cash_operating_growth_1y",
            "health_current_assets",
            "health_current_assets_to_long_term_liab",
            "health_current_assets_to_total_debt",
            "health_current_portion_lease_liabilities",
            "health_current_solvency_ratio",
            "health_debt_to_equity_ratio",
            "health_debt_to_equity_ratio_past",
            "health_fixed_to_total_assets",
            "health_inventory",
            "health_inventory_growth_1y",
            "health_last_balance_sheet_update",
            "health_levered_free_cash_flow_break_even_years",
            "health_levered_free_cash_flow_growth_annual",
            "health_levered_free_cash_flow_growth_years",
            "health_levered_free_cash_flow_stable_years",
            "health_long_term_assets",
            "health_long_term_debt",
            "health_long_term_liab",
            "health_long_term_portion_lease_liabilities",
            "health_management_rate_return",
            "health_median_2yr_net_income",
            "health_net_debt",
            "health_net_debt_to_ebitda",
            "health_net_debt_to_equity",
            "health_net_income",
            "health_net_interest_cover",
            "health_net_interest_expense",
            "health_net_operating_assets",
            "health_net_operating_assets_1y",
            "health_operating_cash_flow_to_total_debt",
            "health_operating_expenses",
            "health_operating_expenses_growth_annual",
            "health_operating_expenses_growth_years",
            "health_operating_expenses_stable_years",
            "health_ppe",
            "health_receivables",
            "health_restricted_cash",
            "health_restricted_cash_ratio",
            "health_total_assets",
            "health_total_debt",
            "health_total_equity",
            "health_total_inventory",
            "health_total_liab_equity",
            "health_total_lease_liabilities",
            "health_total_debt_equity",
        ]

        # Include industry analysis metrics
        industry_columns = [
            "health_industry_analysis_net_int_margin",
            "health_industry_analysis_net_loans",
            "health_industry_analysis_net_loans_to_deposits",
            "health_industry_analysis_net_loans_to_total_assets",
            "health_industry_analysis_non_perf_loans_total_loans",
            "health_industry_analysis_loan_losses",
            "health_industry_analysis_allowance_loan_losses",
            "health_industry_analysis_allowance_non_perf_loans",
            "health_industry_analysis_total_bank_liabilities",
            "health_industry_analysis_total_deposits",
            "health_capitalisation_percent",
            "health_capitalisation_percent_1y",
            "health_book_value_per_share",
        ]

        # Combine all metrics
        all_metrics = health_columns + industry_columns

        # Sort alphabetically by display name (remove 'health_' and 'industry_analysis_' prefix for sorting)
        all_metrics_sorted = sorted(all_metrics, key=lambda x: x.replace("health_", "").replace("industry_analysis_", "").lower())

        # Split into left and right columns (approx equal)
        midpoint = len(all_metrics_sorted) // 2
        left_metrics = all_metrics_sorted[:midpoint]
        right_metrics = all_metrics_sorted[midpoint:]

        # ------------------------------
        # Create columns
        # ------------------------------
        col_left, col_right = st.columns(2)

        # ------------------------------
        # LEFT COLUMN
        # ------------------------------
        with col_left:
            for col in left_metrics:
                value = sw_facts_df.loc[
                    sw_facts_df["source_file"] == selected_ticker, col
                ].iloc[0]

                display_name = (
                    col.replace("health_", "")
                    .replace("industry_analysis_", "")
                    .replace("_", " ")
                    .title()
                )

                if pd.notna(value) and isinstance(value, (int, float)):
                    value_display = round(value, 3)
                else:
                    value_display = "N/A"

                st.markdown(
                    f"""
                    <div style='margin-bottom:12px;'>
                        <span style='font-size:20px;'>{display_name}:</span>
                        <span style='font-size:24px; font-weight:bold;'> {value_display}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # ------------------------------
        # RIGHT COLUMN
        # ------------------------------
        with col_right:
            for col in right_metrics:
                value = sw_facts_df.loc[
                    sw_facts_df["source_file"] == selected_ticker, col
                ].iloc[0]

                display_name = (
                    col.replace("health_", "")
                    .replace("industry_analysis_", "")
                    .replace("_", " ")
                    .title()
                )

                if pd.notna(value) and isinstance(value, (int, float)):
                    value_display = round(value, 3)
                else:
                    value_display = "N/A"

                st.markdown(
                    f"""
                    <div style='margin-bottom:12px;'>
                        <span style='font-size:20px;'>{display_name}:</span>
                        <span style='font-size:24px; font-weight:bold;'> {value_display}</span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    # ------------------------------
    # 🔹 SECTION 7 — HEALTH HISTORY
    # ------------------------------
    with st.expander("Health History", expanded=False):

        # Define colors
        ICE_BLUE = "#7FDBFF"

        # ------------------------------
        # Graph 1: Net Operating Assets History
        # ------------------------------
        noa_cols = [
            "health_net_operating_assets_ltm_history_0",
            "health_net_operating_assets_ltm_history_1",
            "health_net_operating_assets_ltm_history_2",
            "health_net_operating_assets_ltm_history_3"
        ]
        noa_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
            for col in noa_cols
        ]
        noa_years = [f"Year {i}" for i in range(len(noa_cols))]

        fig_noa = go.Figure()
        fig_noa.add_trace(go.Bar(
            x=noa_years,
            y=noa_values,
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in noa_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))
        fig_noa.update_layout(
            title=dict(text="Net Operating Assets History By Year", x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=400,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        st.plotly_chart(fig_noa, use_container_width=True)

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        # ------------------------------
        # Graph 2: Aggregate Accruals History
        # ------------------------------
        accrual_cols = [
            "health_aggregate_accruals_ltm_history_0",
            "health_aggregate_accruals_ltm_history_1",
            "health_aggregate_accruals_ltm_history_2",
            "health_aggregate_accruals_ltm_history_3"
        ]
        accrual_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
            for col in accrual_cols
        ]
        accrual_years = [f"Year {i}" for i in range(len(accrual_cols))]

        fig_accrual = go.Figure()
        fig_accrual.add_trace(go.Bar(
            x=accrual_years,
            y=accrual_values,
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in accrual_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))
        fig_accrual.update_layout(
            title=dict(text="Aggregate Accruals History By Year", x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=400,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        st.plotly_chart(fig_accrual, use_container_width=True)

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        # ------------------------------
        # Graph 3: Accrual Ratio From Cashflow History
        # ------------------------------
        accrual_ratio_cols = [
            "health_accrual_ratio_from_cashflow_ltm_history_0",
            "health_accrual_ratio_from_cashflow_ltm_history_1",
            "health_accrual_ratio_from_cashflow_ltm_history_2"
        ]
        accrual_ratio_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
            for col in accrual_ratio_cols
        ]
        accrual_ratio_years = [f"Year {i}" for i in range(len(accrual_ratio_cols))]

        fig_accrual_ratio = go.Figure()
        fig_accrual_ratio.add_trace(go.Bar(
            x=accrual_ratio_years,
            y=accrual_ratio_values,
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in accrual_ratio_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))
        fig_accrual_ratio.update_layout(
            title=dict(text="Accrual Ratio From Cashflow History By Year", x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=400,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        st.plotly_chart(fig_accrual_ratio, use_container_width=True)

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        # ------------------------------
        # Graph 4: Total Assets History
        # ------------------------------
        total_assets_cols = [
            "health_total_assets_ltm_history_0",
            "health_total_assets_ltm_history_1",
            "health_total_assets_ltm_history_2",
            "health_total_assets_ltm_history_3",
            "health_total_assets_ltm_history_4",
            "health_total_assets_ltm_history_5"
        ]
        total_assets_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
            for col in total_assets_cols
        ]
        total_assets_years = [f"Year {i}" for i in range(len(total_assets_cols))]

        fig_total_assets = go.Figure()
        fig_total_assets.add_trace(go.Bar(
            x=total_assets_years,
            y=total_assets_values,
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in total_assets_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))
        fig_total_assets.update_layout(
            title=dict(text="Total Assets History By Year", x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=400,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        st.plotly_chart(fig_total_assets, use_container_width=True)

        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

        # ------------------------------
        # Graph 5: Total Current Liabilities History
        # ------------------------------
        total_current_liab_cols = [
            "health_total_current_liab_ltm_history_0",
            "health_total_current_liab_ltm_history_1",
            "health_total_current_liab_ltm_history_2",
            "health_total_current_liab_ltm_history_3",
            "health_total_current_liab_ltm_history_4",
            "health_total_current_liab_ltm_history_5"
        ]
        total_current_liab_values = [
            round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
            for col in total_current_liab_cols
        ]
        total_current_liab_years = [f"Year {i}" for i in range(len(total_current_liab_cols))]

        fig_total_current_liab = go.Figure()
        fig_total_current_liab.add_trace(go.Bar(
            x=total_current_liab_years,
            y=total_current_liab_values,
            marker_color=ICE_BLUE,
            text=[f"{v:,}" for v in total_current_liab_values],
            textposition="auto",
            textfont=dict(size=16, color="black")
        ))
        fig_total_current_liab.update_layout(
            title=dict(text="Total Current Liabilities History By Year", x=0.5, xanchor="center", font=dict(size=22)),
            xaxis=dict(title="Year"),
            yaxis=dict(title="Value"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font_color='black',
            height=400,
            margin=dict(l=40, r=40, t=80, b=40)
        )
        st.plotly_chart(fig_total_current_liab, use_container_width=True)

# ------------------------------
# 🔹 SECTION 8 — FUTURE
# ------------------------------
with st.expander("Future", expanded=False):

    # ------------------------------
    # Sub-expander: Future Comparisons
    # ------------------------------
    with st.expander("Future Comparisons", expanded=False):

        # Define colors
        ICE_BLUE = "#7FDBFF"
        GRAY = "#888888"
        BLACK = "#000000"

        # ------------------------------
        # Helper function for bar charts
        # ------------------------------
        def create_future_bar_chart(company_val, industry_val, all_val, title, company_label, industry_label, all_label):
            fig = go.Figure(
                data=[
                    go.Bar(name=company_label, x=["Company"], y=[company_val], marker_color=ICE_BLUE,
                           hoverinfo="skip", text=[f"<b>{company_val}</b>"], textposition="auto", textfont=dict(size=14)),
                    go.Bar(name=industry_label, x=["Industry"], y=[industry_val], marker_color=GRAY,
                           hoverinfo="skip", text=[f"<b>{industry_val}</b>"], textposition="auto", textfont=dict(size=14)),
                    go.Bar(name=all_label, x=["All Companies"], y=[all_val], marker_color=BLACK,
                           hoverinfo="skip", text=[f"<b>{all_val}</b>"], textposition="auto", textfont=dict(size=14)),
                ]
            )
            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=22, color="black")),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font_color="black",
                showlegend=False,
                height=350,
                margin=dict(l=20, r=20, t=60, b=20)
            )
            return fig

        # ------------------------------
        # Graph 1: Future Score Comparison
        # ------------------------------
        col1, col2 = st.columns(2)

        future_score_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_scores_future"].iloc[0], 3)
        future_score_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_future_performance_score"].iloc[0], 3)
        future_score_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_future_performance_score"].iloc[0], 3)

        with col1:
            st.plotly_chart(create_future_bar_chart(future_score_company, future_score_industry, future_score_all,
                                                    "Future Score Comparison",
                                                    "Company Score", "Industry Avg Score", "All Company Avg Score"),
                            use_container_width=True)

        # ------------------------------
        # Graph 2: Future 1 Year Growth Comparison
        # ------------------------------
        growth_1y_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "future_growth_1y"].iloc[0], 3)
        growth_1y_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_future_one_year_growth"].iloc[0], 3)
        growth_1y_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_future_one_year_growth"].iloc[0], 3)

        with col2:
            st.plotly_chart(create_future_bar_chart(growth_1y_company, growth_1y_industry, growth_1y_all,
                                                    "Future 1 Year Growth Comparisons",
                                                    "Company 1Y Growth", "Industry Avg 1Y Growth", "All Company Avg 1Y Growth"),
                            use_container_width=True)

        st.markdown("<div style='height:50px;'></div>", unsafe_allow_html=True)

        # ------------------------------
        # Graph 3: Future 3 Year Growth Comparison (full width)
        # ------------------------------
        growth_3y_company = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "future_growth_3y"].iloc[0], 3)
        growth_3y_industry = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_future_three_year_growth"].iloc[0], 3)
        growth_3y_all = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_future_three_year_growth"].iloc[0], 3)

        st.plotly_chart(create_future_bar_chart(growth_3y_company, growth_3y_industry, growth_3y_all,
                                                "Future 3 Year Growth Comparisons",
                                                "Company 3Y Growth", "Industry Avg 3Y Growth", "All Company Avg 3Y Growth"),
                        use_container_width=True)
    # ------------------------------
    # 🔹 SECTION 9 — FUTURE STATISTICS
    # ------------------------------
    with st.expander("Future Statistics", expanded=False):

        # ------------------------------
        # Columns for left and right
        # ------------------------------
        future_stats_cols = [
            "future_roe_1y",
            "future_roe_3y",
            "future_return_on_equity_1y",
            "future_return_on_equity_3y",
            "future_earnings_per_share_growth_1y",
            "future_earnings_per_share_growth_3y",
            "future_minimum_earnings_growth",
            "future_earnings_per_share_growth_annual",
            "future_revenue_growth_annual",
            "future_cash_ops_growth_annual",
            "future_net_income_growth_annual",
            "future_ebitda_1y",
            "future_ebitda_growth_1y",
            "future_forward_pe_1y",
            "future_forward_price_to_sales_1y",
            "future_forward_ev_to_ebitda_1y",
            "future_forward_ev_to_sales_1y",
            "future_gross_profit_margin_1y"
        ]

        # ------------------------------
        # Extract values dynamically
        # ------------------------------
        stats_data = []
        for col in future_stats_cols:
            value = sw_facts_df.loc[
                sw_facts_df["source_file"] == selected_ticker, col
            ].iloc[0]

            rounded_value = round(value, 3) if pd.notna(value) else "N/A"

            # Create display name:
            # Remove "future" ONLY if followed by "forward"
            parts = col.split("_")
            if len(parts) > 1 and parts[1] == "forward":
                display_name = " ".join(parts[1:]).title()
            else:
                display_name = " ".join(parts).title()

            stats_data.append((display_name, rounded_value))

        # ------------------------------
        # Alphabetize
        # ------------------------------
        stats_data = sorted(stats_data, key=lambda x: x[0])

        # ------------------------------
        # Split into left and right columns
        # ------------------------------
        mid_index = len(stats_data) // 2
        left_stats = stats_data[:mid_index]
        right_stats = stats_data[mid_index:]

        # ------------------------------
        # Create columns in Streamlit
        # ------------------------------
        col_left, col_right = st.columns(2)

        # Formatting function
        def render_stat(name, val):
            st.markdown(
                f"""
                <div style='margin-bottom:12px;'>
                    <span style='font-size:20px;'>{name}:</span>
                    <span style='font-size:24px; font-weight:bold;'> {val}</span>
                </div>
                """,
                unsafe_allow_html=True
            )

        # ------------------------------
        # Left Column
        # ------------------------------
        with col_left:
            for name, val in left_stats:
                render_stat(name, val)

        # ------------------------------
        # Right Column
        # ------------------------------
        with col_right:
            for name, val in right_stats:
                render_stat(name, val)

    # ------------------------------
    # 🔹 SECTION 10 — FUTURE FORECASTS
    # ------------------------------
    with st.expander("Future Forecasts", expanded=False):

        ICE_BLUE = "#7FDBFF"

        # Helper function for bar graphs with proper spacing
        def create_forecast_bar_graph(columns, title):
            years_labels = ["1 Year", "2 Year", "3 Year"]  # x-axis labels
            fig = go.Figure()

            # Add each bar trace
            for i, col in enumerate(columns):
                value = round(sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0], 1)
                fig.add_trace(go.Bar(
                    x=[years_labels[i]],
                    y=[value],
                    name=years_labels[i],
                    marker_color=ICE_BLUE,
                    text=[f"{value:,}"],
                    textposition="auto",
                    textfont=dict(size=16, color="black"),
                    width=0.4  # reduces bar width for more spacing
                ))

            # Layout
            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=22)),
                xaxis=dict(title="Forecast Horizon", tickmode="array", tickvals=years_labels, ticktext=years_labels),
                yaxis=dict(title="Value"),
                barmode='group',
                bargap=0.6,  # adds spacing between groups
                plot_bgcolor='white',
                paper_bgcolor='white',
                font_color='black',
                height=400,
                margin=dict(l=40, r=40, t=60, b=40),
            )

            st.plotly_chart(fig, use_container_width=True)

        # Graphs with spacing and labels
        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_earnings_per_share_1y", "future_earnings_per_share_2y", "future_earnings_per_share_3y"],
            "Forecasted Earnings Per Share"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_revenue_growth_1y", "future_revenue_growth_2y", "future_revenue_growth_3y"],
            "Forecasted Revenue Growth"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_revenue_1y", "future_revenue_2y", "future_revenue_3y"],
            "Forecasted Revenue"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_cash_ops_growth_1y", "future_cash_ops_growth_2y", "future_cash_ops_growth_3y"],
            "Forecasted Cash Ops Growth"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_cash_ops_1y", "future_cash_ops_2y", "future_cash_ops_3y"],
            "Forecasted Cash Ops"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_net_income_growth_1y", "future_net_income_growth_2y", "future_net_income_growth_3y"],
            "Forecasted Net Income Growth"
        )

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
        create_forecast_bar_graph(
            ["future_net_income_1y", "future_net_income_2y", "future_net_income_3y"],
            "Forecasted Net Income"
        )

with st.expander("Past"):
    
    with st.expander("Past Comparisons"):

        # Use the same color palette as the rest of the app
        ICE_BLUE = "#7FDBFF"
        GRAY = "#888888"
        BLACK = "#000000"

        # Reusable bar chart function (consistent with Value Comparisons style)
        def comparison_bar_chart(title, categories, values, colors):
            fig = go.Figure()

            for cat, val, colr in zip(categories, values, colors):
                fig.add_trace(go.Bar(
                    x=[cat],
                    y=[val if pd.notna(val) else 0],
                    marker_color=colr,
                    width=0.45,
                    text=[f"{round(val, 3) if pd.notna(val) else 'N/A'}"],
                    textposition="auto",
                    textfont=dict(size=14, color="black")
                ))

            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=18, color="black")),
                margin=dict(l=10, r=10, t=50, b=30),
                xaxis=dict(title="", tickfont=dict(size=12)),
                yaxis=dict(title="Value", gridcolor="rgba(0,0,0,0.05)"),
                plot_bgcolor="white",
                paper_bgcolor="white",
                showlegend=False,
                height=320
            )

            st.plotly_chart(fig, use_container_width=True)

        # First row: two side-by-side charts
        col1, col2 = st.columns(2)

        # -------------------------
        # 1) Past Score Comparison
        # -------------------------
        with col1:
            categories = ["Company", "Industry Avg", "All Companies Avg"]
            values = [
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_scores_past"].iloc[0],
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_past_performance_score"].iloc[0],
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_past_performance_score"].iloc[0],
            ]
            colors = [ICE_BLUE, GRAY, BLACK]

            comparison_bar_chart("Past Score Comparison", categories, values, colors)

        # --------------------------------
        # 2) Past 1 Year Growth Comparison
        # --------------------------------
        with col2:
            categories = ["Company", "Industry Avg", "All Companies Avg"]
            values = [
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "past_growth_1y"].iloc[0],
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_past_one_year_growth"].iloc[0],
                sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_past_one_year_growth"].iloc[0],
            ]
            colors = [ICE_BLUE, GRAY, BLACK]

            comparison_bar_chart("Past 1 Year Growth Comparison", categories, values, colors)

        # Spacer between rows
        st.markdown("<div style='height:25px'></div>", unsafe_allow_html=True)

        # ---------------------------
        # Centered 3rd chart (full width)
        # ---------------------------
        categories = ["Company", "Industry Avg", "All Companies Avg"]
        values = [
            sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "past_growth_5y"].iloc[0],
            sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_past_five_year_growth"].iloc[0],
            sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_past_five_year_growth"].iloc[0],
        ]
        colors = [ICE_BLUE, GRAY, BLACK]

        comparison_bar_chart("Past 5 Year Growth Comparison", categories, values, colors)

        # ---------------------------------
        # 3) Past Statistics
        # ---------------------------------

    with st.expander("Past Statistics"):

        past_columns = [
            "past_revenue_usd",
            "past_operating_revenue",
            "past_net_income_usd",
            "past_earnings_per_share",
            "past_earnings_per_share_5y_avg",
            "past_ebt_excluding",
            "past_earnings_continued_ops",
            "past_total_assets",
            "past_total_equity",
            "past_current_liabilities",
            "past_return_on_equity",
            "past_return_on_assets",
            "past_return_on_capital_employed",
            "past_return_on_capital_employed_past",
            "past_return_on_capital_growth",
            "past_net_income_5y_avg",
            "past_last_earnings_update",
            "past_last_earnings_update_annual",
            "past_industry_analysis_d_a_expense",
            "past_industry_analysis_r_d_expense",
            "past_industry_analysis_non_op_expense",
            "past_industry_analysis_sales_marketing",
            "past_industry_analysis_revenue_segments_banking",
            "past_industry_analysis_stock_based_comp",
            "past_industry_analysis_general_administrative",
            "past_industry_analysis_selling_general_admin_expenses",
            "past_ebit",
            "past_ebit_3y_avg",
            "past_ebit_5y_avg",
            "past_earnings_per_share_single_growth_3y",
            "past_earnings_per_share_single_growth_5y",
            "past_gross_profit_margin_1y",
            "past_net_income_margin",
            "past_net_income_margin_1y",
            "past_change_in_unearned_revenue",
            "past_unearned_revenue_percent_of_sales",
            "past_ebt_including",
            "past_unusual_items",
            "past_unusual_item_ratio",
            "past_operating_revenue_percent",
            "past_years_profitable",
            "past_trading_since_years",
            "past_non_operating_revenue",
            "past_non_operating_revenue_ratio",
            "past_non_operating_revenue_ratio_delta",
            "past_income_tax_to_ebit_ratio",
            "past_business_revenue_segments_banking",
            "past_selling_general_admin_expense",
            "past_research_development_expense",
            "past_sales_marketing_expense",
            "past_stock_based_compensation",
            "past_depreciation_amortization",
            "past_general_admin_expense",
            "past_non_operating_expense",
            "past_last_processed_filing_date",
            "past_last_company_filing_date",
            "past_last_announced_date",
        ]

        # Process display names
        display_pairs = []
        for col in past_columns:

            # DISPLAY NAME ONLY — leave actual col intact
            display = col.replace("past_", "")
            display = display.replace("_", " ")
            display = display.replace("industry analysis ", "")
            display = display.replace("revenue segments ", "")
            display = display.title()

            display_pairs.append((display, col))

        # Alphabetize
        display_pairs = sorted(display_pairs, key=lambda x: x[0])

        # Split into left/right columns
        mid = len(display_pairs) // 2
        left_items = display_pairs[:mid]
        right_items = display_pairs[mid:]

        left, right = st.columns(2)

        # Left column
        with left:
            for display, col in left_items:
                value = sw_facts_df[col].iloc[0]
                st.markdown(
                    f"""
                    <div style='padding:6px 0;'>
                        <span style='font-size:20px;'>{display}:</span>
                        <span style='font-size:24px; font-weight:bold;'>{value}</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        # Right column
        with right:
            for display, col in right_items:
                value = sw_facts_df[col].iloc[0]
                st.markdown(
                    f"""
                    <div style='padding:6px 0;'>
                        <span style='font-size:20px;'>{display}:</span>
                        <span style='font-size:24px; font-weight:bold;'>{value}</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ------------------------------
    # 📈 PAST → PAST HISTORY
    # ------------------------------
    with st.expander("Past History", expanded=False):

        ICE_BLUE = "#7FDBFF"
        ICE_BLUE_DARK = "#3399CC"
        BG = "white"
        TEXT_COLOR = "black"

        def safe_fetch(col):
            """Return (numeric_value_or_0, text_label) where text_label is 'N/A' if missing."""
            try:
                val = sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, col].iloc[0]
                if pd.isna(val):
                    return 0, "N/A"
                try:
                    num = float(val)
                    return num, f"{num:,.3f}"
                except:
                    return 0, str(val)
            except:
                return 0, "N/A"

        def create_history_chart(title, cols, labels=None, round_digits=3):
            values = []
            text_labels = []
            for c in cols:
                v, txt = safe_fetch(c)
                if txt != "N/A" and txt.replace(",", "").replace(".", "", 1).lstrip("-").isdigit():
                    try:
                        val_num = float(txt.replace(",", ""))
                        txt_fmt = f"{val_num:,.{round_digits}f}"
                    except:
                        txt_fmt = txt
                    text_labels.append(txt_fmt)
                else:
                    text_labels.append(txt)
                values.append(v)

            if labels is None:
                labels = [f"Year {i}" for i in range(len(cols))]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=labels,
                y=values,
                marker_color=ICE_BLUE,
                marker_line=dict(width=1, color="rgba(0,0,0,0.1)"),
                text=text_labels,
                textposition="auto",
                textfont=dict(size=14, family="Arial", color=TEXT_COLOR),
                hoverinfo="skip",
                width=0.5
            ))

            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=20, color=TEXT_COLOR)),
                xaxis=dict(title="", tickfont=dict(size=12, color=TEXT_COLOR)),
                yaxis=dict(title="", tickfont=dict(size=12, color=TEXT_COLOR)),
                plot_bgcolor=BG,
                paper_bgcolor=BG,
                font_color=TEXT_COLOR,
                margin=dict(l=40, r=40, t=70, b=40),
                height=420,
                bargap=0.35,
                bargroupgap=0.15
            )

            st.plotly_chart(fig, use_container_width=True)

        # 1) Revenue LTM History
        create_history_chart("Revenue (LTM) History", [
            f"past_revenue_ltm_history_{i}" for i in range(11)
        ])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 2) Net Income LTM History
        create_history_chart("Net Income (LTM) History", [
            f"past_net_income_ltm_history_{i}" for i in range(11)
        ])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 3) Earnings Per Share LTM History
        create_history_chart("Earnings Per Share (LTM) History", [
            f"past_earnings_per_share_ltm_history_{i}" for i in range(11)
        ])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 4) EPS Summary 1y/3y/5y
        create_history_chart("Earnings Per Share (1Y/3Y/5Y)", [
            "past_earnings_per_share_1y",
            "past_earnings_per_share_3y",
            "past_earnings_per_share_5y"
        ], labels=["1 Year", "3 Year", "5 Year"])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 5) EPS Growth 1y/3y/5y
        create_history_chart("EPS Growth (1Y/3Y/5Y)", [
            "past_earnings_per_share_growth_1y",
            "past_earnings_per_share_growth_3y",
            "past_earnings_per_share_growth_5y"
        ], labels=["1 Year", "3 Year", "5 Year"])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 6) Net Income 1-5y
        create_history_chart("Net Income (1Y → 5Y)", [
            f"past_net_income_{i}y" for i in range(1, 6)
        ], labels=[f"{i} Year" for i in range(1, 6)])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 7) Net Income Growth 1y/3y/5y
        create_history_chart("Net Income Growth (1Y/3Y/5Y)", [
            "past_net_income_growth_1y",
            "past_net_income_growth_3y",
            "past_net_income_growth_5y"
        ], labels=["1 Year", "3 Year", "5 Year"])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 8) Revenue Growth 1y/3y/5y
        create_history_chart("Revenue Growth (1Y/3Y/5Y)", [
            "past_revenue_growth_1y",
            "past_revenue_growth_3y",
            "past_revenue_growth_5y"
        ], labels=["1 Year", "3 Year", "5 Year"])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 9) EBIT LTM History
        create_history_chart("EBIT (LTM) History", [
            f"past_ebit_ltm_history_{i}" for i in range(6)
        ])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 10) EBIT Single Growth 1y/3y/5y
        create_history_chart("EBIT Growth (1Y/3Y/5Y)", [
            "past_ebit_single_growth_1y",
            "past_ebit_single_growth_3y",
            "past_ebit_single_growth_5y"
        ], labels=["1 Year", "3 Year", "5 Year"])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 11) Capital Employed LTM History
        create_history_chart("Capital Employed (LTM) History", [
            f"past_capital_employed_ltm_history_{i}" for i in range(6)
        ])

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # 12) Return on Capital Employed LTM History
        create_history_chart("Return on Capital Employed (LTM) History", [
            f"past_return_on_capital_employed_ltm_history_{i}" for i in range(6)
        ])

# ---------------------------------------------------------
# 📊 DIVIDEND → DIVIDEND COMPARISONS
# ---------------------------------------------------------
with st.expander("Dividend"):

    with st.expander("Dividend Comparisons"):

        # Colors (same as Value/Past Comparisons)
        ICE_BLUE = "#7FDBFF"
        GRAY = "#888888"
        BLACK = "#000000"

        # ------------------------------
        # Helper bar chart function
        # ------------------------------
        def create_dividend_bar_chart(title, categories, values, colors):
            fig = go.Figure()

            for cat, val, colr in zip(categories, values, colors):

                # Switch to white text for black bar
                text_color = "white" if colr.lower() == "#000000" else "black"

                fig.add_trace(go.Bar(
                    x=[cat],
                    y=[val],
                    marker_color=colr,
                    width=0.45,
                    hoverinfo="skip",
                    text=[f"{val:,.3f}" if pd.notna(val) else "N/A"],
                    textposition="auto",
                    textfont=dict(size=14, color=text_color)
                ))

            fig.update_layout(
                title=dict(text=title, x=0.5, xanchor="center", font=dict(size=22, color="black")),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font_color="black",
                showlegend=False,
                height=350,
                margin=dict(l=20, r=20, t=60, b=20)
            )

            return fig

        # -------------------------------------
        # Dividend Comparison — Single Graph
        # -------------------------------------

        categories = ["Company", "Industry Avg", "All Companies Avg"]
        values = [
            snowflake_df.loc[snowflake_df["tickers"] == selected_ticker, "dividend"].iloc[0],
            sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_dividends_score"].iloc[0],
            sw_facts_df.loc[sw_facts_df["source_file"] == selected_ticker, "extended_data_industry_averages_all_dividends_score"].iloc[0]
        ]

        colors = [ICE_BLUE, GRAY, BLACK]

        st.plotly_chart(
            create_dividend_bar_chart(
                "Dividend Score Comparison",
                categories,
                values,
                colors
            ),
            use_container_width=True
        )

        # Legend
        st.markdown(f"""
            <div style="font-size:12px; margin-top:-10px; text-align:center;">
                <span style="color:{ICE_BLUE}; font-weight:bold;">■</span> Company Dividend &nbsp;&nbsp;
                <span style="color:{GRAY}; font-weight:bold;">■</span> Industry Average Dividend &nbsp;&nbsp;
                <span style="color:{BLACK}; font-weight:bold;">■</span> All Companies Average Dividend
            </div>
        """, unsafe_allow_html=True)

    # ---------------------------------------------------------
    # 📊 DIVIDEND → DIVIDEND STATISTICS EXPANDER
    # ---------------------------------------------------------
    with st.expander("Dividend Statistics", expanded=False):

        col_left_div, col_right_div = st.columns(2)

        # -------------------------
        # Get the most recent row for selected ticker
        # -------------------------
        sw_rows = sw_facts_df[sw_facts_df["source_file"] == selected_ticker]
        if not sw_rows.empty:
            sw = sw_rows.sort_values(by="date", ascending=False).iloc[0]  # Most recent row
        else:
            sw = None

        # -------------------------
        # Helper to extract values
        # -------------------------
        def get_div_stat(col):
            try:
                val = sw[col] if sw is not None else None
                if val is None or pd.isna(val):
                    return "N/A"
                if isinstance(val, pd.Timestamp) or pd.api.types.is_datetime64_any_dtype(type(val)):
                    return pd.to_datetime(val).strftime("%Y-%m-%d")
                if isinstance(val, str):
                    return val
                if isinstance(val, (int, float)):
                    return round(val, 3)
                return str(val)
            except:
                return "N/A"

        # -------------------------
        # Columns to display
        # -------------------------
        dividend_stats_columns = [
            "dividend_current",
            "dividend_future",
            "dividend_dividend_paying_years",
            "dividend_payout_ratio",
            "dividend_payout_ratio_3y",
            "dividend_dividend_yield_growth_annual",
            "dividend_first_payment",
            "dividend_last_payment",
            "dividend_buyback_yield",
            "dividend_total_shareholder_yield",
            "dividend_payout_ratio_median_3yr",
            "dividend_dividend_payments_growth_annual",
            "dividend_dividend_payments_ltm",
            "dividend_cash_payout_ratio",
            "dividend_dividend_currency_iso",

            # value_intrinsic_value fields
            "value_intrinsic_value_dividend_discount_dps",
            "value_intrinsic_value_dividend_discount_roe",
            "value_intrinsic_value_dividend_discount_payout",
            "value_intrinsic_value_dividend_discount_ddm_growth",
            "value_intrinsic_value_dividend_discount_npv_per_share",
            "value_intrinsic_value_dividend_discount_expected_growth",

            # upcoming dividend data (dates included)
            "dividend_upcoming_dividend_date",
            "dividend_upcoming_dividend_amount",
            "dividend_upcoming_dividend_pay_date",
            "dividend_upcoming_dividend_record_date",
            "dividend_upcoming_dividend_adjustment_factor",
            "dividend_upcoming_dividend_split_adjusted_amount",
        ]

        # -------------------------
        # Clean Labels
        # - Remove duplicate “Dividend”
        # - Remove “value_intrinsic_value”
        # - Convert underscores → spaces
        # - Title-case the result
        # -------------------------
        cleaned_stats = {}
        for col in dividend_stats_columns:
            label = col.replace("value_intrinsic_value_", "")
            label = label.replace("dividend_dividend", "dividend")
            label = label.replace("_", " ").title()
            cleaned_stats[label] = col

        # -------------------------
        # Alphabetize
        # -------------------------
        alphabetized = dict(sorted(cleaned_stats.items(), key=lambda x: x[0].lower()))

        # Split evenly into left/right columns
        items = list(alphabetized.items())
        mid = len(items) // 2
        left_items = items[:mid]
        right_items = items[mid:]

        # -------------------------
        # LEFT COLUMN
        # -------------------------
        with col_left_div:
            for label, col in left_items:
                st.markdown(
                    f"<div style='font-size:20px; margin-bottom:6px;'><b>{label}:</b> "
                    f"<span style='font-size:24px; font-weight:bold;'>{get_div_stat(col)}</span></div>",
                    unsafe_allow_html=True
                )

        # -------------------------
        # RIGHT COLUMN
        # -------------------------
        with col_right_div:
            for label, col in right_items:
                st.markdown(
                    f"<div style='font-size:20px; margin-bottom:6px;'><b>{label}:</b> "
                    f"<span style='font-size:24px; font-weight:bold;'>{get_div_stat(col)}</span></div>",
                    unsafe_allow_html=True
                )

    # ---------------------------------------------------------
    # 📊 DIVIDEND → DIVIDEND HISTORY EXPANDER
    # ---------------------------------------------------------
    with st.expander("Dividend History", expanded=False):

        # Helper to safely extract numeric values
        def get_hist_value(col):
            try:
                val = sw_facts_df.loc[
                    sw_facts_df["source_file"] == selected_ticker, col
                ].iloc[0]

                if isinstance(val, (int, float)):
                    return round(val, 3)

                return None
            except:
                return None

        # -------------------------------------------------
        # FIRST GRAPH — Dividend Payments Single-Year Growth
        # -------------------------------------------------
        columns = [
            "dividend_dividend_payments_single_growth_1y",
            "dividend_dividend_payments_single_growth_3y",
            "dividend_dividend_payments_single_growth_5y",
        ]

        labels = ["1Y", "3Y", "5Y"]

        values = [get_hist_value(c) for c in columns]

        # Filter only non-null values
        df = pd.DataFrame({
            "Period": [lbl for lbl, val in zip(labels, values) if val is not None],
            "Value":  [val for val in values if val is not None]
        })

        # Ice blue only
        ICE_BLUE = "#7FDBFF"

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df["Period"],
            y=df["Value"],
            marker=dict(color=ICE_BLUE,),
            text=[str(v) for v in df["Value"]],
            textposition="outside",
            width=0.6
        ))

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False,
            xaxis=dict(title=""),
            yaxis=dict(title=""),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )

        st.markdown(
            "<h3 style='margin-top:20px; margin-bottom:0;'>Dividend Payments Growth</h3>",
            unsafe_allow_html=True
        )

        st.plotly_chart(fig, use_container_width=True)
        
# ---------------------------------------------------------
# 📊 MANAGEMENT EXPANDER
# ---------------------------------------------------------
with st.expander("Management", expanded=False):
        
    with st.expander("Management Statistics", expanded=False):

        # Columns provided (minus the one removed)
        mgmt_columns = [
            "health_management_rate_return",
            "management_management_tenure",
            "management_board_tenure",
            "management_management_age",
            "management_board_age",
            "management_insider_buying_ratio",
            "management_total_shares_bought",
            "management_total_shares_sold",
            "management_total_employees",
            "management_ceo_salary_growth_1y",

            # CEO compensation statement data
            "extended_data_statements_management_ceoover_compensation_statement_data_ceo_name",
            "extended_data_statements_management_ceoover_compensation_statement_data_market",
            "extended_data_statements_management_ceoover_compensation_statement_data_median_compensation_usd",
            "extended_data_statements_management_ceoover_compensation_statement_data_ceo_compensation_total_usd",
            "extended_data_statements_management_ceosalary_growth_statement_data_earnings_per_share",
        ]

        # Column that must be removed entirely
        # (you explicitly requested this)
        remove_column = "extended_data_statements_management_ceoover_compensation_statement_data"
        mgmt_columns = [c for c in mgmt_columns if c != remove_column]

        # -------------------------------------------------
        # Helper → get values safely
        # -------------------------------------------------
        def get_val(col):
            try:
                val = sw_facts_df.loc[
                    sw_facts_df["source_file"] == selected_ticker,
                    col
                ].iloc[0]

                # Text remains text
                if isinstance(val, str):
                    return val

                return round(val, 3)
            except:
                return "N/A"

        # -------------------------------------------------
        # Clean + Format Display Names
        # -------------------------------------------------
        cleaned_items = {}

        for col in mgmt_columns:
            disp = col

            # Remove health_
            disp = disp.replace("health_", "")

            # Remove duplicate "management"
            disp = disp.replace("management_management_", "management_")

            # Remove long prefixes
            disp = disp.replace("extended_data_statements_management_", "")
            disp = disp.replace("ceoover_compensation_statement_data_", "")
            disp = disp.replace("ceosalary_growth_statement_data_", "")

            # Remove leading prefix "management_" BEFORE title-case
            # (but keep it if it's meaningful, do not remove suffix uses)
            disp = disp.replace("management_", "management ")

            # Replace underscores → spaces
            disp = disp.replace("_", " ")

            # Fix double spaces caused by replacements
            while "  " in disp:
                disp = disp.replace("  ", " ")

            disp = disp.strip().title()

            cleaned_items[disp] = col

        # -------------------------------------------------
        # Alphabetize
        # -------------------------------------------------
        sorted_items = dict(sorted(cleaned_items.items()))

        # Split evenly into left and right columns
        mid = len(sorted_items) // 2
        left_items = list(sorted_items.items())[:mid]
        right_items = list(sorted_items.items())[mid:]

        # -------------------------------------------------
        # Build Streamlit columns
        # -------------------------------------------------
        col_left, col_right = st.columns(2)

        # LEFT COLUMN
        with col_left:
            for label, col in left_items:
                st.markdown(
                    f"<div style='font-size:20px;'>{label}:</div>"
                    f"<div style='font-size:24px; font-weight:bold; margin-bottom:12px;'>{get_val(col)}</div>",
                    unsafe_allow_html=True
                )

        # RIGHT COLUMN
        with col_right:
            for label, col in right_items:
                st.markdown(
                    f"<div style='font-size:20px;'>{label}:</div>"
                    f"<div style='font-size:24px; font-weight:bold; margin-bottom:12px;'>{get_val(col)}</div>",
                    unsafe_allow_html=True
                )

    with st.expander("Ownership Composition"):

        # --- FILTER DATA FOR SELECTED TICKER ---
        ownership_ticker_df = ownership_df[ownership_df["ticker"] == selected_ticker]
        if ownership_ticker_df.empty:
            st.write("No ownership data available for this ticker.")
        else:

            # --- MAPPING FOR BAR CHART ---
            percent_cols = {
                "Institutions": "institutions_percent",
                "Public Companies": "public_companies_percent",
                "Private Companies": "private_companies_percent",
                "Individual Insiders": "individual_insiders_percent",
                "VC/PE Firms": "vcpe_firms_percent",
                "General Public": "general_public_percent"
            }

            share_cols = {
                "Institutions": "institutions_shares",
                "Public Companies": "public_companies_shares",
                "Private Companies": "private_companies_shares",
                "Individual Insiders": "individual_insiders_shares",
                "VC/PE Firms": "vcpe_firms_shares",
                "General Public": "general_public_shares"
            }

            # --- CUSTOM COLORS ---
            ICE_BLUE = "#7FDBFF"
            color_map = {
                "Institutions": "#888888",       # gray
                "Public Companies": "#ffb5b5",   # light red
                "Private Companies": "#cc0000",  # dark red
                "Individual Insiders": "#e15cae", # pink
                "VC/PE Firms": "#a35aec",        # light purple
                "General Public": ICE_BLUE
            }

            # --- BUILD CHART DATA ---
            ownership_data = []

            for label, pct_col in percent_cols.items():
                if pct_col in ownership_ticker_df.columns and share_cols[label] in ownership_ticker_df.columns:
                    pct_value = ownership_ticker_df[pct_col].iloc[0]
                    shares_value = ownership_ticker_df[share_cols[label]].iloc[0]
                    if pd.notnull(pct_value):
                        pct_value = round(float(pct_value), 2)  # round to nearest 100th
                    ownership_data.append({
                        "Category": label,
                        "Percent": pct_value,
                        "Shares": shares_value,
                        "Color": color_map[label]
                    })

            # Sort descending by percent
            ownership_df_plot = pd.DataFrame(ownership_data).sort_values("Percent", ascending=False)

            # --- PLOT ---
            import plotly.graph_objects as go

            fig = go.Figure()

            for i, row in ownership_df_plot.iterrows():
                fig.add_trace(go.Bar(
                    x=[row["Category"]],
                    y=[row["Percent"]],
                    marker_color=row["Color"],
                    hovertemplate=f"<b>Shares:</b> {row['Shares']}<extra></extra>",
                    text=f"{row['Percent']}%",
                    textposition="auto",
                    textfont=dict(size=14, family="Arial", color="black")
                ))

            fig.update_layout(
                title=dict(text="Ownership Composition", x=0.5, xanchor="center", font=dict(size=24)),
                xaxis=dict(title="Category", tickfont=dict(size=14, family="Arial", color="black")),
                yaxis=dict(title="Percent (%)", tickfont=dict(size=14, family="Arial", color="black")),
                showlegend=False,
                height=450,
                margin=dict(l=20, r=20, t=60, b=20)
            )

            st.plotly_chart(fig, use_container_width=True)

    # ---------------- Company Holders Expander ----------------
    with st.expander("Company Holders", expanded=False):

        # Load Data
        df = sql["company_info"].copy()

        # Ensure columns are strings & lowercase
        df.columns = df.columns.astype(str).str.lower()

        if "ticker" in df.columns and "holding_date" in df.columns:
            # Strip spaces and uppercase for safety
            df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
            ticker_to_use = selected_ticker.strip().upper()

            # Filter for selected ticker first
            filtered_df = df.loc[df["ticker"] == ticker_to_use].copy()

            if not filtered_df.empty:
                # Sort by holding_date descending to get most recent values
                filtered_df["holding_date"] = pd.to_datetime(filtered_df["holding_date"], errors="coerce")
                filtered_df = filtered_df.sort_values("holding_date", ascending=False).head(100)

                # Columns to display
                display_columns = {
                    "owner_name": "Owner Name",
                    "owner_type": "Owner Type",
                    "shares_held": "Shares Held",
                    "percent_shares_outstanding": "Percent Shares Outstanding",
                    "percent_of_portfolio": "Percent of Portfolio",
                    "holding_date": "Holding Date"
                }

                # Keep only existing columns
                filtered_columns = [col for col in display_columns.keys() if col in filtered_df.columns]
                display_df = filtered_df[filtered_columns].rename(
                    columns={col: display_columns[col] for col in filtered_columns}
                )

                # Apply styling: bold headers and hover highlight
                st.markdown(
                    """
                    <style>
                    th {
                        font-size: 16px;
                        font-weight: bold;
                        text-align: left;
                    }
                    td {
                        font-size: 14px;
                    }
                    tbody tr:hover {
                        background-color: #f0f8ff;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                # Display interactive dataframe
                st.dataframe(display_df, use_container_width=True, height=500)

            else:
                st.write("No holder data available for this ticker.")
        else:
            st.write("Ticker or holding_date column missing in company_info_df.")

    # ---------------- Insider Trading Expander ----------------
    with st.expander("Insider Trading", expanded=False):

        # Load Data
        df = sql["insider_transactions"].copy()

        # Ensure columns are strings & lowercase
        df.columns = df.columns.astype(str).str.lower()

        if "ticker" in df.columns and "filing_date" in df.columns:
            # Strip spaces and uppercase for safety
            df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
            ticker_to_use = selected_ticker.strip().upper()

            # Filter for selected ticker first
            filtered_df = df.loc[df["ticker"] == ticker_to_use].copy()

            if not filtered_df.empty:
                # Sort by holding_date descending to get most recent values
                filtered_df["filing_date"] = pd.to_datetime(filtered_df["filing_date"], errors="coerce")
                filtered_df = filtered_df.sort_values("filing_date", ascending=False).head(100)

                # Columns to display
                display_columns = {
                    "filing_date": "Filing Date",
                    "owner_name": "Owner Name",
                    "owner_type": "Owner Type",
                    "transaction_type": "Transaction Type",
                    "shares": "Shares",
                    "price_max": "Price Max",
                    "transaction_value": "Transaction Value",
                }

                # Keep only existing columns
                filtered_columns = [col for col in display_columns.keys() if col in filtered_df.columns]
                display_df = filtered_df[filtered_columns].rename(
                    columns={col: display_columns[col] for col in filtered_columns}
                )

                # Apply styling: bold headers and hover highlight
                st.markdown(
                    """
                    <style>
                    th {
                        font-size: 16px;
                        font-weight: bold;
                        text-align: left;
                    }
                    td {
                        font-size: 14px;
                    }
                    tbody tr:hover {
                        background-color: #f0f8ff;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                # Display interactive dataframe
                st.dataframe(display_df, use_container_width=True, height=500)

            else:
                st.write("No insider trading data available for this ticker.")

st.markdown("---")

# =========================================================
# NORMALIZE TICKERS (RUN ONCE)
# =========================================================

def normalize_ticker(series):
    return series.astype(str).str.upper().str.strip()

# Simply Wall St
if not sw_facts_df.empty:
    sw_facts_df = sw_facts_df.copy()
    sw_facts_df["ticker"] = normalize_ticker(sw_facts_df["source_file"])
    sw_facts_df["date"] = pd.to_datetime(sw_facts_df["date"], errors="coerce")

# Stock prices
if not stock_df.empty:
    stock_df = stock_df.copy()
    stock_df["ticker"] = normalize_ticker(stock_df["tickers"])
    stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"], errors="coerce")

# Insider transactions
insider_df = sql["insider_transactions"].copy()
if not insider_df.empty:
    insider_df.columns = insider_df.columns.str.lower()
    insider_df["ticker"] = normalize_ticker(insider_df["ticker"])
    insider_df["filing_date"] = pd.to_datetime(insider_df["filing_date"], errors="coerce")

# Company info / holders
company_info_df = sql["company_info"].copy()
if not company_info_df.empty:
    company_info_df.columns = company_info_df.columns.str.lower()
    company_info_df["ticker"] = normalize_ticker(company_info_df["ticker"])
    company_info_df["holding_date"] = pd.to_datetime(company_info_df["holding_date"], errors="coerce")

# Ownership
ownership_df = sql["ownership_breakdown"].copy()
if not ownership_df.empty:
    ownership_df["ticker"] = normalize_ticker(ownership_df["ticker"])
    ownership_df["html_creation_date"] = pd.to_datetime(
        ownership_df["html_creation_date"], errors="coerce"
    )

# =========================================================
# Extract selected ticker and company name (once)
# =========================================================
# selected_option format example: "AAPL - Apple Inc."
selected_ticker, selected_company = selected_option.split(" - ")
selected_ticker = selected_ticker.strip().upper()
selected_company = selected_company.strip()

# =========================================================
# SELECTED TICKER
# =========================================================
selected_ticker = selected_option.split(" - ")[0].strip().upper()

# =========================================================
# STOCK TECHNICALS (MOST RECENT)
# =========================================================
metric_map = {
    "rsi_5": "RSI 5",
    "rsi_14": "RSI 14",
    "rsi_30": "RSI 30",
    "rsi_50": "RSI 50",
    "sma_10": "SMA 10",
    "sma_50": "SMA 50",
    "sma_200": "SMA 200",
    "std_dev_10": "Std Dev 10",
    "std_dev_20": "Std Dev 20",
    "std_dev_100": "Std Dev 100",
    "close_price": "Close Price"
}

def build_stock_snapshot(df, ticker):
    df = df[df["ticker"] == ticker]
    if df.empty:
        return "### 📊 Stock Metrics\nNo data available."

    row = df.loc[df["trade_date"].idxmax()]
    lines = ["### 📊 Stock Metrics (Most Recent)"]
    for col, label in metric_map.items():
        val = row.get(col)
        lines.append(f"{label}: {val:,.4f}" if pd.notna(val) else f"{label}: N/A")
    return "\n".join(lines)

# =========================================================
# SIMPLY WALL ST VALUATION
# =========================================================
valuation_stats_map = {
    "Value Score": "extended_data_scores_value",
    "Industry Average Value Score": "extended_data_industry_averages_value_score",
    "All Companies Average Value Score": "extended_data_industry_averages_all_value_score",
    "Market Cap": "value_market_cap",
    "Industry Average Market Cap": "extended_data_industry_averages_market_cap",
    "All Companies Average Market Cap": "extended_data_industry_averages_all_market_cap",
    "PE Ratio": "value_pe",
    "Industry Average PE": "extended_data_industry_averages_pe",
    "All Companies Average PE": "extended_data_industry_averages_all_pe",
    "PB Ratio": "value_pb",
    "Industry Average PB": "extended_data_industry_averages_pb",
    "All Companies Average PB": "extended_data_industry_averages_all_pb",
    "PEG Ratio": "value_peg",
    "Industry Average PEG": "extended_data_industry_averages_peg",
    "All Companies Average PEG": "extended_data_industry_averages_all_peg",
    "Intrinsic Discount": "value_intrinsic_discount",
    "Industry Average Intrinsic Discount": "extended_data_industry_averages_intrinsic_discount",
    "All Companies Average Intrinsic Discount": "extended_data_industry_averages_all_intrinsic_discount",
    "ROE": "roe",
    "Industry Average ROE": "extended_data_industry_averages_roe",
    "All Companies Average ROE": "extended_data_industry_averages_all_roe",
    "ROA": "roa",
    "Industry Average ROA": "extended_data_industry_averages_roa",
    "All Companies Average ROA": "extended_data_industry_averages_all_roa",
    "Levered Beta": "value_intrinsic_value_levered_beta",
    "Industry Average Levered Beta": "extended_data_industry_averages_levered_beta",
    "All Companies Average Levered Beta": "extended_data_industry_averages_all_levered_beta",
    "Unlevered Beta": "value_intrinsic_value_unlevered_beta",
    "Industry Average Unlevered Beta": "extended_data_industry_averages_unlevered_beta",
    "All Companies Average Unlevered Beta": "extended_data_industry_averages_all_unlevered_beta",
    "Two-Stage FCF Capital to Revenue Ratio 3Y Avg": "value_intrinsic_value_two_stage_fcf_capital_to_revenue_ratio_3yr_avg",
    "Cost of Equity": "value_intrinsic_value_cost_of_equity",
    "ADR per Share": "value_intrinsic_value_adr_per_share",
    "Market Cap Band": "value_market_cap_band",
    "NPV per Share": "value_npv_per_share",
    "PV 5Y": "value_intrinsic_value_pv_5y",
    "PVTv": "value_intrinsic_value_pvtv",
    "Risk Free Rate": "value_intrinsic_value_risk_free_rate",
    "Tax Rate": "value_intrinsic_value_tax_rate",
    "Two-Stage FCF Growth CAGR 5Y": "value_intrinsic_value_two_stage_fcf_growth_cagr_5y",
    "Two-Stage FCF Shares Outstanding": "value_intrinsic_value_two_stage_fcf_shares_outstanding",
    "Equity Premium": "value_intrinsic_value_equity_premium",
    "EV to EBITDA": "value_ev_to_ebitda",
    "EV to Sales": "value_ev_to_sales",
    "Excess Return": "value_intrinsic_value_excess_returns_excess_return",
    "Excess Returns Book Value": "value_intrinsic_value_excess_returns_book_value",
    "Excess Returns Equity Cost": "value_intrinsic_value_excess_returns_equity_cost",
    "Excess Returns ROE Avg": "value_intrinsic_value_excess_returns_return_on_equity_avg",
    "Excess Returns Stable Book Value": "value_intrinsic_value_excess_returns_stable_book_value",
    "Excess Returns Stable EPS": "value_intrinsic_value_excess_returns_stable_eps",
    "Terminal Value": "value_intrinsic_value_terminal_value",
    "Two-Stage FCF First Stage 2026 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2026_data",
    "Two-Stage FCF First Stage 2027 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2027_data",
    "Two-Stage FCF First Stage 2028 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2028_data",
    "Two-Stage FCF First Stage 2029 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2029_data",
    "Two-Stage FCF First Stage 2030 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2030_data",
    "Two-Stage FCF First Stage 2031 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2031_data",
    "Two-Stage FCF First Stage 2032 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2032_data",
    "Two-Stage FCF First Stage 2033 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2033_data",
    "Two-Stage FCF First Stage 2034 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2034_data",
    "Two-Stage FCF First Stage 2035 Data": "value_intrinsic_value_two_stage_fcf_first_stage_2035_data",
    "Two-Stage FCF First Stage 2026 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2026_discounted",
    "Two-Stage FCF First Stage 2027 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2027_discounted",
    "Two-Stage FCF First Stage 2028 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2028_discounted",
    "Two-Stage FCF First Stage 2029 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2029_discounted",
    "Two-Stage FCF First Stage 2030 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2030_discounted",
    "Two-Stage FCF First Stage 2031 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2031_discounted",
    "Two-Stage FCF First Stage 2032 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2032_discounted",
    "Two-Stage FCF First Stage 2033 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2033_discounted",
    "Two-Stage FCF First Stage 2034 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2034_discounted",
    "Two-Stage FCF First Stage 2035 Discounted": "value_intrinsic_value_two_stage_fcf_first_stage_2035_discounted",
    "Health Score": "extended_data_scores_health",
    "Industry Average Health Score": "extended_data_industry_averages_health_score",
    "All Companies Average Health Score": "extended_data_industry_averages_all_health_score",
    "Accounts Payable": "health_accounts_payable",
    "Accounts Receivable %": "health_accounts_receivable_percent",
    "Accounts Receivable Growth 1Y": "health_accounts_receivable_growth_1y",
    "Aggregate Accruals": "health_aggregate_accruals",
    "Capex": "health_capex",
    "Capex Growth 1Y": "health_capex_growth_1y",
    "Capex Growth Annual": "health_capex_growth_annual",
    "Cash from Investing": "health_cash_from_investing",
    "Cash from Investing 1Y": "health_cash_from_investing_1y",
    "Cash Operating": "health_cash_operating",
    "Cash Operating Growth 1Y": "health_cash_operating_growth_1y",
    "Current Assets": "health_current_assets",
    "Current Assets to Long Term Liabilities": "health_current_assets_to_long_term_liab",
    "Current Assets to Total Debt": "health_current_assets_to_total_debt",
    "Current Portion Lease Liabilities": "health_current_portion_lease_liabilities",
    "Current Solvency Ratio": "health_current_solvency_ratio",
    "Debt to Equity Ratio": "health_debt_to_equity_ratio",
    "Debt to Equity Ratio Past": "health_debt_to_equity_ratio_past",
    "Fixed Assets to Total Assets": "health_fixed_to_total_assets",
    "Inventory": "health_inventory",
    "Inventory Growth 1Y": "health_inventory_growth_1y",
    "Last Balance Sheet Update": "health_last_balance_sheet_update",
    "Levered Free Cash Flow Break Even Years": "health_levered_free_cash_flow_break_even_years",
    "Levered Free Cash Flow Growth Annual": "health_levered_free_cash_flow_growth_annual",
    "Levered Free Cash Flow Growth Years": "health_levered_free_cash_flow_growth_years",
    "Levered Free Cash Flow Stable Years": "health_levered_free_cash_flow_stable_years",
    "Long Term Assets": "health_long_term_assets",
    "Long Term Debt": "health_long_term_debt",
    "Long Term Liabilities": "health_long_term_liab",
    "Long Term Portion Lease Liabilities": "health_long_term_portion_lease_liabilities",
    "Management Rate of Return": "health_management_rate_return",
    "Median 2Y Net Income": "health_median_2yr_net_income",
    "Net Debt": "health_net_debt",
    "Net Debt to EBITDA": "health_net_debt_to_ebitda",
    "Net Debt to Equity": "health_net_debt_to_equity",
    "Net Income": "health_net_income",
    "Net Interest Cover": "health_net_interest_cover",
    "Net Interest Expense": "health_net_interest_expense",
    "Net Operating Assets": "health_net_operating_assets",
    "Net Operating Assets 1Y": "health_net_operating_assets_1y",
    "Operating Cash Flow to Total Debt": "health_operating_cash_flow_to_total_debt",
    "Operating Expenses": "health_operating_expenses",
    "Operating Expenses Growth Annual": "health_operating_expenses_growth_annual",
    "Operating Expenses Growth Years": "health_operating_expenses_growth_years",
    "Operating Expenses Stable Years": "health_operating_expenses_stable_years",
    "PPE": "health_ppe",
    "Receivables": "health_receivables",
    "Restricted Cash": "health_restricted_cash",
    "Restricted Cash Ratio": "health_restricted_cash_ratio",
    "Total Assets": "health_total_assets",
    "Total Debt": "health_total_debt",
    "Total Equity": "health_total_equity",
    "Total Inventory": "health_total_inventory",
    "Total Liabilities to Equity": "health_total_liab_equity",
    "Total Lease Liabilities": "health_total_lease_liabilities",
    "Total Debt to Equity": "health_total_debt_equity",
    "Net Interest Margin": "health_industry_analysis_net_int_margin",
    "Net Loans": "health_industry_analysis_net_loans",
    "Net Loans to Deposits": "health_industry_analysis_net_loans_to_deposits",
    "Net Loans to Total Assets": "health_industry_analysis_net_loans_to_total_assets",
    "Non-Performing Loans to Total Loans": "health_industry_analysis_non_perf_loans_total_loans",
    "Loan Losses": "health_industry_analysis_loan_losses",
    "Allowance for Loan Losses": "health_industry_analysis_allowance_loan_losses",
    "Allowance for Non-Performing Loans": "health_industry_analysis_allowance_non_perf_loans",
    "Total Bank Liabilities": "health_industry_analysis_total_bank_liabilities",
    "Total Deposits": "health_industry_analysis_total_deposits",
    "Capitalisation %": "health_capitalisation_percent",
    "Capitalisation % 1Y": "health_capitalisation_percent_1y",
    "Book Value per Share": "health_book_value_per_share",
    "Net Operating Assets LTM History 0": "health_net_operating_assets_ltm_history_0",
    "Net Operating Assets LTM History 1": "health_net_operating_assets_ltm_history_1",
    "Net Operating Assets LTM History 2": "health_net_operating_assets_ltm_history_2",
    "Net Operating Assets LTM History 3": "health_net_operating_assets_ltm_history_3",
    "Aggregate Accruals LTM History 0": "health_aggregate_accruals_ltm_history_0",
    "Aggregate Accruals LTM History 1": "health_aggregate_accruals_ltm_history_1",
    "Aggregate Accruals LTM History 2": "health_aggregate_accruals_ltm_history_2",
    "Aggregate Accruals LTM History 3": "health_aggregate_accruals_ltm_history_3",
    "Accrual Ratio from Cashflow LTM History 0": "health_accrual_ratio_from_cashflow_ltm_history_0",
    "Accrual Ratio from Cashflow LTM History 1": "health_accrual_ratio_from_cashflow_ltm_history_1",
    "Accrual Ratio from Cashflow LTM History 2": "health_accrual_ratio_from_cashflow_ltm_history_2",
    "Total Assets LTM History 0": "health_total_assets_ltm_history_0",
    "Total Assets LTM History 1": "health_total_assets_ltm_history_1",
    "Total Assets LTM History 2": "health_total_assets_ltm_history_2",
    "Total Assets LTM History 3": "health_total_assets_ltm_history_3",
    "Total Assets LTM History 4": "health_total_assets_ltm_history_4",
    "Total Assets LTM History 5": "health_total_assets_ltm_history_5",
    "Total Current Liabilities LTM History 0": "health_total_current_liab_ltm_history_0",
    "Total Current Liabilities LTM History 1": "health_total_current_liab_ltm_history_1",
    "Total Current Liabilities LTM History 2": "health_total_current_liab_ltm_history_2",
    "Total Current Liabilities LTM History 3": "health_total_current_liab_ltm_history_3",
    "Total Current Liabilities LTM History 4": "health_total_current_liab_ltm_history_4",
    "Total Current Liabilities LTM History 5": "health_total_current_liab_ltm_history_5",
    "Future Score": "extended_data_scores_future",
    "Industry Average Future Performance Score": "extended_data_industry_averages_future_performance_score",
    "All Companies Average Future Performance Score": "extended_data_industry_averages_all_future_performance_score",
    "Future Growth 1Y": "future_growth_1y",
    "Industry Average Future 1Y Growth": "extended_data_industry_averages_future_one_year_growth",
    "All Companies Average Future 1Y Growth": "extended_data_industry_averages_all_future_one_year_growth",
    "Future Growth 3Y": "future_growth_3y",
    "Industry Average Future 3Y Growth": "extended_data_industry_averages_future_three_year_growth",
    "All Companies Average Future 3Y Growth": "extended_data_industry_averages_all_future_three_year_growth",
    "Future ROE 1Y": "future_roe_1y",
    "Future ROE 3Y": "future_roe_3y",
    "Future Return on Equity 1Y": "future_return_on_equity_1y",
    "Future Return on Equity 3Y": "future_return_on_equity_3y",
    "Future EPS Growth 1Y": "future_earnings_per_share_growth_1y",
    "Future EPS Growth 3Y": "future_earnings_per_share_growth_3y",
    "Future Minimum Earnings Growth": "future_minimum_earnings_growth",
    "Future EPS Growth Annual": "future_earnings_per_share_growth_annual",
    "Future Revenue Growth Annual": "future_revenue_growth_annual",
    "Future Cash Ops Growth Annual": "future_cash_ops_growth_annual",
    "Future Net Income Growth Annual": "future_net_income_growth_annual",
    "Future EBITDA 1Y": "future_ebitda_1y",
    "Future EBITDA Growth 1Y": "future_ebitda_growth_1y",
    "Future Forward PE 1Y": "future_forward_pe_1y",
    "Future Forward Price to Sales 1Y": "future_forward_price_to_sales_1y",
    "Future Forward EV to EBITDA 1Y": "future_forward_ev_to_ebitda_1y",
    "Future Forward EV to Sales 1Y": "future_forward_ev_to_sales_1y",
    "Future Gross Profit Margin 1Y": "future_gross_profit_margin_1y",
    "Future EPS 1Y": "future_earnings_per_share_1y",
    "Future EPS 2Y": "future_earnings_per_share_2y",
    "Future EPS 3Y": "future_earnings_per_share_3y",
    "Future Revenue Growth 1Y": "future_revenue_growth_1y",
    "Future Revenue Growth 2Y": "future_revenue_growth_2y",
    "Future Revenue Growth 3Y": "future_revenue_growth_3y",
    "Future Revenue 1Y": "future_revenue_1y",
    "Future Revenue 2Y": "future_revenue_2y",
    "Future Revenue 3Y": "future_revenue_3y",
    "Future Cash Ops Growth 1Y": "future_cash_ops_growth_1y",
    "Future Cash Ops Growth 2Y": "future_cash_ops_growth_2y",
    "Future Cash Ops Growth 3Y": "future_cash_ops_growth_3y",
    "Future Cash Ops 1Y": "future_cash_ops_1y",
    "Future Cash Ops 2Y": "future_cash_ops_2y",
    "Future Cash Ops 3Y": "future_cash_ops_3y",
    "Future Net Income Growth 1Y": "future_net_income_growth_1y",
    "Future Net Income Growth 2Y": "future_net_income_growth_2y",
    "Future Net Income Growth 3Y": "future_net_income_growth_3y",
    "Future Net Income 1Y": "future_net_income_1y",
    "Future Net Income 2Y": "future_net_income_2y",
    "Future Net Income 3Y": "future_net_income_3y",
    "Past Score": "extended_data_scores_past",
    "Past Performance Score": "extended_data_industry_averages_past_performance_score",
    "All Companies Average Past Performance Score": "extended_data_industry_averages_all_past_performance_score",
    "Past Growth 1Y": "past_growth_1y",
    "Industry Average Past 1Y Growth": "extended_data_industry_averages_past_one_year_growth",
    "All Companies Average Past 1Y Growth": "extended_data_industry_averages_all_past_one_year_growth",
    "Past Growth 5Y": "past_growth_5y",
    "Industry Average Past 5Y Growth": "extended_data_industry_averages_past_five_year_growth",
    "All Companies Average Past 5Y Growth": "extended_data_industry_averages_all_past_five_year_growth",
    "Past Revenue USD": "past_revenue_usd",
    "Past Operating Revenue": "past_operating_revenue",
    "Past Net Income USD": "past_net_income_usd",
    "Past EPS": "past_earnings_per_share",
    "Past EPS 5Y Avg": "past_earnings_per_share_5y_avg",
    "Past EBT Excluding": "past_ebt_excluding",
    "Past Earnings Continued Ops": "past_earnings_continued_ops",
    "Past Total Assets": "past_total_assets",
    "Past Total Equity": "past_total_equity",
    "Past Current Liabilities": "past_current_liabilities",
    "Past ROE": "past_return_on_equity",
    "Past ROA": "past_return_on_assets",
    "Past Return on Capital Employed": "past_return_on_capital_employed",
    "Past Return on Capital Employed Past": "past_return_on_capital_employed_past",
    "Past Return on Capital Growth": "past_return_on_capital_growth",
    "Past Net Income 5Y Avg": "past_net_income_5y_avg",
    "Past Last Earnings Update": "past_last_earnings_update",
    "Past Last Earnings Update Annual": "past_last_earnings_update_annual",
    "Past D&A Expense": "past_industry_analysis_d_a_expense",
    "Past R&D Expense": "past_industry_analysis_r_d_expense",
    "Past Non-Op Expense": "past_industry_analysis_non_op_expense",
    "Past Sales & Marketing Expense": "past_industry_analysis_sales_marketing",
    "Past Banking Revenue Segments": "past_industry_analysis_revenue_segments_banking",
    "Past Stock Based Compensation": "past_industry_analysis_stock_based_comp",
    "Past General Administrative Expense": "past_industry_analysis_general_administrative",
    "Past Selling General Admin Expense": "past_industry_analysis_selling_general_admin_expenses",
    "Past EBIT": "past_ebit",
    "Past EBIT 3Y Avg": "past_ebit_3y_avg",
    "Past EBIT 5Y Avg": "past_ebit_5y_avg",
    "Past EPS Single Growth 3Y": "past_earnings_per_share_single_growth_3y",
    "Past EPS Single Growth 5Y": "past_earnings_per_share_single_growth_5y",
    "Past Gross Profit Margin 1Y": "past_gross_profit_margin_1y",
    "Past Net Income Margin": "past_net_income_margin",
    "Past Net Income Margin 1Y": "past_net_income_margin_1y",
    "Past Change in Unearned Revenue": "past_change_in_unearned_revenue",
    "Past Unearned Revenue % of Sales": "past_unearned_revenue_percent_of_sales",
    "Past EBT Including": "past_ebt_including",
    "Past Unusual Items": "past_unusual_items",
    "Past Unusual Item Ratio": "past_unusual_item_ratio",
    "Past Operating Revenue %": "past_operating_revenue_percent",
    "Past Years Profitable": "past_years_profitable",
    "Past Trading Since Years": "past_trading_since_years",
    "Past Non-Operating Revenue": "past_non_operating_revenue",
    "Past Non-Operating Revenue Ratio": "past_non_operating_revenue_ratio",
    "Past Non-Operating Revenue Ratio Delta": "past_non_operating_revenue_ratio_delta",
    "Past Income Tax to EBIT Ratio": "past_income_tax_to_ebit_ratio",
    "Past Banking Revenue Segments": "past_business_revenue_segments_banking",
    "Past Selling General Admin Expense": "past_selling_general_admin_expense",
    "Past R&D Expense": "past_research_development_expense",
    "Past Sales Marketing Expense": "past_sales_marketing_expense",
    "Past Stock Based Compensation": "past_stock_based_compensation",
    "Past Depreciation & Amortization": "past_depreciation_amortization",
    "Past General Admin Expense": "past_general_admin_expense",
    "Past Non-Operating Expense": "past_non_operating_expense",
    "Past Last Processed Filing Date": "past_last_processed_filing_date",
    "Past Last Company Filing Date": "past_last_company_filing_date",
    "Past Last Announced Date": "past_last_announced_date",
    "Past Revenue LTM History 0": "past_revenue_ltm_history_0",
    "Past Revenue LTM History 1": "past_revenue_ltm_history_1",
    "Past Revenue LTM History 2": "past_revenue_ltm_history_2",
    "Past Revenue LTM History 3": "past_revenue_ltm_history_3",
    "Past Revenue LTM History 4": "past_revenue_ltm_history_4",
    "Past Revenue LTM History 5": "past_revenue_ltm_history_5",
    "Past Revenue LTM History 6": "past_revenue_ltm_history_6",
    "Past Revenue LTM History 7": "past_revenue_ltm_history_7",
    "Past Revenue LTM History 8": "past_revenue_ltm_history_8",
    "Past Revenue LTM History 9": "past_revenue_ltm_history_9",
    "Past Revenue LTM History 10": "past_revenue_ltm_history_10",
    "Past Net Income LTM History 0": "past_net_income_ltm_history_0",
    "Past Net Income LTM History 1": "past_net_income_ltm_history_1",
    "Past Net Income LTM History 2": "past_net_income_ltm_history_2",
    "Past Net Income LTM History 3": "past_net_income_ltm_history_3",
    "Past Net Income LTM History 4": "past_net_income_ltm_history_4",
    "Past Net Income LTM History 5": "past_net_income_ltm_history_5",
    "Past Net Income LTM History 6": "past_net_income_ltm_history_6",
    "Past Net Income LTM History 7": "past_net_income_ltm_history_7",
    "Past Net Income LTM History 8": "past_net_income_ltm_history_8",
    "Past Net Income LTM History 9": "past_net_income_ltm_history_9",
    "Past Net Income LTM History 10": "past_net_income_ltm_history_10",
    "Past EPS LTM History 0": "past_earnings_per_share_ltm_history_0",
    "Past EPS LTM History 1": "past_earnings_per_share_ltm_history_1",
    "Past EPS LTM History 2": "past_earnings_per_share_ltm_history_2",
    "Past EPS LTM History 3": "past_earnings_per_share_ltm_history_3",
    "Past EPS LTM History 4": "past_earnings_per_share_ltm_history_4",
    "Past EPS LTM History 5": "past_earnings_per_share_ltm_history_5",
    "Past EPS LTM History 6": "past_earnings_per_share_ltm_history_6",
    "Past EPS LTM History 7": "past_earnings_per_share_ltm_history_7",
    "Past EPS LTM History 8": "past_earnings_per_share_ltm_history_8",
    "Past EPS LTM History 9": "past_earnings_per_share_ltm_history_9",
    "Past EPS LTM History 10": "past_earnings_per_share_ltm_history_10",
    "Past EPS 1Y": "past_earnings_per_share_1y",
    "Past EPS 3Y": "past_earnings_per_share_3y",
    "Past EPS 5Y": "past_earnings_per_share_5y",
    "Past EPS Growth 1Y": "past_earnings_per_share_growth_1y",
    "Past EPS Growth 3Y": "past_earnings_per_share_growth_3y",
    "Past EPS Growth 5Y": "past_earnings_per_share_growth_5y",
    "Past Net Income 1Y": "past_net_income_1y",
    "Past Net Income 2Y": "past_net_income_2y",
    "Past Net Income 3Y": "past_net_income_3y",
    "Past Net Income 4Y": "past_net_income_4y",
    "Past Net Income 5Y": "past_net_income_5y",
    "Past Net Income Growth 1Y": "past_net_income_growth_1y",
    "Past Net Income Growth 3Y": "past_net_income_growth_3y",
    "Past Net Income Growth 5Y": "past_net_income_growth_5y",
    "Past Revenue Growth 1Y": "past_revenue_growth_1y",
    "Past Revenue Growth 3Y": "past_revenue_growth_3y",
    "Past Revenue Growth 5Y": "past_revenue_growth_5y",
    "Past EBIT LTM History 0": "past_ebit_ltm_history_0",
    "Past EBIT LTM History 1": "past_ebit_ltm_history_1",
    "Past EBIT LTM History 2": "past_ebit_ltm_history_2",
    "Past EBIT LTM History 3": "past_ebit_ltm_history_3",
    "Past EBIT LTM History 4": "past_ebit_ltm_history_4",
    "Past EBIT LTM History 5": "past_ebit_ltm_history_5",
    "Past EBIT Single Growth 1Y": "past_ebit_single_growth_1y",
    "Past EBIT Single Growth 3Y": "past_ebit_single_growth_3y",
    "Past EBIT Single Growth 5Y": "past_ebit_single_growth_5y",
    "Past Capital Employed LTM History 0": "past_capital_employed_ltm_history_0",
    "Past Capital Employed LTM History 1": "past_capital_employed_ltm_history_1",
    "Past Capital Employed LTM History 2": "past_capital_employed_ltm_history_2",
    "Past Capital Employed LTM History 3": "past_capital_employed_ltm_history_3",
    "Past Capital Employed LTM History 4": "past_capital_employed_ltm_history_4",
    "Past Capital Employed LTM History 5": "past_capital_employed_ltm_history_5",
    "Past ROCE LTM History 0": "past_return_on_capital_employed_ltm_history_0",
    "Past ROCE LTM History 1": "past_return_on_capital_employed_ltm_history_1",
    "Past ROCE LTM History 2": "past_return_on_capital_employed_ltm_history_2",
    "Past ROCE LTM History 3": "past_return_on_capital_employed_ltm_history_3",
    "Past ROCE LTM History 4": "past_return_on_capital_employed_ltm_history_4",
    "Past ROCE LTM History 5": "past_return_on_capital_employed_ltm_history_5",
    "Dividend Score": "dividend",
    "Industry Average Dividends Score": "extended_data_industry_averages_dividends_score",
    "All Companies Average Dividends Score": "extended_data_industry_averages_all_dividends_score",
    "Current Dividend": "dividend_current",
    "Future Dividend": "dividend_future",
    "Dividend Paying Years": "dividend_dividend_paying_years",
    "Dividend Payout Ratio": "dividend_payout_ratio",
    "Dividend Payout Ratio 3Y": "dividend_payout_ratio_3y",
    "Dividend Yield Growth Annual": "dividend_dividend_yield_growth_annual",
    "First Dividend Payment": "dividend_first_payment",
    "Last Dividend Payment": "dividend_last_payment",
    "Buyback Yield": "dividend_buyback_yield",
    "Total Shareholder Yield": "dividend_total_shareholder_yield",
    "Dividend Payout Ratio Median 3Y": "dividend_payout_ratio_median_3yr",
    "Dividend Payments Growth Annual": "dividend_dividend_payments_growth_annual",
    "Dividend Payments LTM": "dividend_dividend_payments_ltm",
    "Cash Payout Ratio": "dividend_cash_payout_ratio",
    "Dividend Currency ISO": "dividend_dividend_currency_iso",
    "Dividend Discount DPS": "value_intrinsic_value_dividend_discount_dps",
    "Dividend Discount ROE": "value_intrinsic_value_dividend_discount_roe",
    "Dividend Discount Payout": "value_intrinsic_value_dividend_discount_payout",
    "Dividend Discount DDM Growth": "value_intrinsic_value_dividend_discount_ddm_growth",
    "Dividend Discount NPV per Share": "value_intrinsic_value_dividend_discount_npv_per_share",
    "Dividend Discount Expected Growth": "value_intrinsic_value_dividend_discount_expected_growth",
    "Upcoming Dividend Date": "dividend_upcoming_dividend_date",
    "Upcoming Dividend Amount": "dividend_upcoming_dividend_amount",
    "Upcoming Dividend Pay Date": "dividend_upcoming_dividend_pay_date",
    "Upcoming Dividend Record Date": "dividend_upcoming_dividend_record_date",
    "Upcoming Dividend Adjustment Factor": "dividend_upcoming_dividend_adjustment_factor",
    "Upcoming Dividend Split Adjusted Amount": "dividend_upcoming_dividend_split_adjusted_amount",
    "Dividend Payments Single Growth 1Y": "dividend_dividend_payments_single_growth_1y",
    "Dividend Payments Single Growth 3Y": "dividend_dividend_payments_single_growth_3y",
    "Dividend Payments Single Growth 5Y": "dividend_dividend_payments_single_growth_5y",
    "Management Rate of Return": "health_management_rate_return",
    "Management Tenure": "management_management_tenure",
    "Board Tenure": "management_board_tenure",
    "Price Target Analyst Count": "value_price_target_analyst_count",
    "Price Target Low": "value_price_target_low",
    "Price Target": "value_price_target",
    "Price Target High": "value_price_target_high"
}

def build_sw_snapshot(df, ticker):
    df = df[df["ticker"] == ticker]
    if df.empty:
        return "\n--- Simply Wall St Valuation ---\nNo data available."

    row = df.loc[df["date"].idxmax()]
    lines = ["\n--- Simply Wall St Valuation ---"]

    for label, col in valuation_stats_map.items():
        val = row.get(col)
        # Safe numeric formatting
        if pd.notna(val):
            try:
                # Only format if it's int or float
                if isinstance(val, (int, float)):
                    lines.append(f"{label}: {val:,.4f}")
                else:
                    lines.append(f"{label}: {val}")
            except:
                lines.append(f"{label}: {val}")
        else:
            lines.append(f"{label}: N/A")

    return "\n".join(lines)

# =========================================================
# OWNERSHIP COMPOSITION
# =========================================================
def build_ownership_snapshot(df, ticker):
    df = df[df["ticker"] == ticker]
    if df.empty:
        return "\n--- Ownership Composition ---\nNo data available."

    row = df.loc[df["html_creation_date"].idxmax()]
    return f"""
--- Ownership Composition (Most Recent) ---
Institutions: {row.get('institutions_percent','N/A')}
Insiders: {row.get('individual_insiders_percent','N/A')}
General Public: {row.get('general_public_percent','N/A')}
"""

# =========================================================
# COMPANY HOLDERS
# =========================================================
def build_company_holders_snapshot(df, ticker, n=5):
    df = df[df["ticker"] == ticker].sort_values(
        "holding_date", ascending=False
    ).head(n)
    if df.empty:
        return "\n--- Company Holders ---\nNo data available."

    lines = ["\n--- Company Holders (Most Recent 5) ---"]
    for _, r in df.iterrows():
        lines.append(
            f"{r['holding_date'].date()} | {r.get('owner_name','N/A')} | Shares: {r.get('shares_held','N/A')}"
        )
    return "\n".join(lines)

# =========================================================
# INSIDER TRANSACTIONS
# =========================================================
def build_insider_snapshot(df, ticker, n=5):
    df = df[df["ticker"] == ticker].sort_values(
        "filing_date", ascending=False
    ).head(n)
    if df.empty:
        return "\n--- Insider Transactions ---\nNo data available."

    lines = ["\n--- Insider Transactions (Most Recent 5) ---"]
    for _, r in df.iterrows():
        lines.append(
            f"{r['filing_date'].date()} | {r.get('transaction_type','N/A')} | Shares: {r.get('shares','N/A')}"
        )
    return "\n".join(lines)

# =========================================================
# FEAR & GREED (MARKET)
# =========================================================
if not fear_greed_df.empty:
    fg = fear_greed_df.copy()
    fg["date"] = pd.to_datetime(fg["date"], errors="coerce")
    fg_row = fg.loc[fg["date"].idxmax()]
    fg_text = f"\n### 📈 Fear & Greed Index: {fg_row.get('fear_and_greed','N/A')}"
else:
    fg_text = "\n### 📈 Fear & Greed Index: N/A"

# =========================================================
# COMBINE EVERYTHING
# =========================================================
def build_full_snapshot():
    # Full multi-line analysis instructions
    analysis_instructions = f"""
========================
ANALYSIS INSTRUCTIONS
========================
• All data comes from simplywallstreet, FINBERT sentiment analysis, or fear & greed index
• Evaluate all statistics I have provided you in way a professional analyst would, putting adequate weight on certain measurements over others or combining certain statistics together to get a better picture
• Focus on giving a more in depth statistical analysis first
• Analyze ownership composition and implications for control & liquidity
• Review company holders & insider activity for confidence/skepticism
• Consider macroeconomic context (rates, bonds, VIX index, inflation, federal reserve, equity market trends, global liquidity) in a few sentences.
• Highlight risks and sensitivities and factor in macro indicators such as the fear and greed index
• Provide a professional, conservative, institutional analyst opinion that offers a 1, 6, 12 month forecast in bull, neutral and bear circumstances and what those circumstances might be
• What price range is a good entry point? Factor in dividend payments.
• Talk about current news regarding the stock that could contribute in a good or bad way.
Stock analyzed: {selected_ticker} — {selected_company}
"""

    # Combine all sections
    snapshot_parts = [
        build_stock_snapshot(stock_df, selected_ticker),
        fg_text,
        build_sw_snapshot(sw_facts_df, selected_ticker),
        build_ownership_snapshot(ownership_df, selected_ticker),
        build_company_holders_snapshot(company_info_df, selected_ticker),
        build_insider_snapshot(insider_df, selected_ticker),
        analysis_instructions  # append at the end
    ]
    return "\n".join(snapshot_parts)

# =========================================================
# DISPLAY
# =========================================================
full_snapshot_text = build_full_snapshot()

st.markdown(
    """
    <div style="text-align:center; font-weight:bold; font-size:20px; margin-bottom:10px;">
        📊 Full Stock Snapshot & Analyst Prompt
    </div>
    """,
    unsafe_allow_html=True
)

st.text_area(
    "",
    full_snapshot_text,
    height=200,
    key=f"snapshot_{selected_ticker}"
)
