import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from openai import OpenAI
import pkg_resources
from datetime import datetime
import os
from packaging import version

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Stock Dashboard", page_icon="📈")

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
    st.warning(f"⚠️ OpenAI version {installed_version} is outdated. Please upgrade to ≥ {required_version}. Try: pip install --upgrade openai")

# --- OpenAI Client ---
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- Load and cache data ---
@st.cache_data
def load_data():
    tickers = pd.read_csv("tickers.csv")
    stock = pd.read_csv("stock_data.csv", parse_dates=["date"])
    analyst = pd.read_csv("analyst_summary.csv")
    snowflake = pd.read_csv("snowflake_chart.csv")
    return tickers, stock, analyst, snowflake

tickers_df, stock_df, analyst_df, snowflake_df = load_data()

# --- Ticker selection ---
with st.sidebar:
    selected_ticker = st.selectbox("", sorted(tickers_df["tickers"].unique()))

info = tickers_df[tickers_df["tickers"] == selected_ticker].squeeze()
instrument_type = info.get("financial_instrument", "").upper()
price_data = stock_df[stock_df["ticker"] == selected_ticker].sort_values("date")
latest = price_data.iloc[-1]

# --- Sidebar Statistics Section ---
with st.sidebar.expander("📊 Statistics", expanded=False):
    def styled_header(title, tooltip):
        return f"""
        <div style='background-color:#000000;padding:8px 12px;border-radius:6px;'>
            <span style='color:#ffffff;font-weight:bold;text-decoration:underline;' title='{tooltip}'>{title} 🛈</span>
        </div>
        """

    if instrument_type not in ["FUTURE", "INDEX"]:
        st.markdown(styled_header("Valuation Layer", "These metrics assess how the stock is priced relative to company fundamentals like earnings, book value, and revenue."), unsafe_allow_html=True)
        st.markdown(f"PE Ratio: <span title='Shows how much investors are paying for each unit of earnings; low values may indicate undervaluation.'><strong>{latest['pe_ratio']:.2f}</strong> 🛈</span>", unsafe_allow_html=True)
        st.markdown(f"PB Ratio: <span title='Compares market value to book value; indicates how the market values the company’s net assets.'><strong>{latest['pb_ratio']:.2f}</strong> 🛈</span>", unsafe_allow_html=True)
        st.markdown(f"PS Ratio: <span title='Measures how much investors are willing to pay per unit of revenue.'><strong>{latest['ps_ratio']:.2f}</strong> 🛈</span>", unsafe_allow_html=True)

        st.markdown(styled_header("Profitability Anchor", "Profitability metrics show how effectively the company turns revenue into profit and creates shareholder value."), unsafe_allow_html=True)
        st.markdown(f"EPS: <span title='Earnings Per Share shows how much net income is allocated to each share.'><strong>{latest.get('eps', 0):.2f}</strong> 🛈</span>", unsafe_allow_html=True)
        st.markdown(f"Book Value/Share: <span title='Represents the total equity value per share if the company were liquidated.'><strong>{latest.get('bvs', 0):.2f}</strong> 🛈</span>", unsafe_allow_html=True)

    st.markdown(styled_header("Market Pulse", "These indicators measure price momentum and trend behavior, providing clues about investor sentiment."), unsafe_allow_html=True)
    st.markdown(f"Relative Strength Index: <span title='Momentum indicator suggesting if a stock is overbought or oversold.'><strong>{latest.get('rsi', 0):.2f}</strong> 🛈</span>", unsafe_allow_html=True)
    st.markdown(f"SMA 30 Day: <span title='Simple Moving Average over 30 days helps smooth short-term price fluctuations.'><strong>{latest.get('sma_30', 0):.2f}</strong> 🛈</span>", unsafe_allow_html=True)

    st.markdown(styled_header("Volatility Check", "Volatility gauges the magnitude of price changes—higher values signal more risk but also more opportunity."), unsafe_allow_html=True)
    st.markdown(f"Std. Dev 30 Day: <span title='Standard deviation of closing prices over 30 days; reflects price stability.'><strong>{latest.get('sd_30', 0):.2f}</strong> 🛈</span>", unsafe_allow_html=True)

# --- Sidebar Company Description ---
if instrument_type not in ["FUTURE", "INDEX"]:
    with st.sidebar.expander("📘 Company Description", expanded=False):
        st.markdown(f"""
            <div title='Summary of what this company does and where it’s based.'>
                <strong>Headquarter Location:</strong> {info['country']}  
                <br><br>
                {info['description']}
            </div>
        """, unsafe_allow_html=True)

# --- Snowflake Fallback ---
if selected_ticker in snowflake_df["tickers"].values:
    snow = snowflake_df[snowflake_df["tickers"] == selected_ticker].squeeze()
else:
    snow = pd.Series({k: 0 for k in ["value", "future", "past", "health", "dividend"]})

# --- Header ---
st.markdown(f"<div class='header-text'>{info['name']} ({selected_ticker})</div>", unsafe_allow_html=True)

# --- Info and Metrics Display ---
def colorize(value):
    color = "red" if str(value).startswith("-") else "black"
    return f"<span class='info-value' style='color:{color}'>{value}</span>"

colL, colR = st.columns([3, 2])
recent_close = latest['close']
past_week = price_data.iloc[-7]['close'] if len(price_data) >= 7 else price_data.iloc[0]['close']
past_year = price_data.iloc[-252]['close'] if len(price_data) >= 252 else price_data.iloc[0]['close']

change_7d = ((recent_close - past_week) / past_week) * 100
change_1y = ((recent_close - past_year) / past_year) * 100

color_7d = "green" if change_7d >= 0 else "red"
color_1y = "green" if change_1y >= 0 else "red"

with colL:
    if instrument_type not in ["FUTURE", "INDEX"]:
        st.markdown(f"<div class='sector-text'>{info['sector']} – {info['industry']}</div>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown(f"<span class='info-label'>Market Cap: </span><span class='info-value'>{latest['market_cap']}</span>", unsafe_allow_html=True)
        st.markdown(f"<span class='info-label'>Net Income: </span>{colorize(latest['net_income'])}", unsafe_allow_html=True)
        st.markdown(f"<span class='info-label'>Total Revenue: </span>{colorize(latest['total_revenue'])}", unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)

    st.markdown(f"""
        <div class='inline-metrics' style='margin-top: 5px'>
            <span class='info-value'>Recent Close: {recent_close:.2f}</span>
            <span class='change-text' style='color:{color_7d}'>(7D {change_7d:.1f}%)</span>
            <span class='change-text' style='color:{color_1y}'>(1Y {change_1y:.1f}%)</span>
        </div>
    """, unsafe_allow_html=True)

    if instrument_type not in ["FUTURE", "INDEX"]:
        dividends = price_data["dividend"].fillna(0)
        recent_dividends = dividends[dividends > 0].tail(4)
        if not recent_dividends.empty:
            div_sum = recent_dividends.sum()
            div_yield = (div_sum / recent_close) * 100 if recent_close else 0
        else:
            div_yield = 0

        st.markdown(f"<div class='sector-text'>4Q Dividend Yield: {div_yield:.2f}%</div>", unsafe_allow_html=True)


with colR:
    def build_snowflake_chart(data, label):
        axes = ["Value", "Future", "Past", "Health", "Dividend"]
        values = [int(round(data[a.lower()])) for a in axes]
        theta = axes + [axes[0]]
        r = values + [values[0]]

        hover_descriptions = {
            "Value": "Is the company undervalued compared to peers and cashflows?",
                    "Future": "Forecasted performance in 1–3 years?",
        "Past": "Performance over the last 5 years?",
        "Health": "Financial health and debt levels?",
        "Dividend": "Dividend quality and reliability?"
        }

        hover_text = [
            f"<span style='font-size:13px'><b>{a}</b>: {v}/6<br>{hover_descriptions[a]}</span>"
            for a, v in zip(axes, values)
        ]
        hover_text.append(hover_text[0])

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=r,
            theta=theta,
            fill='toself',
            name=label,
            line=dict(color="#00ccff", width=4),
            marker=dict(size=6, color="#00ccff"),
            hoverinfo="text",
            hovertext=hover_text
        ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 6], tickfont=dict(size=11)),
                angularaxis=dict(
                    tickmode="array",
                    tickvals=axes,
                    ticktext=axes,
                    tickfont=dict(size=13, color="black"),
                    direction="clockwise"
                )
            ),
            showlegend=False,
            template="plotly_dark",
            margin=dict(t=1, b=20, l=55, r=35),
            width=400,
            height=360
        )
        return fig

    # --- Only show snowflake chart for stocks, not INDEX or FUTURE ---
    if instrument_type not in ["FUTURE", "INDEX"]:
        fig = build_snowflake_chart(snow, selected_ticker)
        st.plotly_chart(fig, use_container_width=False)
    else:
        st.info("Snowflake chart is not available for this financial instrument.")

# --- AI Commentary ---
prev30 = price_data.iloc[-30] if len(price_data) >= 30 else price_data.iloc[0]
change_pct = ((latest["close"] - prev30["close"]) / prev30["close"]) * 100
rsi = latest.get("rsi", "N/A")
std_dev = latest.get("sd_30", "N/A")
sma30 = latest.get("sma_30", "N/A")

# Determine prompt based on instrument type
if instrument_type in ["INDEX", "FUTURE"]:
    type_label = "index" if instrument_type == "INDEX" else "commodity future"
    prompt = (
        f"Write a concise 3–5 sentence market commentary in English about the {type_label} {info['name']} ({selected_ticker}). "
        f"Over the past 30 days, the price changed by {change_pct:.2f}%. RSI is {rsi}, standard deviation over 30 days is {std_dev}, "
        f"and the 30-day simple moving average is {sma30}. Focus only on momentum, price trend, and short-term volatility. "
        "Do not mention valuation ratios, fundamentals, earnings, or book value — treat this as a technical market update."
    )
else:
    prompt = (
        f"Write a 3–5 sentence market commentary in English for the stock {info['name']} ({selected_ticker}). "
        f"In the past 30 days, the stock price changed by {change_pct:.2f}%. RSI is {rsi}, standard deviation is {std_dev}, "
        f"and the 30-day simple moving average is {sma30}. You may comment on short-term momentum, volatility, valuation metrics"
        "or fundamental signals if appropriate — frame this for investors assessing the stock's recent performance."
    )

try:
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    summary = response.choices[0].message.content.strip()
    st.markdown(f"<div class='description-box'><b>🧠 AI Commentary:</b><br>{summary}</div>", unsafe_allow_html=True)
except Exception as e:
    st.warning(f"Could not generate commentary. Error: {e}")

# Line break and title above the chart
st.markdown("<br>", unsafe_allow_html=True)

# --- Analyst Summary Centered Subheader Info ---
if instrument_type not in ["FUTURE", "INDEX"]:
    if selected_ticker in analyst_df["ticker"].values:
        analyst_row = analyst_df[analyst_df["ticker"] == selected_ticker].squeeze()

        # Display and color mappings
        rec_display_map = {
            "strong_buy": "Strong Buy",
            "buy": "Buy",
            "hold": "Hold",
            "underperform": "Underperform",
            "sell": "Sell",
            "none": "Nothing"
        }

        rec_color_map = {
            "strong_buy": "darkgreen",
            "buy": "limegreen",
            "hold": "gold",
            "underperform": "orange",
            "sell": "red",
            "none": "black"
        }

        raw_rec = analyst_row["recommendation"].lower()
        rec_display = rec_display_map.get(raw_rec, raw_rec.title())
        rec_color = rec_color_map.get(raw_rec, "black")
        num_analysts = int(round(analyst_row["number_of_analysts"]))

        st.markdown(f"""
            <div style='text-align: center; font-size:30px; font-weight:bold; color:black;'>
                {num_analysts} Analysts' Overall Recommendation: <span style='color:{rec_color};'>{rec_display}</span>
            </div>
        """, unsafe_allow_html=True)

        # Target price points
        low = analyst_row["target_price_low"]
        high = analyst_row["target_price_high"]
        avg = analyst_row["target_price_avg"]
        recent_close = latest["close"]

        price_points = {
            "lowest_estimate": low,
            "avg_estimate": avg,
            "highest_estimate": high,
            "current_price": recent_close
        }

        sorted_points = sorted(price_points.items(), key=lambda x: x[1])
        x_vals = [p[1] for p in sorted_points]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=[1]*len(x_vals),
            mode="lines",
            line=dict(color="black", width=5),
            hoverinfo="skip",
            showlegend=False
        ))

        for label, value in sorted_points:
            is_current = label == "current_price"
            fig.add_trace(go.Scatter(
                x=[value],
                y=[1],
                mode="markers+text",
                marker=dict(
                    size=14 if is_current else 11,
                    color="#00ccff" if is_current else "black"
                ),
                text=[f"<b>${value:.2f}</b>" if is_current else f"${value:.2f}"],
                textposition="top center" if is_current else "bottom center",
                textfont=dict(size=22),
                hovertext=label.replace("_", " ").title(),
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
        st.info("No analyst data available for this ticker.")

# --- Time Series Chart ---

# Mapping of internal metric keys to user-friendly labels
metric_label_map = {
    "open": "Open",
    "close": "Close",
    "high": "High",
    "low": "Low",
    "pe_ratio": "PE Ratio",
    "pb_ratio": "PB Ratio",
    "ps_ratio": "PS Ratio",
    "rsi": "Relative Strength Index",
    "sma_30": "Simple Moving Average - 30 Day",
    "sd_30": "Standard Deviation - 30 Day",
    "bvs": "Book Value per Share",
    "eps": "Earning per Share"
}

# Reverse the mapping for lookup after selection
label_to_metric_map = {v: k for k, v in metric_label_map.items()}

# Create multiselect with display labels
selected_labels = st.multiselect(
    "Select up to 3 metrics:",
    options=list(metric_label_map.values()),
    default=[metric_label_map["close"]]
)

if len(selected_labels) > 3:
    st.error("Please select no more than 3 metrics.")
else:
    selected_metrics = [label_to_metric_map[label] for label in selected_labels]

    min_date = price_data["date"].min()
    max_date = price_data["date"].max()
    col_start, col_end = st.columns(2)
    with col_start:
        start_date = st.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
    with col_end:
        end_date = st.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)

    mask = (price_data["date"] >= pd.to_datetime(start_date)) & (price_data["date"] <= pd.to_datetime(end_date))
    filtered = price_data.loc[mask].set_index("date")

    if not filtered.empty:
        # Rename columns to display labels for chart display
        chart_df = filtered[selected_metrics].copy()
        chart_df.rename(columns=metric_label_map, inplace=True)
        st.line_chart(chart_df)

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