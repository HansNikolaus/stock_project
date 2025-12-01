import pandas as pd
import yfinance as yf
import datetime
import logging
import numpy as np

# --- Configure logging ---
logging.basicConfig(
    filename="error_log.txt",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.WARNING
)
logger = logging.getLogger()

# --- Load tickers from tickers.csv ---
try:
    tickers_df = pd.read_csv("tickers.csv")
    tickers = tickers_df["tickers"].dropna().unique().tolist()
except Exception as e:
    logger.error(f"Error reading tickers.csv: {e}")
    raise

# --- Set date range ---
start_date = "2024-05-01"
end_date = datetime.datetime.today().strftime("%Y-%m-%d")

# --- Indicator Functions ---
def calculate_rsi(data, period=14):
    delta = data["Close"].diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = -delta.where(delta < 0, 0).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_sma(data, period=30):
    return data["Close"].rolling(period).mean()

def calculate_std(data, period=30):
    return data["Close"].rolling(period).std()

# --- Formatters ---
def format_large_currency(value):
    try:
        value = float(value)
        sign = "-" if value < 0 else ""
        value = abs(value)
        if value >= 1_000_000_000_000:
            return f"{sign}${value / 1_000_000_000_000:.2f}T"
        elif value >= 1_000_000_000:
            return f"{sign}${value / 1_000_000_000:.2f}B"
        elif value >= 1_000_000:
            return f"{sign}${value / 1_000_000:.2f}M"
        elif value >= 1_000:
            return f"{sign}${value / 1_000:.2f}k"
        else:
            return f"{sign}${value:.2f}"
    except (ValueError, TypeError):
        return "N/A"

# --- Containers ---
stock_data = []
metadata = []

# --- Main loop ---
for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        hist = stock.history(start=start_date, end=end_date)

        if hist.empty:
            logger.warning(f"No historical price data for {ticker}")
            continue

        dividends = stock.dividends
        hist["dividend"] = dividends.reindex(hist.index, fill_value=0.0)
        hist["ticker"] = ticker
        hist["eps"] = round(info.get("trailingEps", float("nan")), 2)
        hist["bvs"] = round(info.get("bookValue", float("nan")), 2)
        hist["net_income"] = round(info.get("netIncomeToCommon", float("nan")), 2)
        hist["num_outstanding_shares"] = round(info.get("sharesOutstanding", float("nan")), 2)
        hist["total_revenue"] = float(info.get("totalRevenue", float("nan")))
        hist["market_cap"] = float(info.get("marketCap", float("nan")))
        hist["rsi"] = calculate_rsi(hist)
        hist["sma_30"] = calculate_sma(hist)
        hist["sd_30"] = calculate_std(hist)

        hist["pe_ratio"] = hist["Close"] / hist["eps"]
        hist["pb_ratio"] = hist["Close"] / hist["bvs"]
        hist["ps_ratio"] = hist["market_cap"] / hist["total_revenue"]

        stock_data.append(hist)

        instrument = info.get("quoteType", "N/A")
        if ticker == "GC=F":
            name = "Gold Price"
            sector = "Commodity"
        elif ticker == "CL=F":
            name = "Oil Price"
            sector = "Commodity"
        elif instrument == "INDEX":
            name = info.get("longName", "N/A")
            sector = "Index"
        else:
            name = info.get("longName", "N/A")
            sector = info.get("sector", "N/A")

        metadata.append({
            "tickers": ticker,
            "name": name,
            "financial_instrument": instrument,
            "sector": sector,
            "industry": info.get("industry", "N/A"),
            "country": info.get("country", "N/A"),
            "description": info.get("longBusinessSummary", "N/A")
        })

    except Exception as e:
        logger.error(f"Error processing {ticker}: {e}")

# --- Save stock_data.csv ---
if stock_data:
    df = pd.concat(stock_data).reset_index()
    df = df.rename(columns={
        "Date": "date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume"
    })

    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(None).dt.date
    df = df[df["date"] >= datetime.date(2024, 6, 20)]

    round_cols = ["open", "high", "low", "close", "sma_30",
                  "pe_ratio", "pb_ratio", "ps_ratio", "eps", "net_income",
                  "num_outstanding_shares", "bvs", "rsi", "sd_30"]
    for col in round_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df["market_cap"] = df["market_cap"].apply(format_large_currency)
    df["total_revenue"] = df["total_revenue"].apply(format_large_currency)
    df["net_income"] = df["net_income"].apply(format_large_currency)
    df["num_outstanding_shares"] = df["num_outstanding_shares"].apply(format_large_currency)

    df = df[[
        "date", "ticker", "open", "high", "low", "close", "volume",
        "dividend", "pe_ratio", "pb_ratio", "ps_ratio",
        "eps", "net_income", "num_outstanding_shares", "bvs",
        "total_revenue", "market_cap", "rsi", "sma_30", "sd_30"
    ]]
    df.to_csv("stock_data.csv", index=False)
    print("✅ stock_data.csv saved with calculated ratios and proper formatting.")

# --- Save updated tickers.csv ---
if metadata:
    meta_df = pd.DataFrame(metadata)
    meta_df.to_csv("tickers.csv", index=False)
    print("✅ tickers.csv updated with INDEX-sector handling.")
