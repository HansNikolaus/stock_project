import pandas as pd
import yfinance as yf
import datetime
import numpy as np
import logging
import time
import random
import traceback
import pyodbc
import math
from concurrent.futures import ThreadPoolExecutor

# --- Logging setup ---
logging.basicConfig(
    filename="error_log.txt",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# --- Load tickers ---
try:
    tickers_df = pd.read_csv("snowflake_chart.csv")
    tickers = tickers_df["tickers"].dropna().unique().tolist()
    print(f"✅ Loaded {len(tickers)} tickers from CSV.")
except Exception:
    logger.exception("Failed to load snowflake_chart.csv")
    raise SystemExit("❌ Could not read tickers CSV")

# --- Helper functions ---
def calculate_rsi(data, period=14):
    delta = data["Close"].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, min_periods=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_sma(data, period):
    return data["Close"].rolling(period).mean()

def calculate_std(data, period):
    return data["Close"].rolling(period).std()

def timestamp():
    return datetime.datetime.now().strftime("[%H:%M:%S]")

def safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return float(x)
    except:
        return None

# --- SQL Server connection params ---
DB_PARAMS = {
    "DRIVER": "{ODBC Driver 18 for SQL Server}",
    "SERVER": "localhost",
    "DATABASE": "stock_project",
    "Trusted_Connection": "yes",
    "TrustServerCertificate": "yes"
}

def get_connection():
    conn_str = ";".join(f"{k}={v}" for k, v in DB_PARAMS.items())
    return pyodbc.connect(conn_str)

start_date = "2025-11-10"
end_date = datetime.date.today().strftime("%Y-%m-%d")

# --- Commodity tickers ---
commodity_tickers = ["GC=F", "CL=F", "HG=F", "SI=F", "BTC-USD", "ETH-USD", "ALI=F", "ZW=F", "ZC=F"]

# --- SQL templates ---
INSERT_SQL = """
INSERT INTO stock_data 
(tickers, trade_date, open_price, high_price, low_price, close_price,
 volume, dividend, split, rsi_5, rsi_14, rsi_30, rsi_50,
 sma_10, sma_50, sma_200,
 std_dev_10, std_dev_20, std_dev_100)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_TICKER_SQL = """
MERGE INTO tickers AS target
USING (SELECT ? AS ticker, ? AS name, ? AS instrument, ? AS sector, ? AS industry, ? AS country, ? AS description) AS source
ON target.ticker = source.ticker
WHEN MATCHED THEN 
    UPDATE SET 
        name = source.name,
        instrument = source.instrument,
        sector = source.sector,
        industry = source.industry,
        country = source.country,
        description = source.description
WHEN NOT MATCHED THEN
    INSERT (ticker, name, instrument, sector, industry, country, description)
    VALUES (source.ticker, source.name, source.instrument, source.sector, source.industry, source.country, source.description);
"""

# --- Main ticker processing with retries ---
MAX_RETRIES = 3  # max attempts per ticker
RETRY_DELAY = 5  # seconds between retries

def process_ticker_with_retry(ticker):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return process_ticker(ticker)
        except Exception as e:
            msg = f"{timestamp()} ⚠️ Attempt {attempt} failed for {ticker}: {e}"
            print(msg)
            logger.warning(f"{msg}\n{traceback.format_exc()}")
            time.sleep(RETRY_DELAY)
    print(f"{timestamp()} ❌ Permanent failure for {ticker} after {MAX_RETRIES} attempts")
    return 0

# --- Process single ticker ---
def process_ticker(ticker):
    conn = get_connection()
    cursor = conn.cursor()
    success_rows = 0

    try:
        print(f"\n{timestamp()} 🔄 Processing {ticker}...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        info = stock.info or {}

        time.sleep(random.uniform(3.0, 10.0))  # rate limit

        if hist.empty or hist["Close"].dropna().empty:
            msg = f"{timestamp()} ⚠️ No valid data for {ticker}"
            print(msg)
            logger.warning(msg)
            return 0

        # --- Dividends ---
        try:
            dividends = stock.dividends
            hist["dividend"] = dividends.reindex(hist.index, fill_value=0.0) if not dividends.empty else 0.0
        except Exception:
            hist["dividend"] = 0.0

        # --- Splits ---
        try:
            splits_series = stock.splits
            hist["split"] = splits_series.reindex(hist.index, fill_value=np.nan) if not splits_series.empty else np.nan
        except Exception:
            hist["split"] = np.nan

        # --- Reset index and keep date only ---
        hist = hist.reset_index().rename(columns={"Date": "trade_date"})
        hist["trade_date"] = hist["trade_date"].dt.date

        # --- Indicators ---
        hist_indexed = hist.set_index(pd.DatetimeIndex(hist["trade_date"]))
        for p in [5, 14, 30, 50]:
            hist[f"rsi_{p}"] = calculate_rsi(hist_indexed, p).values
        for p, sma in [(10, "sma_10"), (50, "sma_50"), (200, "sma_200")]:
            hist[sma] = calculate_sma(hist_indexed, p).values
        for p, std in [(10, "std_dev_10"), (20, "std_dev_20"), (100, "std_dev_100")]:
            hist[std] = calculate_std(hist_indexed, p).values
        hist.replace([np.inf, -np.inf], np.nan, inplace=True)

        # --- Prepare batch insert for stock_data ---
        rows_to_insert = []
        for _, row in hist.iterrows():
            rows_to_insert.append((
                ticker,
                row["trade_date"],
                safe_float(row.get("Open")),
                safe_float(row.get("High")),
                safe_float(row.get("Low")),
                safe_float(row.get("Close")),
                int(row.get("Volume")) if row.get("Volume") not in [None, ""] else None,
                safe_float(row.get("dividend")),
                safe_float(row.get("split")),
                safe_float(row.get("rsi_5")),
                safe_float(row.get("rsi_14")),
                safe_float(row.get("rsi_30")),
                safe_float(row.get("rsi_50")),
                safe_float(row.get("sma_10")),
                safe_float(row.get("sma_50")),
                safe_float(row.get("sma_200")),
                safe_float(row.get("std_dev_10")),
                safe_float(row.get("std_dev_20")),
                safe_float(row.get("std_dev_100")),
            ))

        # --- Insert all rows into stock_data ---
        try:
            cursor.executemany(INSERT_SQL, rows_to_insert)
            conn.commit()
            success_rows = len(rows_to_insert)
            print(f"{timestamp()} ✅ Wrote {success_rows:,} rows for {ticker}")

        except pyodbc.IntegrityError:
            conn.rollback()
            print(f"{timestamp()} ⚠️ Batch insert failed — retrying row-by-row for {ticker}")

            for row in rows_to_insert:
                try:
                    cursor.execute(INSERT_SQL, row)
                    success_rows += 1
                except pyodbc.IntegrityError:
                    # duplicate — safe to ignore
                    continue

            conn.commit()
            print(f"{timestamp()} ✅ Wrote {success_rows:,} rows for {ticker} (duplicates skipped)")

    except Exception as e:
        msg = f"{timestamp()} ❌ Failed to process {ticker}: {e}"
        print(msg)
        logger.error(f"{msg}\n{traceback.format_exc()}")
    finally:
        cursor.close()
        conn.close()

    return success_rows

# --- Parallel processing ---
success_count = 0
MAX_WORKERS = 5
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    results = list(executor.map(process_ticker_with_retry, tickers))
    success_count = sum(results)

print(f"\n🎯 Done! Total rows inserted: {success_count}")
print("📘 Any errors logged to: error_log.txt")
