import pyodbc
import yfinance as yf
import time
import random
from datetime import datetime
import logging
import pandas as pd

# -------------------------------------------------------
# Logging setup
# -------------------------------------------------------
logging.basicConfig(
    filename="tickers.csv",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

def timestamp():
    return datetime.now().strftime("[%H:%M:%S]")

# -------------------------------------------------------
# SQL Server connection params (Windows auth)
# -------------------------------------------------------
DB_PARAMS = {
    "DRIVER": "{ODBC Driver 18 for SQL Server}",
    "SERVER": "localhost",       # adjust if needed
    "DATABASE": "stock_project",
    "Trusted_Connection": "yes",
    "TrustServerCertificate": "yes"
}

def get_connection():
    conn_str = ";".join(f"{k}={v}" for k, v in DB_PARAMS.items())
    return pyodbc.connect(conn_str)

# -------------------------------------------------------
# Corrected MERGE SQL
# -------------------------------------------------------
INSERT_TICKER_SQL = """
MERGE INTO tickers AS target
USING (
    SELECT 
        ? AS tickers, 
        ? AS names, 
        ? AS financial_instrument, 
        ? AS sector, 
        ? AS industry, 
        ? AS country, 
        ? AS descriptions
) AS source
ON target.tickers = source.tickers
WHEN MATCHED THEN 
    UPDATE SET 
        names = source.names,
        financial_instrument = source.financial_instrument,
        sector = source.sector,
        industry = source.industry,
        country = source.country,
        descriptions = source.descriptions
WHEN NOT MATCHED THEN
    INSERT (tickers, names, financial_instrument, sector, industry, country, descriptions)
    VALUES (source.tickers, source.names, source.financial_instrument, source.sector, source.industry, source.country, source.descriptions);
"""

# -------------------------------------------------------
# Commodity tickers override
# -------------------------------------------------------
commodity_tickers = {"GC=F", "CL=F", "HG=F", "SI=F", "BTC-USD", "ETH-USD", "ALI=F", "ZW=F", "ZC=F"}

# -------------------------------------------------------
# Load tickers CSV
# -------------------------------------------------------
try:
    tickers_df = pd.read_csv("snowflake_chart.csv")
    tickers = tickers_df["tickers"].dropna().unique().tolist()
    print(f"✅ Loaded {len(tickers)} tickers from CSV.")
except Exception as e:
    logger.exception("Failed to load tickers CSV")
    raise SystemExit("❌ Could not read tickers CSV")

# -------------------------------------------------------
# Helper: fetch metadata
# -------------------------------------------------------
def fetch_metadata(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}
        name = info.get("longName", ticker)
        instrument = info.get("quoteType", "N/A")
        sector = info.get("sector", "N/A")
        industry = info.get("industry", "N/A")
        country = info.get("country", "N/A")
        description = info.get("longBusinessSummary", "N/A")

        if ticker in commodity_tickers:
            instrument = "Futures"
            sector = "Commodity"
            industry = "Commodity"

        if instrument.upper() == "INDEX":
            sector = industry = "Index"

        return (ticker, name, instrument, sector, industry, country, description)

    except Exception as e:
        logger.warning(f"{timestamp()} ❌ Failed to fetch metadata for {ticker}: {e}")
        return None

# -------------------------------------------------------
# Process all tickers
# -------------------------------------------------------
success_count = 0
fail_count = 0

conn = get_connection()
cursor = conn.cursor()

print("\n🚀 Starting metadata import...\n")

for ticker in tickers:
    data = fetch_metadata(ticker)
    if not data:
        fail_count += 1
        continue

    try:
        cursor.execute(INSERT_TICKER_SQL, data)
        conn.commit()
        success_count += 1
        print(f"{timestamp()} ✅ Saved metadata for {ticker}")
        logger.info(f"{timestamp()} ✅ Saved metadata for {ticker}")
    except Exception as e:
        fail_count += 1
        print(f"{timestamp()} ❌ Failed to save metadata for {ticker}: {e}")
        logger.warning(f"{timestamp()} ❌ Failed to save metadata for {ticker}: {e}")

    # gentle delay to avoid rate limits
    time.sleep(random.uniform(0.3, 0.8))

cursor.close()
conn.close()

print(f"\n🎯 Done! Successful: {success_count}, Failed: {fail_count}")
logger.info(f"Finished metadata import. Successful: {success_count}, Failed: {fail_count}")
