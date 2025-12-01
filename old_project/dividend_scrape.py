import os
import json
import time
import pyodbc
import requests
from pathlib import Path
from datetime import datetime

# === Configuration ===
CRUMB = "EMv2l/jFVvK"  # Update if crumb expires
COOKIE = "A1=d=AQABBO9ctWUCEBx3Wm1V0sPXx-nrJiijmiEFEgABCAF3UGh3aPU70CMA9qMCAAcI71y1ZSijmiE&S=AQAAAjEv0aYtpX3o0Q5AOnrmDZ8; A3=d=AQABBO9ctWUCEBx3Wm1V0sPXx-nrJiijmiEFEgABCAF3UGh3aPU70CMA9qMCAAcI71y1ZSijmiE&S=AQAAAjEv0aYtpX3o0Q5AOnrmDZ8;"  # Replace with full, valid cookie string
HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "cookie": COOKIE,
    "accept": "*/*",
    "referer": "https://finance.yahoo.com/quote/AAPL/"
}
YAHOO_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=summaryDetail,calendarEvents&crumb={crumb}"

SQL_CONN = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=hans.database.windows.net;"
    "DATABASE=stock_project;"
    "UID=hans_student;"
    "PWD=LeoNiklas!88"
)
CURSOR = SQL_CONN.cursor()

JSON_DIR = Path(__file__).parent / "json_data"
JSON_DIR.mkdir(exist_ok=True)
TODAY = datetime.today().date()

def format_ts(ts):
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else None
    except:
        return None

def upsert_event(symbol, date_str, event_type, amount):
    if not date_str:
        return
    CURSOR.execute("""
        MERGE stock_events AS target
        USING (SELECT ? AS tickers, ? AS event_date, ? AS event_type) AS source
        ON target.tickers = source.tickers AND target.event_date = source.event_date AND target.event_type = source.event_type
        WHEN MATCHED THEN UPDATE SET amount = ?, last_updated = GETDATE()
        WHEN NOT MATCHED THEN INSERT (tickers, event_date, event_type, amount, last_updated)
        VALUES (?, ?, ?, ?, GETDATE());
    """, (symbol, date_str, event_type, amount, symbol, date_str, event_type, amount))

def save_quote_summary(symbol):
    url = YAHOO_URL.format(symbol=symbol, crumb=CRUMB)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            file_path = JSON_DIR / f"{symbol}_quotesummary.json"
            file_path.write_text(response.text, encoding="utf-8")
            print(f"📥 Saved: {symbol}")
        else:
            print(f"❌ {symbol}: HTTP {response.status_code}")
    except Exception as e:
        print(f"⚠️ {symbol}: Fetch failed — {e}")

def extract_and_store(file_path):
    symbol = file_path.stem.split("_")[0]
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        result = data.get("quoteSummary", {}).get("result", [{}])[0]
        if not result:
            print(f"⚠️ {symbol}: No result data")
            return

        sd = result.get("summaryDetail", {})
        cal = result.get("calendarEvents", {})
        earnings = cal.get("earnings", {})

        # Dividend fields
        ex_div = format_ts(sd.get("exDividendDate", {}).get("raw"))
        div_date = format_ts(cal.get("dividendDate", {}).get("raw"))
        div_yield = sd.get("dividendYield", {}).get("raw")
        forward_rate = sd.get("forwardDividendRate", {}).get("raw")
        dividend_rate = sd.get("dividendRate", {}).get("raw")
        trailing_rate = sd.get("trailingAnnualDividendRate", {}).get("raw")
        amount = forward_rate or dividend_rate or trailing_rate

        if ex_div and datetime.strptime(ex_div, "%Y-%m-%d").date() >= TODAY:
            upsert_event(symbol, ex_div, "ex_dividend", amount)

        if div_date and datetime.strptime(div_date, "%Y-%m-%d").date() >= TODAY:
            upsert_event(symbol, div_date, "dividend", amount)

        if div_yield:
            upsert_event(symbol, ex_div or str(TODAY), "dividend_yield", div_yield)

        # Earnings
        earnings_avg = earnings.get("earningsAverage", {}).get("raw")
        for ed in earnings.get("earningsDate", []):
            edate = format_ts(ed.get("raw"))
            if edate:
                upsert_event(symbol, edate, "earnings_report", earnings_avg)

        for call in earnings.get("earningsCallDate", []):
            cdate = format_ts(call.get("raw"))
            if cdate:
                upsert_event(symbol, cdate, "earnings_call", None)

        print(f"✅ Parsed: {symbol}")

    except Exception as e:
        print(f"❌ {symbol}: Parsing error — {e}")

def main():
    # Step 1: Load tickers from DB
    CURSOR.execute("SELECT tickers FROM tickers")
    tickers = [row[0] for row in CURSOR.fetchall() if not row[0].startswith("^")]

    # Step 2: Download quoteSummaries
    print("📡 Fetching quoteSummary data...")
    for symbol in tickers:
        save_quote_summary(symbol)
        time.sleep(1.5)

    # Step 3: Parse JSONs and populate DB
    print("\n🧠 Parsing saved JSONs into stock_events...")
    for json_file in JSON_DIR.glob("*_quotesummary.json"):
        extract_and_store(json_file)

    SQL_CONN.commit()
    CURSOR.close()
    SQL_CONN.close()
    print("\n🎯 Done. All events updated.")

if __name__ == "__main__":
    main()