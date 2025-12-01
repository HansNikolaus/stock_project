import yfinance as yf
import pandas as pd
import time
import logging

# --- Load tickers from tickers.csv ---
try:
    df = pd.read_csv("tickers.csv")
    tickers = df["tickers"].dropna().unique().tolist()
except Exception as e:
    print(f"❌ Failed to load tickers.csv: {e}")
    exit()

results = []

# --- Loop through each ticker ---
for ticker in tickers:
    print(f"📈 Processing {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        results.append({
            "ticker": ticker,
            "recommendation": info.get("recommendationKey", "N/A"),
            "number_of_analysts": info.get("numberOfAnalystOpinions", "N/A"),
            "target_price_avg": info.get("targetMeanPrice", "N/A"),
            "target_price_low": info.get("targetLowPrice", "N/A"),
            "target_price_high": info.get("targetHighPrice", "N/A")
        })

    except Exception as e:
        print(f"⚠️ Failed for {ticker}: {e}")

    time.sleep(0.5)  # polite delay

# --- Save results ---
output_df = pd.DataFrame(results)
output_df.to_csv("analyst_summary.csv", index=False)
print(f"\n✅ analyst_summary.csv created with {len(results)} entries.")
