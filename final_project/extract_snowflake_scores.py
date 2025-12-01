import os
import re
import time
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# File paths
csv_file = "snowflake_chart.csv"
html_folder = "html_dump"
output_csv = "snowflake_chart_updated.csv"

# Regex pattern to find the score JSON and capture the five values
pattern = re.compile(
    r'"score":\{"dividend":(\d+),"future":(\d+),"health":(\d+),"past":(\d+),"value":(\d+)\}'
)

# Load the CSV
df = pd.read_csv(csv_file)

# Ensure snowflake columns exist
for col in ["value", "future", "past", "health", "dividend"]:
    if col not in df.columns:
        df[col] = pd.NA

# Add date column if missing
if "date" not in df.columns:
    df["date"] = pd.NaT

total_tickers = len(df)
print(f"Starting extraction for {total_tickers} tickers...\n")

# -------- Progress bar helper --------
def print_progress(done, total, start_time):
    percent = (done / total) * 100
    elapsed = time.time() - start_time
    rate = done / elapsed if elapsed > 0 else 0
    remaining = (total - done) / rate if rate > 0 else 0

    bar_len = 40
    filled_len = int(bar_len * percent / 100)
    bar = "█" * filled_len + "-" * (bar_len - filled_len)

    print(
        f"\r[{bar}] {percent:5.1f}% "
        f"({done}/{total}) "
        f"Elapsed: {elapsed:5.1f}s  ETA: {remaining:5.1f}s",
        end="",
        flush=True
    )

# --- Function to process a single ticker ---
def process_ticker(idx, ticker):
    html_path = os.path.join(html_folder, f"{ticker}.html")
    result = {
        "idx": idx, "ticker": ticker,
        "value": pd.NA, "future": pd.NA, "past": pd.NA,
        "health": pd.NA, "dividend": pd.NA, "date": pd.NaT
    }

    if not os.path.isfile(html_path):
        return result

    # Get last modified date
    try:
        modified_ts = os.path.getmtime(html_path)
        result["date"] = datetime.fromtimestamp(modified_ts).date()
    except Exception:
        pass

    # Read HTML and extract scores
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        match = pattern.search(html_content)
        if match:
            dividend, future, health, past, value = match.groups()
            result.update({
                "value": int(value),
                "future": int(future),
                "past": int(past),
                "health": int(health),
                "dividend": int(dividend),
            })
    except Exception:
        pass

    return result


# --- Run all tickers in parallel ---
results = []
max_workers = min(32, os.cpu_count() * 2)

start_time = time.time()
completed = 0

print(f"Using {max_workers} workers...\n")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_ticker, idx, ticker)
               for idx, ticker in enumerate(df["tickers"])]

    for future in as_completed(futures):
        results.append(future.result())
        completed += 1
        print_progress(completed, total_tickers, start_time)

# Final newline after progress bar
print("\n")


# --- Update dataframe with results ---
for res in results:
    idx = res["idx"]
    df.loc[idx, ["value","future","past","health","dividend","date"]] = \
        res["value"], res["future"], res["past"], res["health"], res["dividend"], res["date"]

# Ensure date column is proper date type
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

# Save updated CSV
df.to_csv(output_csv, index=False)
print(f"\nUpdated CSV saved to {output_csv}")
print("All done! 🎉")
