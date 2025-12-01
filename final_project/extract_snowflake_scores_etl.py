import pandas as pd
import pyodbc
import time
from datetime import datetime

# ---- Start timer ----
start_time = time.time()

# ---- Load CSV ----
df = pd.read_csv("snowflake_chart_updated.csv")
print(f"✅ Loaded {len(df)} rows from CSV at {datetime.now().strftime('%H:%M:%S')}")

# ---- Ensure proper date format ----
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
if df["date"].isna().any():
    print(f"⚠️ Found {df['date'].isna().sum()} rows with invalid dates. They will be skipped.")

# ---- SQL Connection ----
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=stock_project;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print(f"✅ Connected to SQL Server at {datetime.now().strftime('%H:%M:%S')}\n")
except pyodbc.Error as e:
    raise SystemExit(f"❌ Could not connect to SQL Server: {e}")

# ---- Insert SQL ----
insert_sql = """
    INSERT INTO snowflake_scores (
        tickers, date, canonical_url, value, future, past, health, dividend
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# ---- Insert Rows With Timing ----
row_count = len(df)
processed = 0
skipped = 0

for row in df.itertuples(index=False):
    row_start = time.time()

    # Skip rows with invalid dates
    if pd.isna(row.date):
        skipped += 1
        continue

    try:
        cursor.execute(
            insert_sql,
            row.tickers,
            row.date,
            row.canonical_url,
            int(row.value) if pd.notna(row.value) else None,
            int(row.future) if pd.notna(row.future) else None,
            int(row.past) if pd.notna(row.past) else None,
            int(row.health) if pd.notna(row.health) else None,
            int(row.dividend) if pd.notna(row.dividend) else None,
        )
        conn.commit()  # commit per row to avoid large transaction locks
        processed += 1
        elapsed = time.time() - row_start
        print(f"[{processed}/{row_count}] Inserted {row.tickers} ({elapsed:.3f}s)")

    except pyodbc.IntegrityError as e:
        # Likely duplicate due to UNIQUE constraint
        skipped += 1
        print(f"⚠️ Duplicate/constraint error on {row.tickers}: {e}")

    except pyodbc.Error as e:
        skipped += 1
        print(f"❌ SQL Error on {row.tickers}: {e}")

# ---- Close connection ----
cursor.close()
conn.close()

# ---- Summary ----
total_elapsed = time.time() - start_time
print(f"\n🎯 Done at {datetime.now().strftime('%H:%M:%S')}")
print(f"⏱ Total time: {total_elapsed:.2f} seconds")
print(f"✅ Rows inserted: {processed}")
print(f"⚠️ Rows skipped (invalid/duplicate): {skipped}")
