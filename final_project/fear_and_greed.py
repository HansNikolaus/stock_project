import requests
import pandas as pd
import pyodbc

# --- Fetch Fear & Greed Index data ---
url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/116.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9"
}

resp = requests.get(url, headers=headers)
if resp.status_code != 200:
    raise Exception(f"Failed to fetch data: {resp.status_code}")

data = resp.json()
records = data.get("fear_and_greed_historical", {}).get("data", [])
if not records:
    raise ValueError("No historical data found in the response.")

# --- Convert to DataFrame ---
df = pd.DataFrame(records)
df['date'] = pd.to_datetime(df['x'], unit='ms').dt.date  # pure date only
df['fear_and_greed'] = pd.to_numeric(df['y'], errors='coerce')
df = df[['date', 'fear_and_greed']].sort_values('date').reset_index(drop=True)

print(f"✅ Prepared {len(df)} Fear & Greed records to insert")

# --- Connect to SQL Server ---
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=stock_project;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()
print("✅ Connected to SQL Server")

# --- Create table if it does not exist ---
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fear_and_greed_index' AND xtype='U')
CREATE TABLE [dbo].[fear_and_greed_index] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    fear_and_greed FLOAT NOT NULL
)
""")
conn.commit()
print("✅ Table 'fear_and_greed_index' ensured")

# --- Insert data into SQL table ---
insert_sql = "INSERT INTO [dbo].[fear_and_greed_index] (date, fear_and_greed) VALUES (?, ?)"
for idx, row in df.iterrows():
    try:
        # Skip if date already exists (UNIQUE constraint protects table)
        cursor.execute("SELECT COUNT(*) FROM [dbo].[fear_and_greed_index] WHERE date=?", row['date'])
        if cursor.fetchone()[0] > 0:
            continue
        cursor.execute(insert_sql, row['date'], row['fear_and_greed'])
    except Exception as e:
        print(f"⚠️ Failed to insert {row['date']}: {e}")

conn.commit()
cursor.close()
conn.close()
print(f"✅ Inserted Fear & Greed data into SQL Server (stock_project)")
