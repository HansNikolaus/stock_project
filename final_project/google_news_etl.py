import pandas as pd
import pyodbc

# ============================================
# CONFIG
# ============================================

CSV_FILE = "google_news.csv"
TABLE_NAME = "google_news"
BATCH_SIZE = 100  # commit every 100 rows

# ============================================
# CONNECT TO SQL SERVER
# ============================================

print("\n🔌 Connecting to SQL Server...")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=stock_project;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()
cursor.fast_executemany = True
print("Connected.\n")

# ============================================
# CREATE TABLE IF NOT EXISTS
# ============================================

create_table_sql = f"""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{TABLE_NAME}' AND xtype='U')
BEGIN
    CREATE TABLE {TABLE_NAME} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        query_text VARCHAR(200),
        title_text NVARCHAR(2000),
        link_url NVARCHAR(2000) NOT NULL,
        published_at DATETIME,
        source_name VARCHAR(200),
        sentiment_label VARCHAR(50),
        sentiment_negative DECIMAL(10, 9),
        sentiment_neutral  DECIMAL(10, 9),
        sentiment_positive DECIMAL(10, 9),
        CONSTRAINT uq_{TABLE_NAME}_title_published UNIQUE (title_text, published_at)
    );
END
"""
cursor.execute(create_table_sql)
conn.commit()
print(f"✔ Table '{TABLE_NAME}' is ready.\n")

# ============================================
# LOAD CSV
# ============================================

df = pd.read_csv(CSV_FILE)
print(f"✔ Loaded {len(df)} rows from CSV.\n")

# ============================================
# RENAME COLUMNS
# ============================================

df.rename(columns={
    "query": "query_text",
    "title": "title_text",
    "link": "link_url",
    "published": "published_at",
    "source": "source_name",
}, inplace=True)

# ============================================
# CLEAN query_text (IMPORTANT FIX)
# ============================================

df["query_text"] = (
    df["query_text"]
    .astype(str)
    .str.replace("+", " ", regex=False)
    .str.replace("%20", " ", regex=False)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

# ============================================
# DATETIME CONVERSION
# ============================================

if "published_at" in df.columns:
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

# ============================================
# PREPARE MERGE STATEMENT
# ============================================

merge_sql = f"""
MERGE {TABLE_NAME} AS target
USING (SELECT ? AS title_text, ? AS published_at) AS src
ON target.title_text = src.title_text
AND target.published_at = src.published_at
WHEN NOT MATCHED THEN
    INSERT (
        query_text,
        title_text,
        link_url,
        published_at,
        source_name,
        sentiment_label,
        sentiment_negative,
        sentiment_neutral,
        sentiment_positive
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

# ============================================
# INSERT LOOP
# ============================================

print("⏳ Inserting rows...\n")
batch_count = 0

for idx, row in df.iterrows():
    try:
        cursor.execute(
            merge_sql,
            row.get("title_text"),
            row.get("published_at"),
            row.get("query_text"),
            row.get("title_text"),
            row.get("link_url"),
            row.get("published_at"),
            row.get("source_name"),
            row.get("sentiment_label"),
            row.get("sentiment_negative"),
            row.get("sentiment_neutral"),
            row.get("sentiment_positive")
        )
        batch_count += 1

        if batch_count % BATCH_SIZE == 0:
            conn.commit()
            print(f"  → {idx + 1}/{len(df)} rows processed")

    except Exception as e:
        print(f"❌ Error inserting row {idx}: {e}")

conn.commit()
print(f"\n✔ All {len(df)} rows processed.\n")

# ============================================
# CLEAN UP
# ============================================

cursor.close()
conn.close()
print("🎉 Done! SQL Server connection closed.")
