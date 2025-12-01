#!/usr/bin/env python3
"""
Unified ETL pipeline for simply_wallstreet_facts:

1. Load raw simply_wallstreet_facts.csv
2. Clean and normalize all columns
3. Detect and fix dtype issues (Int64 / float64 / string)
4. Connect to SQL Server
5. Auto-add missing SQL columns
6. Auto-upgrade INT → BIGINT for overflow
7. Auto-upgrade NVARCHAR(n) → NVARCHAR(MAX) for truncation
8. Insert in perfect SQL column order (excluding facts_id)
9. Chunked inserts — no memory explosion

This script is fully production-safe.
"""

import pandas as pd
import numpy as np
import pyodbc
import math
from datetime import datetime, date

RAW_CSV = "simply_wallstreet_facts_clean.csv"
SCHEMA = "dbo"
TABLE = "simply_wallstreet_facts"
INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647

# ====================================================
# Utility Functions
# ====================================================
def pythonize_value(v):
    """Convert pandas/numpy values into pure Python types that pyodbc accepts."""
    if v is pd.NA or v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (np.integer, np.int32, np.int64)):
        return int(v)
    if isinstance(v, (np.floating, np.float32, np.float64)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        try:
            return v.to_pydatetime().date()
        except:
            return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return v

def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk

def infer_safe_dtype(series: pd.Series):
    """Determine a safe dtype for the column."""
    nonnull = series.dropna()
    if nonnull.empty:
        return "string"
    try:
        numeric = pd.to_numeric(nonnull, errors="coerce")
    except:
        return "string"
    if numeric.isna().all():
        return "string"
    # Has decimals → float
    if (numeric % 1 != 0).any():
        return "float64"
    minv = numeric.min()
    maxv = numeric.max()
    if minv >= INT32_MIN and maxv <= INT32_MAX:
        return "Int64"
    return "float64"

# ====================================================
# MAIN ETL PIPELINE
# ====================================================
def main():
    # ------------------------------------------------
    # Load RAW CSV
    # ------------------------------------------------
    print(f"📄 Loading raw CSV: {RAW_CSV}")
    df = pd.read_csv(RAW_CSV).astype("string")
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns.\n")

    # ------------------------------------------------
    # CLEAN + Normalize dtypes
    # ------------------------------------------------
    print("🧹 Inferring and applying safe dtypes...\n")
    inferred = {}
    for col in df.columns:
        inferred[col] = infer_safe_dtype(df[col])
        print(f" {col}: {inferred[col]}")
    for col, dtype in inferred.items():
        if dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = df[col].astype("string")
    df = df.where(pd.notnull(df), None)
    df = df.replace({pd.NA: None})

    # ------------------------------------------------
    # Connect to SQL Server
    # ------------------------------------------------
    print("\n🔌 Connecting to SQL Server...")
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=stock_project;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    cursor = conn.cursor()
    print("Connected.\n")

    # ------------------------------------------------
    # Get SQL metadata
    # ------------------------------------------------
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE}'
        ORDER BY ORDINAL_POSITION
    """)
    sql_meta = {
        row.COLUMN_NAME: {
            "datatype": row.DATA_TYPE,
            "maxlen": row.CHARACTER_MAXIMUM_LENGTH,
            "pos": row.ORDINAL_POSITION,
        } for row in cursor.fetchall()
    }

    # ------------------------------------------------
    # Add missing SQL columns
    # ------------------------------------------------
    print("🧩 Checking for missing SQL columns...\n")
    missing_cols = [c for c in df.columns if c not in sql_meta]
    for col in missing_cols:
        dtype = inferred[col]
        if dtype == "Int64":
            sqltype = "INT"
        elif dtype == "float64":
            sqltype = "FLOAT"
        else:
            sqltype = "NVARCHAR(MAX)"
        print(f" ➕ Adding: {col} ({sqltype})")
        cursor.execute(f"""ALTER TABLE {SCHEMA}.{TABLE} ADD [{col}] {sqltype} NULL;""")
        conn.commit()

    # Refresh metadata
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE}'
        ORDER BY ORDINAL_POSITION
    """)
    sql_meta = {
        row.COLUMN_NAME: {
            "datatype": row.DATA_TYPE,
            "maxlen": row.CHARACTER_MAXIMUM_LENGTH,
            "pos": row.ORDINAL_POSITION,
        } for row in cursor.fetchall()
    }

    # ------------------------------------------------
    # Fix INT columns that overflow
    # ------------------------------------------------
    print("\n📏 Checking SQL INT columns for overflow...\n")
    for col, meta in sql_meta.items():
        if meta["datatype"] != "int":
            continue
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.isna().all():
            continue
        minv = numeric.min()
        maxv = numeric.max()
        if minv < INT32_MIN or maxv > INT32_MAX:
            print(f" ⚠ Overflow in {col}, converting INT → BIGINT")
            cursor.execute(f"""ALTER TABLE {SCHEMA}.{TABLE} ALTER COLUMN [{col}] BIGINT NULL;""")
            conn.commit()

    # ------------------------------------------------
    # Fix NVARCHAR truncation
    # ------------------------------------------------
    print("\n🔠 Checking NVARCHAR truncation...\n")
    for col, meta in sql_meta.items():
        if meta["datatype"] != "nvarchar":
            continue
        if meta["maxlen"] in (None, -1):
            continue
        if col not in df.columns:
            continue
        max_csv_len = df[col].dropna().map(len).max()
        if max_csv_len > meta["maxlen"]:
            print(f" ⚠ Expanding {col} → NVARCHAR(MAX)")
            cursor.execute(f"""ALTER TABLE {SCHEMA}.{TABLE} ALTER COLUMN [{col}] NVARCHAR(MAX) NULL;""")
            conn.commit()

    # ------------------------------------------------
    # Final SQL column order
    # ------------------------------------------------
    cursor.execute(f"""
        SELECT COLUMN_NAME, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{TABLE}'
        ORDER BY ORDINAL_POSITION
    """)
    sql_order = [row.COLUMN_NAME for row in cursor.fetchall()]
    if "facts_id" in sql_order:
        sql_order.remove("facts_id")
    insert_cols = [c for c in sql_order if c in df.columns]
    print(f"\nFinal insert column count: {len(insert_cols)}\n")

    col_list = ", ".join(f"[{c}]" for c in insert_cols)
    placeholders = ", ".join("?" for _ in insert_cols)
    insert_sql = f"""INSERT INTO {SCHEMA}.{TABLE} ({col_list}) VALUES ({placeholders})"""

    # ------------------------------------------------
    # Insert rows (chunked, safe)
    # ------------------------------------------------
    print("🚀 Inserting rows...\n")
    cursor.fast_executemany = False
    rows = []
    for _, row in df[insert_cols].iterrows():
        rows.append(tuple(pythonize_value(v) for v in row.values))
    total = 0
    CHUNK = 25
    for chunk in chunked_iterable(rows, CHUNK):
        cursor.executemany(insert_sql, chunk)
        conn.commit()
        total += len(chunk)
        print(f"Inserted {total}/{len(rows)}...", end="\r", flush=True)

    print(f"\n\n🎉 DONE! Successfully inserted {total} rows.")
    cursor.close()
    conn.close()
    print("\n🔒 SQL connection closed.\n")

if __name__ == "__main__":
    main()
