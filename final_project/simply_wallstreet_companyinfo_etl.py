#!/usr/bin/env python3

import time
import pandas as pd
import pyodbc
from datetime import datetime

# ========== CONFIG ==========
CSV_OWNERSHIP = "simply_wallstreet_ownershipbreakdown.csv"
CSV_INSIDER = "simply_wallstreet_insidertransactions.csv"
CSV_COMPANY = "simply_wallstreet_companyinfo.csv"

BATCH_SIZE = 100
PROGRESS_STEP = 100

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=stock_project;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# ========== Helper functions ==========

def to_date_only(series):
    """Convert a pandas Series to date-only (YYYY-MM-DD) safely."""
    return pd.to_datetime(series, errors="coerce").dt.date

def prepare_dataframe_for_sql(df):
    """
    Replace NaN with None (so pyodbc will insert NULL),
    ensure proper dtypes where possible.
    Return a copy.
    """
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    return df

def insert_df_duplicate_safe(df, table_name, cursor, conn, batch_size=BATCH_SIZE):
    """
    Insert DataFrame into SQL Server using batches.
    If a batch raises pyodbc.IntegrityError (unique constraint), rollback and retry row-by-row,
    skipping only the rows that throw IntegrityError.
    """
    df = prepare_dataframe_for_sql(df)
    total = len(df)
    if total == 0:
        print(f"→ No rows to insert into {table_name}.")
        return {"inserted": 0, "skipped": 0}

    # Build insert SQL from dataframe columns (assume column names match DB)
    cols = [f"[{c}]" for c in df.columns]
    placeholders = ",".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

    inserted = 0
    skipped = 0
    batch = []
    start_time = time.time()

    print(f"\n⏳ Inserting {total} rows into {table_name} (batch_size={batch_size})...")

    for idx, row in df.iterrows():
        batch.append(tuple(row))
        # Progress print
        if (idx + 1) % PROGRESS_STEP == 0:
            print(f"  → {idx + 1}/{total} rows prepared")

        if len(batch) >= batch_size:
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                batch = []
            except pyodbc.IntegrityError as e:
                # Batch had duplicates (or constraint violations) - rollback and retry row-by-row
                conn.rollback()
                for r in batch:
                    try:
                        cursor.execute(insert_sql, r)
                        inserted += 1
                    except pyodbc.IntegrityError:
                        skipped += 1
                    except pyodbc.Error as e2:
                        # other SQL error on a single row: log and skip
                        print(f"  ⚠ SQL error on single row: {e2}. Row skipped.")
                        skipped += 1
                conn.commit()
                batch = []
            except pyodbc.Error as e:
                # Unexpected SQL error on batch; rollback and retry rows individually
                conn.rollback()
                print(f"  ⚠ Unexpected SQL error on batch: {e}. Retrying rows individually...")
                for r in batch:
                    try:
                        cursor.execute(insert_sql, r)
                        inserted += 1
                    except pyodbc.IntegrityError:
                        skipped += 1
                    except pyodbc.Error as e2:
                        print(f"    ⚠ SQL error on single row: {e2}. Row skipped.")
                        skipped += 1
                conn.commit()
                batch = []

    # final partial batch
    if batch:
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
        except pyodbc.IntegrityError:
            conn.rollback()
            for r in batch:
                try:
                    cursor.execute(insert_sql, r)
                    inserted += 1
                except pyodbc.IntegrityError:
                    skipped += 1
                except pyodbc.Error as e2:
                    print(f"  ⚠ SQL error on single row: {e2}. Row skipped.")
                    skipped += 1
            conn.commit()
        except pyodbc.Error as e:
            conn.rollback()
            print(f"  ⚠ Unexpected SQL error on final batch: {e}. Retrying rows individually...")
            for r in batch:
                try:
                    cursor.execute(insert_sql, r)
                    inserted += 1
                except pyodbc.IntegrityError:
                    skipped += 1
                except pyodbc.Error as e2:
                    print(f"    ⚠ SQL error on single row: {e2}. Row skipped.")
                    skipped += 1
            conn.commit()

    elapsed = time.time() - start_time
    print(f"✔ Insert into {table_name} complete: inserted={inserted}, skipped={skipped} ({elapsed:.2f}s)")
    return {"inserted": inserted, "skipped": skipped}

# ========== Main ETL ==========

def main():
    overall_start = time.time()

    print("\n🔌 Connecting to SQL Server...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
    except Exception as e:
        print(f"❌ Could not connect to SQL Server: {e}")
        return

    cursor = conn.cursor()
    cursor.fast_executemany = True
    print("Connected.\n")

    # ---------- 1) load CSVs ----------
    print("Loading CSV files...")
    ownership_df = pd.read_csv(CSV_OWNERSHIP)
    insider_df = pd.read_csv(CSV_INSIDER)
    company_df = pd.read_csv(CSV_COMPANY)
    print(f"  → ownership rows: {len(ownership_df)}")
    print(f"  → insider rows  : {len(insider_df)}")
    print(f"  → company rows  : {len(company_df)}\n")

    # ---------- 2) convert date columns to date-only ----------
    # Adjust column names used in your CSVs; these match the earlier script
    # Ownership CSV: 'HTML Creation Date'
    # Insider CSV: 'HTML Creation Date', 'FilingDate'
    # Company CSV: 'HTML Creation Date', 'Holding Date'
    print("Converting date columns to date-only (YYYY-MM-DD)...")
    try:
        if "HTML Creation Date" in ownership_df.columns:
            ownership_df["HTML Creation Date"] = to_date_only(ownership_df["HTML Creation Date"])
        if "HTML Creation Date" in insider_df.columns:
            insider_df["HTML Creation Date"] = to_date_only(insider_df["HTML Creation Date"])
        if "FilingDate" in insider_df.columns:
            insider_df["FilingDate"] = to_date_only(insider_df["FilingDate"])
        if "HTML Creation Date" in company_df.columns:
            company_df["HTML Creation Date"] = to_date_only(company_df["HTML Creation Date"])
        if "Holding Date" in company_df.columns:
            company_df["Holding Date"] = to_date_only(company_df["Holding Date"])
    except Exception as e:
        print(f"❌ Date conversion error: {e}")
        conn.close()
        return
    print("Date conversion done.\n")

    # ---------- 3) deduplicate where appropriate ----------
    # These are the dedup rules from your previous script
    print("Removing duplicates (per your constraints)...")
    insider_df = insider_df.drop_duplicates(subset=["FilingDate", "OwnerName"])
    company_df = company_df.drop_duplicates(subset=["Owner Name", "Holding Date"])
    print("Duplicates removed where required.\n")

    # ---------- 4) rename columns to match DB schema ----------
    print("Renaming columns to DB-friendly names...")
    ownership_df.rename(columns={
        'Ticker': 'ticker',
        'HTML Creation Date': 'html_creation_date',
        'InstitutionsShares': 'institutions_shares',
        'InstitutionsPercent': 'institutions_percent',
        'PublicCompaniesShares': 'public_companies_shares',
        'PublicCompaniesPercent': 'public_companies_percent',
        'PrivateCompaniesShares': 'private_companies_shares',
        'PrivateCompaniesPercent': 'private_companies_percent',
        'IndividualInsidersShares': 'individual_insiders_shares',
        'IndividualInsidersPercent': 'individual_insiders_percent',
        'VCPEFirmsShares': 'vcpe_firms_shares',
        'VCPEFirmsPercent': 'vcpe_firms_percent',
        'GeneralPublicShares': 'general_public_shares',
        'GeneralPublicPercent': 'general_public_percent'
    }, inplace=True)

    insider_df.rename(columns={
        'Ticker': 'ticker',
        'HTML Creation Date': 'html_creation_date',
        'FilingDate': 'filing_date',
        'OwnerName': 'owner_name',
        'OwnerType': 'owner_type',
        'TransactionType': 'transaction_type',
        'Shares': 'shares',
        'PriceMax': 'price_max',
        'TransactionValue': 'transaction_value'
    }, inplace=True)

    company_df.rename(columns={
        'Ticker': 'ticker',
        'HTML Creation Date': 'html_creation_date',
        'Owner Name': 'owner_name',
        'Owner Type': 'owner_type',
        'Shares Held': 'shares_held',
        'Percent of Shares Outstanding': 'percent_shares_outstanding',
        'Percent of Portfolio': 'percent_of_portfolio',
        'Holding Date': 'holding_date'
    }, inplace=True)

    print("Renaming done.\n")

    # ---------- 5) create tables if not exists (safe) ----------
    # Using the same CREATE statements as you used before
    print("Ensuring destination tables exist (IF NOT EXISTS)...")
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ownership_breakdown' AND xtype='U')
    CREATE TABLE dbo.ownership_breakdown (
        ticker NVARCHAR(50),
        html_creation_date DATE,
        institutions_shares BIGINT,
        institutions_percent FLOAT,
        public_companies_shares BIGINT,
        public_companies_percent FLOAT,
        private_companies_shares BIGINT,
        private_companies_percent FLOAT,
        individual_insiders_shares BIGINT,
        individual_insiders_percent FLOAT,
        vcpe_firms_shares BIGINT,
        vcpe_firms_percent FLOAT,
        general_public_shares BIGINT,
        general_public_percent FLOAT,
        CONSTRAINT uq_ticker_html_date UNIQUE (ticker, html_creation_date)
    )
    """)
    conn.commit()

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='insider_transactions' AND xtype='U')
    CREATE TABLE dbo.insider_transactions (
        ticker NVARCHAR(50),
        html_creation_date DATE,
        filing_date DATE,
        owner_name NVARCHAR(255),
        owner_type NVARCHAR(100),
        transaction_type NVARCHAR(50),
        shares BIGINT,
        price_max FLOAT,
        transaction_value FLOAT,
        CONSTRAINT uq_filing_owner UNIQUE (filing_date, owner_name)
    )
    """)
    conn.commit()

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='company_info' AND xtype='U')
    CREATE TABLE dbo.company_info (
        ticker NVARCHAR(50),
        html_creation_date DATE,
        owner_name NVARCHAR(255),
        owner_type NVARCHAR(100),
        shares_held BIGINT,
        percent_shares_outstanding FLOAT,
        percent_of_portfolio FLOAT,
        holding_date DATE,
        CONSTRAINT uq_owner_holding UNIQUE (owner_name, holding_date)
    )
    """)
    conn.commit()
    print("Tables ensured.\n")

    # ---------- 6) Insert data (duplicate-safe batches) ----------
    # Reorder columns to match the DB create statements so insertion columns line up in expected order:
    ownership_cols = [
        "ticker", "html_creation_date",
        "institutions_shares", "institutions_percent",
        "public_companies_shares", "public_companies_percent",
        "private_companies_shares", "private_companies_percent",
        "individual_insiders_shares", "individual_insiders_percent",
        "vcpe_firms_shares", "vcpe_firms_percent",
        "general_public_shares", "general_public_percent"
    ]
    insider_cols = [
        "ticker", "html_creation_date", "filing_date",
        "owner_name", "owner_type", "transaction_type",
        "shares", "price_max", "transaction_value"
    ]
    company_cols = [
        "ticker", "html_creation_date", "owner_name",
        "owner_type", "shares_held", "percent_shares_outstanding",
        "percent_of_portfolio", "holding_date"
    ]

    # Ensure the dataframes contain those columns (if any column missing, add with None)
    def ensure_cols(df, cols):
        for c in cols:
            if c not in df.columns:
                df[c] = None
        return df[cols]

    ownership_df = ensure_cols(ownership_df, ownership_cols)
    insider_df = ensure_cols(insider_df, insider_cols)
    company_df = ensure_cols(company_df, company_cols)

    # Run the duplicate-safe inserts
    # (prints inserted/skipped counts)
    insert_results = {}
    insert_results['ownership'] = insert_df_duplicate_safe(ownership_df, "dbo.ownership_breakdown", cursor, conn, batch_size=BATCH_SIZE)
    insert_results['insider'] = insert_df_duplicate_safe(insider_df, "dbo.insider_transactions", cursor, conn, batch_size=BATCH_SIZE)
    insert_results['company'] = insert_df_duplicate_safe(company_df, "dbo.company_info", cursor, conn, batch_size=BATCH_SIZE)

    # ---------- 7) FINISH ----------
    cursor.close()
    conn.close()

    total_elapsed = time.time() - overall_start
    print("\nETL complete ✅")
    print(f"Total runtime: {total_elapsed:.2f}s")
    print("Summary:")
    for k, v in insert_results.items():
        print(f"  • {k}: inserted={v['inserted']}, skipped={v['skipped']}")

if __name__ == "__main__":
    main()
