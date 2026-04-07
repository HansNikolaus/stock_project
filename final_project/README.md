# 📊 Stock Data & Simply Wall Street Automation Project

## 1️⃣ Setup Base Data
Start with your CSV files containing tickers and URLs.

### tickers.csv
- `tickers` – Holds all unique stock symbols used for this project (matches Yahoo Finance tickers).  

### snowflake_chart.csv
- `tickers` – Unique stock symbols (matches Yahoo Finance tickers).  
- `canonical_url` – Corresponding Simply Wall Street canonical URLs for each ticker.  

> ✅ Optional: Add or remove columns if needed to expand the scraping scope.  

---

## 2️⃣ Scrape Daily Stock Data
**Script:** `yf_scraping.py`  
**Action:**
- Scrapes daily trading values using the **yfinance** Python library.  
- Populates the **`stock_data`** table in SQL Server.  

> ⚠️ Make sure to update SQL database connection info before running.

---

## 3️⃣ Scrape Company Information
**Script:** `tickers.py`  
**Action:**
- Scrapes company info using **yfinance**.  
- Populates the **`tickers`** table in SQL Server.

---

## 4️⃣ Login to Simply Wall Street
**Script:** `login.js`  
**Action:**
- Launches **Chrome via Puppeteer** with your **user profile folder** (`userDataDir`) for session persistence.  
- Uses **StealthPlugin** to avoid bot detection.  
- Opens Simply Wall Street login page and waits for **manual login**.  
- Waits **30 seconds**, then collects cookies and saves them as `cookies.json`.  

> These cookies allow automated scripts to remain logged in without repeated logins.

---

## 5️⃣ Download HTML for Snowflake Charts
**Script:** `svg_downloader.js`  
**Action:**
- Reads `snowflake_chart.csv` containing tickers and URLs.  
- Launches **Chrome via Puppeteer** (optionally with StealthPlugin).  
- Loads **previously saved cookies (`cookies.json`)**.  
- Visits each stock’s **canonical_url** on Simply Wall Street.  
- Saves HTML content to `html_dump/` folder.  
- Logs errors per ticker in `errors.log`.

---

## 6️⃣ Extract Simply Wall Street Data
**Scripts:**
- `simply_wallstreet_facts.py`  
- `simply_wallstreet_companyinfo.py`  
- `extract_snowflake_scores.py`  
- `fear_and_greed.py`  
- `google_news.csv`  

**Action:**
- Populates:
  - `simply_wallstreet_facts.csv`  
  - `simply_wallstreet_companyinfo.csv`  
  - `simply_wallstreet_insidertransactions.csv`  
  - `simply_wallstreet_ownershipbreakdown.csv`  
  - `snowflake_chart_updated.csv`  
  - `fear_and_greed` table in SQL Server  
  - `google_news.csv`  

---

## 7️⃣ ETL / Clean Data for SQL
**Scripts:**
- `simply_wallstreet_facts_stage.py`  
- `simply_wallstreet_companyinfo_etl.py`  
- `extract_snowflake_scores_etl.py`  
- `google_news_etl.py`  

**Action:**
- Populates:
  - `simply_wallstreet_facts_clean.csv`  
  - `company_info` table (SQL Server)  
  - `insider_transactions` table (SQL Server)  
  - `ownership_breakdown` table (SQL Server)  
  - `snowflake_scores` table (SQL Server)  
  - `google_news` table (SQL Server)  

---

## 8️⃣ Final ETL
**Script:** `simply_wallstreet_facts_etl.py`  
**Action:**
- Populates **`simply_wallstreet_facts`** table in SQL Server.  

