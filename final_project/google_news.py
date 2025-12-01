# =====================================================================
# MEMORY-SAFE GOOGLE NEWS RSS + FINBERT SENTIMENT (SQL COMPANY NAMES)
# =====================================================================

import pandas as pd
import feedparser
from datetime import datetime
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import time
import os
import pyodbc
import streamlit as st

# =============================================================
# SETTINGS
# =============================================================
OUTPUT_CSV = "google_news.csv"
BATCH_SIZE = 10  # Process this many articles at a time

# =============================================================
# LOAD FINBERT MODEL
# =============================================================
print("Loading FinBERT model...")
FINBERT_MODEL = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(FINBERT_MODEL)
model = AutoModelForSequenceClassification.from_pretrained(FINBERT_MODEL)
label_mapping = {0: "negative", 1: "neutral", 2: "positive"}

device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

def finbert_sentiment(text):
    """Returns FinBERT sentiment label + probabilities."""
    if not isinstance(text, str) or text.strip() == "":
        return "neutral", 0.0, 0.0, 0.0
    try:
        inputs = tokenizer(text, return_tensors="pt", truncation=True).to(device)
        with torch.no_grad():
            outputs = model(**inputs)
        scores = outputs.logits.softmax(dim=1).tolist()[0]
        label = label_mapping[int(torch.argmax(outputs.logits))]
        return label, scores[0], scores[1], scores[2]
    except Exception as e:
        print(f"  ❌ Error in FinBERT sentiment for: {text[:50]}... -> {e}")
        return "neutral", 0.0, 0.0, 0.0

# =============================================================
# LOAD COMPANY NAMES FROM SQL SERVER
# =============================================================
@st.cache_data(ttl=600)
def load_sql_data():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=stock_project;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    query = "SELECT names FROM tickers WHERE names IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

print("Loading company names from SQL Server...")
tickers_df = load_sql_data()
tickers = tickers_df["names"].dropna().unique().tolist()
print(f"✅ Loaded {len(tickers)} company names from SQL database.")

# =============================================================
# GOOGLE NEWS RSS FETCHER
# =============================================================
def fetch_google_news(query):
    url = f"https://news.google.com/rss/search?q={query}"
    try:
        feed = feedparser.parse(url)
        articles = []
        for entry in feed.entries:
            articles.append({
                "query": query,
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", None),
                "source": entry.source.title if "source" in entry else None,
            })
        return articles
    except Exception as e:
        print(f"❌ Failed to fetch RSS for query '{query}': {e}")
        return []

# =============================================================
# MACRO KEYWORDS
# =============================================================
macro_terms = [
    "stock market",
    "economy",
    "inflation",
    "federal reserve",
    "interest rates",
    "CPI",
    "jobs report"
]

# =============================================================
# PREPARE OUTPUT CSV
# =============================================================
if os.path.exists(OUTPUT_CSV):
    os.remove(OUTPUT_CSV)

columns = ["query", "title", "link", "published", "source",
           "sentiment_label", "sentiment_negative", "sentiment_neutral", "sentiment_positive"]

# =============================================================
# UTILITY: Parse date
# =============================================================
def try_parse(date):
    try:
        return datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")
    except:
        return date

# =============================================================
# PROCESS NEWS AND STREAM TO CSV
# =============================================================
def process_and_save(articles, batch_size=BATCH_SIZE):
    total = len(articles)
    start_time = time.time()
    for i in range(0, total, batch_size):
        batch = articles[i:i+batch_size]
        rows = []
        for article in batch:
            article["published"] = try_parse(article.get("published", None))
            label, neg, neu, pos = finbert_sentiment(article["title"])
            article["sentiment_label"] = label
            article["sentiment_negative"] = neg
            article["sentiment_neutral"] = neu
            article["sentiment_positive"] = pos
            rows.append(article)
        # Append batch to CSV
        pd.DataFrame(rows)[columns].to_csv(OUTPUT_CSV, mode='a', index=False, header=not os.path.exists(OUTPUT_CSV))
        elapsed = time.time() - start_time
        print(f"  → Processed {min(i+batch_size, total)}/{total} articles. Elapsed: {elapsed:.1f} sec")

# =============================================================
# START GLOBAL TIMER
# =============================================================
start_time = time.time()

# =============================================================
# GATHER AND PROCESS MACRO NEWS
# =============================================================
print("\nFetching macro news...")
macro_articles = []
for idx, term in enumerate(macro_terms, 1):
    print(f"[{idx}/{len(macro_terms)}] Macro: '{term}'")
    articles = fetch_google_news(term.replace(" ", "+"))
    print(f"  → Retrieved {len(articles)} articles.")
    macro_articles.extend(articles)

process_and_save(macro_articles)

# =============================================================
# GATHER AND PROCESS COMPANY NEWS
# =============================================================
print("\nFetching company news...")
ticker_articles = []
for idx, company_name in enumerate(tickers, 1):
    print(f"[{idx}/{len(tickers)}] Company: '{company_name}'")
    articles = fetch_google_news(company_name.replace(" ", "+"))
    print(f"  → Retrieved {len(articles)} articles.")
    ticker_articles.extend(articles)
    if len(ticker_articles) >= BATCH_SIZE:
        process_and_save(ticker_articles)
        ticker_articles = []  # Clear batch to free memory

# Process any remaining articles
if ticker_articles:
    process_and_save(ticker_articles)

# =============================================================
# FINAL TIME SUMMARY
# =============================================================
total_time = time.time() - start_time
print("\n====================================================")
print(f"✔ Finished! All articles saved to {OUTPUT_CSV}")
print(f"⏱ Total elapsed time: {total_time:.1f} sec")
print("====================================================")
