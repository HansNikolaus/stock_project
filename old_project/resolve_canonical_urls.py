import pandas as pd
import requests
import time
import os

# 🧾 Load your tickers and canonical URLs
df = pd.read_csv("snowflake_chart.csv")

# 🌐 GraphQL endpoint and headers
url = "https://simplywall.st/graphql"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://simplywall.st",
    "Referer": "https://simplywall.st",
    "apollographql-client-name": "web",
    "apollographql-client-version": "mono-auto-89001bda"
}

# 🔐 Add your valid clearance cookie
cookies = {
    "cf_clearance": "CBxP7vIoLG.Oyc0S1qi9DDZYA6bhRr_hVEfW.02xDCc-1750526901-1.2.1.1-QQ7sgJTDCYRuE83k.g4J._s0cxYJxiifyJpTjtTto418QZgNJgJCfMFhaNOmQ7TxIZxk9WVcbVkIdCtA4Ir_E_3pvKeq1KGq_XHOZeoTj8_6LyMDw4JhHygaOPVZKIjedYkvccWsJXUWdJeu9hs5davcNIV..nlRWaP_Axv_z6xK6bC.gTK3aiX04dBS9dbZZb.ztK9CMPTqPm8praQYgAKMe6sLAVfwCHwbgi1Do2WhN8pk3Plta6GrAao5dvY7HIEL9UK6zCVx.D2KXjp8vOQrb_p_7.yep5FsROu1cSPc2Z45u_Bs2uwo3Gwqwae7JSm2WcJwg73.GBgfyDyjh7L5HrszympXF3BoRxujvU8"
}

# 🔎 GraphQL query
query = """
query CompanySummary($canonicalUrl: String!) {
  Company(canonicalURL: $canonicalUrl) {
    score {
      value
      future
      past
      health
      dividend
    }
  }
}
"""

# 🔁 Loop through each company
for i, row in df.iterrows():
    ticker = row["tickers"]
    canonical_url = row["canonical_url"]

    if pd.isna(canonical_url) or not canonical_url.startswith("/stocks/"):
        print(f"⚠️ Skipping {ticker}: invalid canonical URL")
        continue

    # 🚀 Fetch Snowflake scores
    payload = {
        "query": query,
        "variables": {"canonicalUrl": canonical_url},
        "operationName": "CompanySummary"
    }

    try:
        resp = requests.post(url, headers=headers, cookies=cookies, json=payload)
        resp.raise_for_status()
        data = resp.json()
        score = data.get("data", {}).get("Company", {}).get("score", {})

        if score:
            df.at[i, "value"] = score.get("value")
            df.at[i, "future"] = score.get("future")
            df.at[i, "past"] = score.get("past")
            df.at[i, "health"] = score.get("health")
            df.at[i, "dividend"] = score.get("dividend")
            print(f"✅ {ticker}: score updated")
        else:
            print(f"⚠️ {ticker}: no score returned")
    except Exception as e:
        print(f"❌ {ticker}: GraphQL error - {e}")

    time.sleep(0.5)  # gentle pause

# 💾 Save updates to disk
df.to_csv("snowflake_chart.csv", index=False)
print("\n✅ All done! Scores updated.")

