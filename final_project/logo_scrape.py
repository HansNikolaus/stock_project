import requests
import os
import time
import pandas as pd

API_KEY = 'd1897uhr01ql1b4mjrhgd1897uhr01ql1b4mjri0'
BASE_URL = 'https://finnhub.io/api/v1'
CSV_FILE = 'tickers.csv'
SAVE_PATH = './logos'

# Load ticker list from CSV
df = pd.read_csv(CSV_FILE)
tickers = df['tickers'].dropna().unique()  # Ensures no duplicates or NaNs

os.makedirs(SAVE_PATH, exist_ok=True)

for ticker in tickers:
    try:
        profile_url = f'{BASE_URL}/stock/profile2?symbol={ticker}&token={API_KEY}'
        profile_response = requests.get(profile_url)
        profile_data = profile_response.json()
        logo_url = profile_data.get('logo')

        if logo_url:
            logo_response = requests.get(logo_url)
            with open(os.path.join(SAVE_PATH, f'{ticker}.png'), 'wb') as f:
                f.write(logo_response.content)
            print(f"{ticker}: Logo saved.")
        else:
            print(f"{ticker}: Logo not found.")

        time.sleep(1)  # Respect API rate limits
    except Exception as e:
        print(f"{ticker}: Error - {e}")
