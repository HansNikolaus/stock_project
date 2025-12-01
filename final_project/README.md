This is the final project!

1) begin by adding/removing to these columns if you want. they are the base data to begin scraping.
  tickers.csv
      - tickers - Holds all unique stock symbols used for this project that match Yahoo Finance's ticker for them.
  snowflake_chart.csv
      - tickers - Holds all unique stock symbols used for this project that match Yahoo Finance's ticker for them.
      - canonical_url - Holds all unique simplywallstreet canoncial urls to mathc their corresponding Yahoo Finance ticker.


2) run yf_scraping.py (change to match your sql database information)
      - Scrapes daily trade values using yfinance python library.
      - Populates
          stock_data table SQL Server
  
3) run tickers.py
      - Scrapes company information using yfinance python library.
      - Populates
          tickers table SQL Server

4) run login.js (you will need to set up your own puppeteer library, chrome extension, and chrome user profile folder)
      - Launches Chrome via Puppeteer, pointing to your user profile folder (userDataDir) so login sessions persist.
      - Uses StealthPlugin to avoid detection as a bot.
      - Opens the Simply Wall Street login page and waits for the user to log in manually.
      - After 30 seconds, collects cookies relevant to simplywall.st and saves them as cookies.json.
      - This allows later scripts to use the authenticated session without logging in again.
  
4) run svg_downloader.js
      - Reads a CSV file (snowflake_chart.csv) containing stock tickers and URLs.
      - Launches Chrome via Puppeteer (again using StealthPlugin optionally, but in your code you didn’t specify stealth here—it can be added).
      - Loads previously saved cookies (cookies.json) to remain logged in.
      - Visits each stock’s page (canonical_url) on Simply Wall Street.
      - Saves the HTML content of each page into a folder html_dump.
      - Logs errors per ticker in errors.log.

5) run simply_wallstreet_facts.py, simply_wallstreet_companyinfo.py, extract_snowflake_scorees.py, fear_and_greed.py, google_news.csv
      - Populates
          simply_wallstreet_facts.csv
          simply_wallstreet_companyinfo.csv
          simply_wallstreet_insidertransactions.csv
          simply_wallstreet_ownershipbreakdown.csv
          snowflake_chart_updated.csv
          fear_and_greed table SQL Server
          google_news.csv

6) run simply_wallstreet_facts_stage.py, simply_wallstreet_companyinfo_etl.py, extract_snowflake_scores_etl.py, google_news_etl.py
      - Populates
          simply_wallstreet_facts_clean.csv
          company_info table SQL Server
          insider_transactions table SQL Server
          ownership_breakdown table SQL Server
          snowflake_scores table SQL Server
          google_news table SQL Server

7) run simply_wallstreet_facts_etl.py
       - Populates
          simply_wallstreet_facts table SQL Server
