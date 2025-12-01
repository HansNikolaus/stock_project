const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

const CSV_FILE = 'snowflake_chart.csv';
const COOKIES_FILE = 'cookies.json';
const HTML_DUMP_FOLDER = 'html_dump';
const BASE_URL = 'https://simplywall.st';
const ERROR_LOG = 'errors.log';

// Read CSV file
function readCSV() {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(CSV_FILE)
      .pipe(csv())
      .on('data', row => rows.push(row))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

// Apply cookies to a page
async function applyCookies(page) {
  if (!fs.existsSync(COOKIES_FILE)) {
    throw new Error(`Cookies file "${COOKIES_FILE}" not found. Run login.js first.`);
  }
  const cookieData = fs.readFileSync(COOKIES_FILE, 'utf8');
  const cookies = JSON.parse(cookieData);
  await page.setCookie(...cookies);
  console.log('🍪 Cookies applied');
}

// Log errors
function logError(ticker, error) {
  const logMsg = `${new Date().toISOString()} | ${ticker} | ${error}\n`;
  fs.appendFileSync(ERROR_LOG, logMsg);
}

// Dump HTML for each ticker
async function dumpHTML(rows) {
  // Use your installed Chrome for better reliability
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
    args: ['--start-maximized'],
    executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe', // <-- Adjust if needed
  });

  const page = await browser.newPage();
  await applyCookies(page);

  if (!fs.existsSync(HTML_DUMP_FOLDER)) fs.mkdirSync(HTML_DUMP_FOLDER);

  for (const row of rows) {
    const ticker = row['tickers'].trim();
    const pathUrl = row['canonical_url'].trim();
    const fullUrl = BASE_URL + pathUrl;

    try {
      console.log(`🌐 Visiting ${fullUrl}`);
      await page.goto(fullUrl, { waitUntil: 'networkidle2', timeout: 60000 });
      await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for dynamic content

      const htmlContent = await page.content();
      const htmlPath = path.join(HTML_DUMP_FOLDER, `${ticker}.html`);
      fs.writeFileSync(htmlPath, htmlContent, 'utf8');
      console.log(`📝 HTML dumped for ${ticker}`);

    } catch (err) {
      console.error(`❌ Error dumping ${ticker}: ${err.message}`);
      logError(ticker, err.message);
    }
  }

  await browser.close();
  console.log('✅ All HTML files created');
}

// Main execution
(async () => {
  try {
    const tickers = await readCSV();
    await dumpHTML(tickers);
  } catch (err) {
    console.error('🚨 Fatal error:', err.message);
  }
})();
