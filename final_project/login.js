const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const path = require('path');
const fs = require('fs');

puppeteer.use(StealthPlugin());

// ✅ Explicit user path for root
const profilePath = 'C:/Users/hanse/AppData/Local/Google/Chrome/User Data/Default';
; // 👈 Use 'Default' or other if needed

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
    args: ['--start-maximized'],
    executablePath: 'C:/Program Files/Google/Chrome/Application/chrome.exe',
    userDataDir: profilePath
  });

  const page = await browser.newPage();

  await page.goto('https://simplywall.st/login', {
    waitUntil: 'domcontentloaded',
    timeout: 60000
  });

  console.log('🧑‍💻 Please log in manually in the opened browser...');
  console.log('⏳ Waiting 30 seconds for login and session to persist...');

  await new Promise(resolve => setTimeout(resolve, 30000));

  // 🍪 Collect and save cookies
  const cookies = await page.cookies();
  const sessionCookies = cookies.filter(c => c.domain.includes('simplywall.st'));
  console.log('🍪 Simply Wall Street session cookies:', sessionCookies);

  const cookiePath = path.resolve(__dirname, 'cookies.json');
  fs.writeFileSync(cookiePath, JSON.stringify(sessionCookies, null, 2));
  console.log(`✅ Saved session cookies to ${cookiePath}`);

  await browser.close();
})();
