#!/usr/bin/env python3
import os
import json
import csv
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# === CONFIG ===
HTML_FOLDER = 'html_dump'
OUT_SHAREHOLDERS = 'simply_wallstreet_companyinfo.csv'
OUT_OWNERSHIP = 'simply_wallstreet_ownershipbreakdown.csv'
OUT_INSIDERS = 'simply_wallstreet_insidertransactions.csv'
# =============

def epochms_to_iso(epoch_ms):
    """Convert epoch milliseconds (int/float or numeric string) to ISO date YYYY-MM-DD.
       If input is falsy or invalid, return empty string.
    """
    if epoch_ms is None:
        return ''
    try:
        # sometimes numbers come as strings; attempt conversion
        ms = int(epoch_ms)
        # guard against obviously wrong values
        if ms <= 0:
            return ''
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return ''

def safe_int(x):
    try:
        if x is None:
            return 0
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0

def safe_float(x):
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        try:
            # if it's a string like "1,234" remove commas
            xs = str(x).replace(',', '')
            return float(xs)
        except Exception:
            return 0.0

def clean_json_like_text(s):
    """Quick cleaning for JSON-ish string extracted from HTML script:
       - replace 'undefined' with null
       - replace single trailing commas in objects/arrays (simple heuristic)
       - keep other characters untouched
    """
    if s is None:
        return s
    # Replace "undefined" (JS) with null so json.loads can handle it
    s = s.replace('undefined', 'null')
    # Remove control characters that might break JSON
    # (keep it conservative)
    s = s.replace('\r', ' ').replace('\n', ' ')
    # Remove sequences like ,} or ,] (trailing commas) - simple fix
    s = re.sub(r',\s*}', '}', s)
    s = re.sub(r',\s*]', ']', s)
    return s

def extract_json_block_from_script(script_text, key_name):
    """Locate the JSON object for key_name (e.g. "topShareholders" or "ownershipBreakdown")
       inside a script text using index and brace counting. Returns the string of the JSON value
       (which may be an object or primitive) or None.
    """
    if not script_text:
        return None
    start_token = f'"{key_name}":'
    idx = script_text.find(start_token)
    if idx == -1:
        # try without quotes (defensive)
        start_token2 = f'{key_name}:'
        idx = script_text.find(start_token2)
        if idx == -1:
            return None
        start = idx + len(start_token2)
    else:
        start = idx + len(start_token)

    # Trim leading whitespace
    rest = script_text[start:].lstrip()

    # If it starts with { or [, parse till matching brace
    if rest.startswith('{') or rest.startswith('['):
        open_ch = rest[0]
        close_ch = '}' if open_ch == '{' else ']'
        brace_count = 0
        end_idx = None
        for i, ch in enumerate(rest):
            if ch == open_ch:
                brace_count += 1
            elif ch == close_ch:
                brace_count -= 1
                if brace_count == 0:
                    end_idx = i + 1
                    break
        if end_idx is None:
            return None
        return rest[:end_idx]
    else:
        # Might be a primitive (number, string, null) up to next comma or closing brace
        # find first comma or brace
        m = re.match(r'^(.*?)(,|\}|\])', rest)
        if m:
            return m.group(1).strip()
        else:
            return rest.strip()

# Prepare output CSVs and headers
shareholders_header = [
    'Ticker', 'HTML Creation Date', 'Owner Name', 'Owner Type', 'Shares Held',
    'Percent of Shares Outstanding', 'Percent of Portfolio', 'Holding Date'
]

ownership_header = [
    'Ticker', 'HTML Creation Date',
    'InstitutionsShares', 'InstitutionsPercent',
    'PublicCompaniesShares', 'PublicCompaniesPercent',
    'PrivateCompaniesShares', 'PrivateCompaniesPercent',
    'IndividualInsidersShares', 'IndividualInsidersPercent',
    'VCPEFirmsShares', 'VCPEFirmsPercent',
    'GeneralPublicShares', 'GeneralPublicPercent'
]

insiders_header = [
    'Ticker', 'HTML Creation Date', 'FilingDate', 'OwnerName', 'OwnerType',
    'TransactionType', 'Shares', 'PriceMax', 'TransactionValue'
]

# Create/open CSVs
csv_shareholders = open(OUT_SHAREHOLDERS, mode='w', newline='', encoding='utf-8')
csv_ownership = open(OUT_OWNERSHIP, mode='w', newline='', encoding='utf-8')
csv_insiders = open(OUT_INSIDERS, mode='w', newline='', encoding='utf-8')

writer_sh = csv.writer(csv_shareholders)
writer_own = csv.writer(csv_ownership)
writer_ins = csv.writer(csv_insiders)

writer_sh.writerow(shareholders_header)
writer_own.writerow(ownership_header)
writer_ins.writerow(insiders_header)

# File processing loop
filenames = [f for f in os.listdir(HTML_FOLDER) if f.endswith('.html')]
total_files = len(filenames)
processed = 0
skipped = 0
errors = 0

for idx, filename in enumerate(sorted(filenames), start=1):
    filepath = os.path.join(HTML_FOLDER, filename)
    ticker = os.path.splitext(filename)[0]
    creation_ts = None
    try:
        creation_ts = os.path.getctime(filepath)
    except Exception:
        creation_ts = None
    creation_date_iso = ''
    if creation_ts:
        creation_date_iso = datetime.fromtimestamp(creation_ts, tz=timezone.utc).strftime('%Y-%m-%d')

    print(f"\nProcessing {ticker} ({idx}/{total_files})...")

    try:
        with open(filepath, 'r', encoding='utf-8') as fh:
            text = fh.read()
    except Exception as e:
        print(f"  ERROR reading file: {e}")
        skipped += 1
        continue

    soup = BeautifulSoup(text, 'html.parser')
    scripts = soup.find_all('script')
    if not scripts:
        print("  No <script> tags found, skipping.")
        skipped += 1
        continue

    # try to find the script(s) that contain the keys we need
    combined_script_text = None
    for s in scripts:
        # prefer s.string if available, else s.text
        script_text = s.string if s.string is not None else s.text
        if not script_text:
            continue
        # if it contains at least one of our keys, prefer it
        if ('topShareholders' in script_text) or ('ownershipBreakdown' in script_text) or ('insiderTransactionsMap' in script_text):
            combined_script_text = script_text
            break
    # fallback: join all scripts
    if combined_script_text is None:
        combined_script_text = " ".join([ (s.string if s.string is not None else s.text) or "" for s in scripts ])

    # clean
    cleaned_script = clean_json_like_text(combined_script_text)

    # ---------- topShareholders extraction ----------
    shareholders_extracted = 0
    try:
        sh_block = extract_json_block_from_script(cleaned_script, 'topShareholders')
        json_topshareholders = None
        if sh_block:
            # ensure valid JSON
            sh_block = clean_json_like_text(sh_block)
            try:
                json_topshareholders = json.loads(sh_block)
            except json.JSONDecodeError as e:
                # last-ditch attempt: try to wrap in {} if it looks like inner object
                try:
                    json_topshareholders = json.loads('{' + sh_block + '}')
                except Exception:
                    json_topshareholders = None
                    print(f"  topShareholders JSON decode error (ticker {ticker}): {e}")
        else:
            # not found
            json_topshareholders = None

        if json_topshareholders:
            # flatten
            flattened = []
            for outer in json_topshareholders.values():
                if not isinstance(outer, dict):
                    continue
                for shareholder in outer.values():
                    owner = shareholder.get('owner', {}) or {}
                    shares_held = safe_int(shareholder.get('sharesHeld'))
                    percent_out = safe_float(shareholder.get('percentOfSharesOutstanding'))
                    percent_port = safe_float(shareholder.get('percentOfPortfolio'))
                    holding_date = shareholder.get('holdingDate') or ''
                    # if periodStartDate / end present, convert if desired (not required here)
                    flattened.append({
                        'owner_name': owner.get('name', ''),
                        'owner_type': owner.get('type', ''),
                        'shares_held': shares_held,
                        'percent_shares_outstanding': percent_out,
                        'percent_portfolio': percent_port,
                        'holding_date': holding_date
                    })
            # sort by percent outstanding desc
            flattened.sort(key=lambda x: x['percent_shares_outstanding'], reverse=True)
            for sh in flattened:
                writer_sh.writerow([
                    ticker,
                    creation_date_iso,
                    sh['owner_name'],
                    sh['owner_type'],
                    sh['shares_held'],
                    sh['percent_shares_outstanding'],
                    sh['percent_portfolio'],
                    sh['holding_date']
                ])
            shareholders_extracted = len(flattened)
        else:
            print("  topShareholders not found or failed to parse.")
    except Exception as e:
        print(f"  ERROR extracting topShareholders: {e}")
        errors += 1

    # ---------- ownershipBreakdown extraction ----------
    try:
        ob_block = extract_json_block_from_script(cleaned_script, 'ownershipBreakdown')
        if ob_block:
            ob_block = clean_json_like_text(ob_block)
            try:
                ob_json = json.loads(ob_block)
            except json.JSONDecodeError:
                # try simpler recovery by wrapping braces
                try:
                    ob_json = json.loads('{' + ob_block + '}')
                except Exception:
                    ob_json = None
            if ob_json and isinstance(ob_json, dict):
                # read keys (some keys have slashes or spaces)
                inst = safe_int(ob_json.get('Institutions'))
                pubco = safe_int(ob_json.get('Public Companies') or ob_json.get('PublicCompanies'))
                privco = safe_int(ob_json.get('Private Companies') or ob_json.get('PrivateCompanies'))
                indiv = safe_int(ob_json.get('Individual Insiders') or ob_json.get('IndividualInsiders'))
                vcpe = safe_int(ob_json.get('VC/PE Firms') or ob_json.get('VCPEFirms') or ob_json.get('VC\\\\/PE Firms'))
                genpub = safe_int(ob_json.get('General Public') or ob_json.get('GeneralPublic'))

                totalsum = inst + (pubco or 0) + (privco or 0) + (indiv or 0) + (vcpe or 0) + (genpub or 0)
                # avoid division by zero
                def pct(val):
                    return round((val / totalsum) * 100, 4) if totalsum > 0 else 0.0

                writer_own.writerow([
                    ticker,
                    creation_date_iso,
                    inst, pct(inst),
                    pubco or 0, pct(pubco or 0),
                    privco or 0, pct(privco or 0),
                    indiv or 0, pct(indiv or 0),
                    vcpe or 0, pct(vcpe or 0),
                    genpub or 0, pct(genpub or 0)
                ])
            else:
                # write empty row with zeros if not present
                writer_own.writerow([ticker, creation_date_iso] + [0,0]*6)
                print("  ownershipBreakdown not found as a dict; wrote zeros row.")
        else:
            writer_own.writerow([ticker, creation_date_iso] + [0,0]*6)
            print("  ownershipBreakdown block not found; wrote zeros row.")
    except Exception as e:
        print(f"  ERROR extracting ownershipBreakdown: {e}")
        errors += 1

    # ---------- insiderTransactionsMap extraction ----------
    insiders_extracted = 0
    try:
        it_block = extract_json_block_from_script(cleaned_script, 'insiderTransactionsMap')
        if it_block:
            it_block = clean_json_like_text(it_block)
            try:
                it_json = json.loads(it_block)
            except json.JSONDecodeError:
                # try wrapping braces
                try:
                    it_json = json.loads('{' + it_block + '}')
                except Exception:
                    it_json = None
            if it_json and isinstance(it_json, dict):
                for tx_id, tx in it_json.items():
                    filing = tx.get('filingDate')
                    filing_iso = epochms_to_iso(filing)
                    owner_name = tx.get('ownerName') or tx.get('ownername') or ''
                    owner_type = tx.get('ownerType') or ''
                    transaction_type = tx.get('transactionType') or ''
                    shares = safe_int(tx.get('shares'))
                    price_max = safe_float(tx.get('priceMax'))
                    tx_value = safe_float(tx.get('transactionValue') or tx.get('transactionValue') or tx.get('transaction_value') or tx.get('transactionValue'))
                    writer_ins.writerow([
                        ticker,
                        creation_date_iso,
                        filing_iso,
                        owner_name,
                        owner_type,
                        transaction_type,
                        shares,
                        price_max,
                        tx_value
                    ])
                    insiders_extracted += 1
            else:
                # no insider map present
                # do nothing (no rows)
                pass
        else:
            # not found; do nothing
            pass
    except Exception as e:
        print(f"  ERROR extracting insiderTransactionsMap: {e}")
        errors += 1

    print(f"  Extracted {shareholders_extracted} shareholders, {insiders_extracted} insider txns.")
    processed += 1

# close files
csv_shareholders.close()
csv_ownership.close()
csv_insiders.close()

print("\n=== Summary ===")
print(f"Total files discovered: {total_files}")
print(f"Processed: {processed}")
print(f"Skipped: {skipped}")
print(f"Errors encountered: {errors}")
print(f"Outputs written:\n - {OUT_SHAREHOLDERS}\n - {OUT_OWNERSHIP}\n - {OUT_INSIDERS}")
