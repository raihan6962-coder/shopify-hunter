from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import os

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=30,
                              headers={'Content-Type': 'application/json'})
            try:
                return r.json()
            except:
                time.sleep(2); continue
        except requests.exceptions.Timeout:
            log(f"Sheet timeout ({attempt+1}/3)", "WARN"); time.sleep(2)
        except Exception as e:
            log(f"Sheet error ({attempt+1}/3): {e}", "WARN"); time.sleep(2)
    return {'error': 'Sheet API failed'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY — No Google, No API key
# Sources: CommonCrawl + URLScan + Bing (direct scrape) + Sitemap crawl
# ─────────────────────────────────────────────────────────────────────────────

def source_commoncrawl(keyword):
    """CommonCrawl index — huge free dataset, no API key."""
    urls = set()
    log(f"   [CommonCrawl] searching '{keyword}'...", "INFO")
    # Use multiple recent crawl indexes
    indexes = ['CC-MAIN-2025-08', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46']
    for idx in indexes:
        if len(urls) >= 150:
            break
        try:
            # Search for myshopify.com pages containing keyword
            r = requests.get(
                f"https://index.commoncrawl.org/{idx}-index",
                params={
                    'url': '*.myshopify.com/*',
                    'output': 'json',
                    'limit': 500,
                    'fl': 'url',
                    'filter': f'=url:.*{keyword.replace(" ","-")}.*',
                },
                timeout=20
            )
            if r.status_code == 200 and r.text.strip():
                for line in r.text.strip().split('\n'):
                    try:
                        m = MYSHOPIFY_RE.search(json.loads(line).get('url', ''))
                        if m:
                            urls.add(f"https://{m.group(1)}.myshopify.com")
                    except: continue
            # Also search without filter for broad coverage
            r2 = requests.get(
                f"https://index.commoncrawl.org/{idx}-index",
                params={
                    'url': f'*.myshopify.com/*{keyword.replace(" ", "*")}*',
                    'output': 'json',
                    'limit': 300,
                    'fl': 'url',
                },
                timeout=20
            )
            if r2.status_code == 200 and r2.text.strip():
                for line in r2.text.strip().split('\n'):
                    try:
                        m = MYSHOPIFY_RE.search(json.loads(line).get('url', ''))
                        if m:
                            urls.add(f"https://{m.group(1)}.myshopify.com")
                    except: continue
        except Exception as e:
            log(f"   CommonCrawl {idx} error: {e}", "WARN")
        time.sleep(1)
    log(f"   CommonCrawl: {len(urls)} stores", "INFO")
    return urls

def source_urlscan(keyword):
    """URLScan.io — indexes recently scanned sites, free, no API key."""
    urls = set()
    log(f"   [URLScan] searching '{keyword}'...", "INFO")
    queries = [
        f"domain:myshopify.com AND page.title:{keyword.replace(' ', '+')}",
        f"domain:myshopify.com AND page.body:{keyword.replace(' ', '+')}",
        f"domain:myshopify.com AND page.url:*{keyword.replace(' ', '-')}*",
    ]
    for q in queries:
        try:
            r = requests.get(
                "https://urlscan.io/api/v1/search/",
                params={'q': q, 'size': 100, 'sort': 'time'},
                timeout=15, headers={'User-Agent': 'Mozilla/5.0'}
            )
            if r.status_code == 200:
                for result in r.json().get('results', []):
                    page_url = result.get('page', {}).get('url', '')
                    m = MYSHOPIFY_RE.search(page_url)
                    if m:
                        urls.add(f"https://{m.group(1)}.myshopify.com")
            time.sleep(0.5)
        except Exception as e:
            log(f"   URLScan error: {e}", "WARN")
    log(f"   URLScan: {len(urls)} stores", "INFO")
    return urls

def source_bing_scrape(keyword, country):
    """
    Scrape Bing search results directly — no API key.
    Bing doesn't block Render IPs like Google does.
    """
    urls = set()
    log(f"   [Bing Scrape] '{keyword}' {country}...", "INFO")
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    queries = [
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com "{keyword}" "be the first to know"',
        f'site:myshopify.com "{keyword}" {country}',
        f'site:myshopify.com {keyword} shop {country}',
        f'site:myshopify.com "{keyword}" "powered by shopify"',
    ]
    session = requests.Session()
    for q in queries:
        if len(urls) >= 200:
            break
        # Bing pages: first=1, first=11, first=21...
        for first in [1, 11, 21]:
            try:
                r = session.get(
                    'https://www.bing.com/search',
                    params={'q': q, 'first': first, 'count': 10},
                    headers=headers, timeout=15
                )
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, 'html.parser')
                    # Extract all links from results
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        m = MYSHOPIFY_RE.search(href)
                        if m:
                            urls.add(f"https://{m.group(1)}.myshopify.com")
                    # Also raw HTML scan
                    for m in MYSHOPIFY_RE.finditer(r.text):
                        urls.add(f"https://{m.group(1)}.myshopify.com")
                elif r.status_code == 429:
                    log(f"   Bing rate limit — waiting 15s", "WARN")
                    time.sleep(15)
                    break
            except Exception as e:
                log(f"   Bing error: {e}", "WARN")
            time.sleep(random.uniform(2, 4))
        time.sleep(1)
    log(f"   Bing: {len(urls)} stores", "INFO")
    return urls

def source_yahoo_scrape(keyword, country):
    """Scrape Yahoo search — different IP treatment than Google/Bing."""
    urls = set()
    log(f"   [Yahoo] '{keyword}'...", "INFO")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0',
        'Accept': 'text/html,*/*;q=0.8',
    }
    queries = [
        f'site:myshopify.com "{keyword}" {country}',
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com {keyword}',
    ]
    for q in queries:
        if len(urls) >= 100:
            break
        try:
            r = requests.get(
                'https://search.yahoo.com/search',
                params={'p': q, 'n': 10},
                headers=headers, timeout=15
            )
            if r.status_code == 200:
                for m in MYSHOPIFY_RE.finditer(r.text):
                    urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   Yahoo error: {e}", "WARN")
        time.sleep(random.uniform(2, 3))
    log(f"   Yahoo: {len(urls)} stores", "INFO")
    return urls

def source_duckduckgo_html(keyword, country):
    """
    Scrape DuckDuckGo HTML endpoint directly (not the API).
    Different from duckduckgo-search library — direct HTTP scrape.
    """
    urls = set()
    log(f"   [DDG HTML] '{keyword}'...", "INFO")
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    queries = [
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}" {country}',
    ]
    session = requests.Session()
    for q in queries:
        if len(urls) >= 100:
            break
        try:
            # DDG HTML search endpoint
            r = session.get(
                'https://html.duckduckgo.com/html/',
                params={'q': q},
                headers=headers,
                timeout=15
            )
            if r.status_code == 200:
                for m in MYSHOPIFY_RE.finditer(r.text):
                    urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   DDG HTML error: {e}", "WARN")
        time.sleep(random.uniform(3, 5))
    log(f"   DDG HTML: {len(urls)} stores", "INFO")
    return urls

def source_ask_scrape(keyword):
    """Scrape Ask.com — rarely blocked."""
    urls = set()
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
        r = requests.get(
            'https://www.ask.com/web',
            params={'q': f'site:myshopify.com "{keyword}"'},
            headers=headers, timeout=15
        )
        if r.status_code == 200:
            for m in MYSHOPIFY_RE.finditer(r.text):
                urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    return urls

def find_shopify_stores(keyword, country, serpapi_key=None):
    """
    Collect myshopify.com URLs from 6 sources — no Google API needed.
    """
    all_urls = set()

    # Run all sources
    all_urls.update(source_commoncrawl(keyword))
    all_urls.update(source_urlscan(keyword))
    all_urls.update(source_bing_scrape(keyword, country))
    all_urls.update(source_yahoo_scrape(keyword, country))
    all_urls.update(source_duckduckgo_html(keyword, country))
    all_urls.update(source_ask_scrape(keyword))

    total = list(all_urls)
    random.shuffle(total)
    log(f"📦 Total: {len(total)} unique stores from all sources", "INFO")
    return total


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT DETECTION — Checkout HTML analysis
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password protected"}
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}
            prod_data = prod_req.json()
            if not prod_data.get('products'):
                chk = session.get(f"{base_url}/checkout", headers=headers, timeout=12)
                chk_lower = chk.text.lower()
                for msg in ["isn't accepting payments", "not accepting payments",
                            "no payment methods", "payment provider hasn't been set up"]:
                    if msg in chk_lower:
                        return {"is_shopify": True, "is_lead": True, "reason": "0 products + no payment"}
                return {"is_shopify": True, "is_lead": False, "reason": "0 products, unclear"}

            variant_id = prod_data['products'][0]['variants'][0]['id']
            session.post(f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'}, timeout=10)
            chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15, allow_redirects=True)
            chk_html = chk_req.text.lower()

            if ('checkout' not in chk_html and 'contact information' not in chk_html
                    and "isn't accepting payments" not in chk_html):
                return {"is_shopify": True, "is_lead": False, "reason": "Could not reach checkout"}

            payment_keywords = ['visa', 'mastercard', 'amex', 'paypal', 'credit card',
                'debit card', 'card number', 'stripe', 'klarna', 'afterpay',
                'shop pay', 'apple pay', 'google pay', 'discover', 'diners club']
            found = [pk for pk in payment_keywords if pk in chk_html]
            if found:
                return {"is_shopify": True, "is_lead": False, "reason": f"Has payment ('{found[0]}')"}
            return {"is_shopify": True, "is_lead": True, "reason": "No payment methods in checkout HTML"}
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"Checkout error: {e}"}
    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL + PHONE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
              '.svg', 'noreply', 'domain.com', 'no-reply', 'schema.org']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def is_valid_email(email):
    e = email.lower()
    if any(s in e for s in SKIP_EMAIL): return False
    parts = e.split('@')
    if len(parts) != 2 or not parts[0] or '.' not in parts[1]: return False
    return 2 <= len(parts[1].split('.')[-1]) <= 6

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if is_valid_email(e): return e
    for match in EMAIL_RE.findall(html):
        if is_valid_email(match): return match.lower()
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    result = {'store_name': base_url.replace('https://', '').split('.')[0],
               'email': None, 'phone': None}
    pages = ['', '/pages/contact', '/pages/contact-us', '/contact',
             '/pages/about-us', '/pages/about', '/pages/faq',
             '/pages/help', '/pages/support',
             '/policies/contact-information', '/policies/refund-policy']
    for path in pages:
        if result['email'] and result['phone']: break
        try:
            r = session.get(base_url + path, headers=headers, timeout=10)
            if r.status_code != 200: continue
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            if path == '':
                title = soup.find('title')
                if title:
                    name = title.text.strip()
                    for sfx in [' – Shopify', ' | Shopify', ' - Powered by Shopify']:
                        name = name.replace(sfx, '')
                    result['store_name'] = name.strip()[:80]
            if not result['email']:
                e = extract_email(html, soup)
                if e:
                    result['email'] = e
                    log(f"   📧 Email on '{path or '/'}': {e}", "INFO")
            if not result['phone']:
                result['phone'] = extract_phone(html)
        except: continue

    # JSON-LD
    if not result['email']:
        try:
            r = session.get(base_url, headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '{}')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        e = item.get('email', '') or item.get('contactPoint', {}).get('email', '')
                        if e and is_valid_email(e):
                            result['email'] = e.lower()
                            log(f"   📧 JSON-LD: {e}", "INFO")
                            break
                except: continue
        except: pass
    return result


# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: NO payment gateway — cannot accept payments.
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, 1 soft CTA, HTML <p> tags
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        headers_h = {"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"}
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers=headers_h,
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 500, "temperature": 0.7},
            timeout=20)
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            raw = raw.replace('\n', ' ').replace('\r', '')
            data = json.loads(raw, strict=False)
            return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq fallback: {e}", "WARN")
    return tpl_subject, f'<p>{tpl_body}</p>'


# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL: {e}", "ERROR")
        log(traceback.format_exc()[:600], "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running
    log("📋 Loading config...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script error: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return

    log(f"✅ Config OK | Target: {min_leads} leads", "INFO")

    kw_resp   = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    tpl_resp  = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"📧 Template: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — HUNTING NO-PAYMENT STORES", "SUCCESS")
    log(f"🎯 Target: {min_leads} | Sources: CommonCrawl+URLScan+Bing+Yahoo+DDG+Ask", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = rej_pay = rej_other = 0

        log(f"\n🎯 [{keyword}] [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search failed: {e}", "WARN"); store_urls = []

        if not store_urls:
            log("⚠️  No URLs found from any source", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Testing {len(store_urls)} stores...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break
            try:
                result = check_store_target(url, session)
                if not result.get("is_shopify"): continue
                if not result.get("is_lead"):
                    reason = result.get('reason', '')
                    if any(w in reason.lower() for w in ['payment', 'visa', 'stripe', 'paypal']):
                        rej_pay += 1
                    else:
                        rej_other += 1
                    if (idx + 1) % 10 == 0:
                        log(f"   [{idx+1}/{len(store_urls)}] leads:{kw_leads} paid:{rej_pay} other:{rej_other}", "INFO")
                    time.sleep(0.5)
                    continue

                log(f"   🎯 {result.get('reason')}", "SUCCESS")
                info = get_store_info(url, session)
                save_resp = call_sheet({
                    'action': 'save_lead', 'store_name': info['store_name'],
                    'url': url, 'email': info['email'] or '',
                    'phone': info['phone'] or '', 'country': country, 'keyword': keyword
                })
                if save_resp.get('error'):
                    log(f"   Sheet error: {save_resp['error']}", "WARN"); continue
                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️  Duplicate", "INFO"); continue

                total_leads += 1; kw_leads += 1
                email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                phone_str = f"| 📞 {info['phone']}" if info['phone'] else ""
                log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {email_str} {phone_str}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                log(f"   Error: {e}", "WARN"); continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"\n✅ '{keyword}' — leads:{kw_leads} paid:{rej_pay} other:{rej_other}", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(10)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email', ''))
                  and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to,
            'subject': subject, 'body': body, 'lead_id': lead.get('id', '')
        })
        if send_resp.get('status') == 'ok':
            log(f"   ✅ Sent → {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ Next in {delay}s...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check Google Sheet.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")


# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr = call_sheet({'action': 'get_leads'})
            if not lr.get('error'):
                leads = lr.get('leads', [])
                total_leads = len(leads)
                emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            if not kr.get('error'):
                kws = kr.get('keywords', [])
                kw_total = len(kws)
                kw_used  = sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads,
                    'emails_sent': emails_sent, 'kw_total': kw_total,
                    'kw_used': kw_used, 'script_connected': bool(script_url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try:
                msg = log_queue.get(timeout=25)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL', ''):
        return jsonify({'error': 'APPS_SCRIPT_URL not set'})
    return jsonify(call_sheet(request.json))

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if automation_running:
        return jsonify({'status': 'already_running'})
    automation_thread = threading.Thread(target=run_automation, daemon=True)
    automation_thread.start()
    return jsonify({'status': 'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    log("⛔ Stopped by user", "WARN")
    return jsonify({'status': 'stopped'})

@app.route('/api/schedule', methods=['POST'])
def api_schedule():
    data = request.json
    try:
        run_time = datetime.fromisoformat(data.get('time', ''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time, id='scheduled_run', replace_existing=True
        )
        log(f"📅 Scheduled for {data['time']}", "INFO")
        return jsonify({'status': 'scheduled', 'time': data['time']})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
