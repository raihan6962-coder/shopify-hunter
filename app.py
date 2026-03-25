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
# SOURCE 1: crt.sh — SSL Certificate Transparency Logs
# Every new Shopify store gets an SSL cert → appears here within hours
# FREE, no API key, no rate limit, works perfectly from Render
# ─────────────────────────────────────────────────────────────────────────────
def source_crtsh(keyword):
    """
    crt.sh SSL Certificate Transparency Logs.
    Filter 1: Only certs issued in last 7 days (brand new stores).
    Filter 2: Keyword must appear in the store subdomain name.
    This ensures we get ONLY new, niche-relevant stores.
    """
    urls = set()
    log(f"   [crt.sh] New SSL certs (≤7 days) for '{keyword}'...", "INFO")

    kw_lower  = keyword.lower()
    kw_parts  = kw_lower.split()
    kw_joined = kw_lower.replace(' ', '')

    from datetime import timezone
    now = datetime.utcnow()

    try:
        r = requests.get(
            "https://crt.sh/",
            params={'q': '%.myshopify.com', 'output': 'json'},
            timeout=40,
            headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
        )
        if r.status_code != 200:
            log(f"   crt.sh HTTP {r.status_code}", "WARN")
            return urls

        certs = r.json()
        log(f"   crt.sh: {len(certs)} total certs — filtering...", "INFO")

        total_new = 0
        keyword_matched = 0

        for cert in certs:
            # ── FILTER 1: Only last 7 days ────────────────────────────────
            not_before = cert.get('not_before', '')
            if not_before:
                try:
                    cert_dt = datetime.strptime(not_before, '%Y-%m-%dT%H:%M:%S')
                    days_old = (now - cert_dt).days
                    if days_old > 7:
                        continue  # older than 7 days → skip
                    total_new += 1
                except:
                    pass  # can't parse → include

            # ── FILTER 2: Keyword in subdomain ────────────────────────────
            raw_name = cert.get('common_name', '') or cert.get('name_value', '')
            subdomain = raw_name.lower().replace('.myshopify.com', '').replace('*.', '').strip()

            matched = (
                kw_joined in subdomain or
                kw_lower  in subdomain or
                all(p in subdomain for p in kw_parts) or
                any(p in subdomain for p in kw_parts if len(p) >= 4)
            )
            if not matched:
                continue

            keyword_matched += 1
            m = MYSHOPIFY_RE.search(raw_name)
            if m:
                urls.add(f"https://{m.group(1)}.myshopify.com")

        log(f"   crt.sh: {total_new} certs ≤7 days | {keyword_matched} keyword-matched | {len(urls)} unique stores", "INFO")

    except Exception as e:
        log(f"   crt.sh error: {e}", "WARN")

    return urls

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: CommonCrawl — large free dataset
# ─────────────────────────────────────────────────────────────────────────────
def source_commoncrawl(keyword):
    urls = set()
    log(f"   [CommonCrawl] '{keyword}'...", "INFO")
    indexes = ['CC-MAIN-2025-08', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46']
    for idx in indexes:
        if len(urls) >= 300: break
        for kv in [keyword.replace(' ', '-'), keyword.replace(' ', ''), keyword]:
            try:
                r = requests.get(
                    f"https://index.commoncrawl.org/{idx}-index",
                    params={'url': f'*.myshopify.com/*{kv}*',
                            'output': 'json', 'limit': 500, 'fl': 'url'},
                    timeout=20)
                if r.status_code == 200 and r.text.strip():
                    for line in r.text.strip().split('\n'):
                        try:
                            m = MYSHOPIFY_RE.search(json.loads(line).get('url', ''))
                            if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                        except: continue
            except Exception as e:
                pass
            time.sleep(0.5)
    log(f"   CommonCrawl: {len(urls)} stores", "INFO")
    return urls

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: URLScan.io — recently scanned sites
# ─────────────────────────────────────────────────────────────────────────────
def source_urlscan(keyword):
    urls = set()
    log(f"   [URLScan] '{keyword}'...", "INFO")
    for q in [
        f"domain:myshopify.com AND page.title:{keyword.replace(' ', '+')}",
        f"domain:myshopify.com AND page.body:{keyword.replace(' ', '+')}",
        f"domain:myshopify.com AND page.domain:*{keyword.replace(' ', '-')}*",
    ]:
        try:
            r = requests.get("https://urlscan.io/api/v1/search/",
                params={'q': q, 'size': 100, 'sort': 'time'},
                timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                for result in r.json().get('results', []):
                    page_url = result.get('page', {}).get('url', '')
                    m = MYSHOPIFY_RE.search(page_url)
                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            pass
        time.sleep(0.5)
    log(f"   URLScan: {len(urls)} stores", "INFO")
    return urls

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4: Bing direct scrape
# ─────────────────────────────────────────────────────────────────────────────
def source_bing(keyword, country):
    urls = set()
    log(f"   [Bing] '{keyword}'...", "INFO")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    queries = [
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com "{keyword}" "be the first to know"',
        f'site:myshopify.com "{keyword}"',
        f'site:myshopify.com {keyword} shop',
    ]
    session = requests.Session()
    for q in queries:
        if len(urls) >= 150: break
        for first in [1, 11, 21]:
            try:
                r = session.get('https://www.bing.com/search',
                    params={'q': q, 'first': first, 'count': 10},
                    headers=headers, timeout=15)
                if r.status_code == 200:
                    for m in MYSHOPIFY_RE.finditer(r.text):
                        urls.add(f"https://{m.group(1)}.myshopify.com")
                elif r.status_code == 429:
                    time.sleep(20); break
            except Exception as e:
                pass
            time.sleep(random.uniform(2, 4))
        time.sleep(1)
    log(f"   Bing: {len(urls)} stores", "INFO")
    return urls

# ─────────────────────────────────────────────────────────────────────────────
# MAIN DISCOVERY — keyword-filtered new stores from all sources
# ─────────────────────────────────────────────────────────────────────────────
def find_shopify_stores(keyword, country):
    all_urls = set()

    # SOURCE 1: crt.sh — brand new (≤7 days) + keyword-matched stores
    # This is the most important source — gives truly new stores
    all_urls.update(source_crtsh(keyword))
    log(f"   After crt.sh: {len(all_urls)} stores", "INFO")

    # SOURCE 2: CommonCrawl — keyword-matched URL pages
    all_urls.update(source_commoncrawl(keyword))
    log(f"   After CommonCrawl: {len(all_urls)} stores", "INFO")

    # SOURCE 3: URLScan — recently scanned keyword stores
    all_urls.update(source_urlscan(keyword))
    log(f"   After URLScan: {len(all_urls)} stores", "INFO")

    # SOURCE 4: Bing scrape — new store signal queries
    all_urls.update(source_bing(keyword, country))
    log(f"   After Bing: {len(all_urls)} stores", "INFO")

    total = list(all_urls)
    random.shuffle(total)
    log(f"📦 Total: {len(total)} keyword-matched stores to test", "INFO")
    return total


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT DETECTION — checkout HTML analysis with detailed debug logs
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
        html_lower = r.text.lower()
        if 'shopify' not in html_lower and 'cdn.shopify.com' not in html_lower:
            return {"is_shopify": False, "is_lead": False}

        is_password = '/password' in r.url or 'password-page' in html_lower

        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}

            products = prod_req.json().get('products', [])

            if not products:
                chk = session.get(f"{base_url}/checkout", headers=headers, timeout=12, allow_redirects=True)
                chk_lower = chk.text.lower()
                for phrase in ["isn't accepting payments", "not accepting payments",
                                "no payment methods", "payment provider hasn't been set up",
                                "this store is unavailable", "unable to process payment"]:
                    if phrase in chk_lower:
                        return {"is_shopify": True, "is_lead": True, "reason": f"0 products + '{phrase}'"}
                if is_password:
                    return {"is_shopify": True, "is_lead": True, "reason": "Password + 0 products = new store"}
                return {"is_shopify": True, "is_lead": False, "reason": "0 products, unclear"}

            variant_id = products[0]['variants'][0]['id']
            session.post(f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'}, timeout=10)

            chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15, allow_redirects=True)
            chk_html = chk_req.text
            chk_lower = chk_html.lower()

            # Explicit no-payment = CONFIRMED LEAD
            for phrase in ["isn't accepting payments", "not accepting payments",
                           "no payment methods", "payment provider hasn't been set up",
                           "this store is unavailable"]:
                if phrase in chk_lower:
                    return {"is_shopify": True, "is_lead": True, "reason": f"CONFIRMED: '{phrase}'"}

            # Payment keywords = HAS payment
            payment_kws = ['visa', 'mastercard', 'amex', 'american express',
                'paypal', 'credit card', 'debit card', 'card number',
                'stripe', 'klarna', 'afterpay', 'shop pay', 'shoppay',
                'apple pay', 'google pay', 'discover', 'diners',
                'card-fields', 'payment-method', 'pay with']
            found_pay = [kw for kw in payment_kws if kw in chk_lower]
            if found_pay:
                return {"is_shopify": True, "is_lead": False, "reason": f"has: {found_pay[:2]}"}

            # Redirected away from checkout
            if base_url.replace('https://', '') in chk_req.url and '/checkout' not in chk_req.url:
                return {"is_shopify": True, "is_lead": True, "reason": "Redirected from checkout = no payment"}

            # Reached checkout, no payment found = LEAD
            if any(s in chk_lower for s in ['contact information', 'shipping address',
                                              'order summary', 'express checkout', 'your email']):
                return {"is_shopify": True, "is_lead": True, "reason": "Checkout OK, no payment options"}

            return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive"}

        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"error: {e}"}
    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL + PHONE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
              '.svg', 'noreply', 'domain.com', 'no-reply', 'schema.org', 'w3.org']
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
                    for sfx in [' – Shopify', ' | Shopify', ' - Powered by Shopify', ' – Online Store']:
                        name = name.replace(sfx, '')
                    result['store_name'] = name.strip()[:80]
            if not result['email']:
                e = extract_email(html, soup)
                if e:
                    result['email'] = e
                    log(f"   📧 '{path or '/'}': {e}", "INFO")
            if not result['phone']:
                result['phone'] = extract_phone(html)
        except: continue

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
Problem: NO payment gateway configured.
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, 1 soft CTA, HTML <p> tags
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
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
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key  = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

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
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — HUNTING NEW NO-PAYMENT STORES", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Sources: crt.sh + CommonCrawl + URLScan + Bing", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = rej_pay = rej_other = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country)
        except Exception as e:
            log(f"Search failed: {e}", "WARN"); store_urls = []

        if not store_urls:
            log("⚠️  No URLs found for this keyword. Moving to next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores for payment gateways...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                result = check_store_target(url, session)
                if not result.get("is_shopify"): continue

                if not result.get("is_lead"):
                    reason = result.get('reason', '')
                    if any(w in reason.lower() for w in ['has:', 'payment', 'visa', 'stripe']):
                        rej_pay += 1
                    else:
                        rej_other += 1
                    # Show reason for first 5 to debug
                    if idx < 5:
                        log(f"   [{idx+1}] SKIP ({reason}) — {url}", "WARN")
                    elif (idx + 1) % 10 == 0:
                        log(f"   Progress: [{idx+1}/{len(store_urls)}] leads:{kw_leads} paid:{rej_pay} other:{rej_other}", "INFO")
                    time.sleep(0.5)
                    continue

                # ✅ LEAD!
                log(f"   🎯 100% MATCH: {result.get('reason')} — collecting info...", "SUCCESS")
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
                log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_str} {phone_str}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                log(f"   Error: {e}", "WARN"); continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(10)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email',''))
                  and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    if not pending:
        log("⚠️  No leads with emails found — check your collected leads", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to,
            'subject': subject, 'body': body, 'lead_id': lead.get('id', '')
        })
        if send_resp.get('status') == 'ok':
            log(f"   ✅ Email sent to {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Send failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ Waiting {delay}s before next email...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check your Google Sheet for leads.", "SUCCESS")
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
