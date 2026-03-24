from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
from bs4 import BeautifulSoup
from groq import Groq
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
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except requests.exceptions.Timeout:
            log(f"Sheet timeout (attempt {attempt+1}/3)", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet error (attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(3)
    return {'error': 'Sheet API failed after 3 retries'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────
def find_new_shopify_stores(keyword, country, serpapi_key):
    all_urls = set()

    # URLScan
    log(f"   [URLScan] scanning...", "INFO")
    for uq in [
        f"page.domain:myshopify.com AND page.title:{keyword.replace(' ', '+')}",
        f"page.domain:myshopify.com AND page.body:{keyword.replace(' ', '+')}",
    ]:
        try:
            r = requests.get("https://urlscan.io/api/v1/search/",
                params={'q': uq, 'size': 100, 'sort': 'time'},
                timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                for result in r.json().get('results', []):
                    url = result.get('page', {}).get('url', '')
                    m = MYSHOPIFY_RE.search(url)
                    if m:
                        all_urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   URLScan error: {e}", "WARN")
        time.sleep(0.5)
    log(f"   URLScan: {len(all_urls)} stores", "INFO")

    # SerpAPI
    log(f"   [SerpAPI] multi-range search...", "INFO")
    new_store_queries = [
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com "{keyword}" "be the first to know"',
        f'site:myshopify.com "{keyword}" "launching soon"',
        f'site:myshopify.com "{keyword}" "we\'re getting ready"',
        f'site:myshopify.com "{keyword}" "enter using password"',
        f'site:myshopify.com {keyword} {country} "powered by shopify"',
        f'site:myshopify.com {keyword} {country}',
        f'site:myshopify.com {keyword} shop',
    ]
    time_configs = [
        ('qdr:w',  new_store_queries[:7]),
        ('qdr:m',  new_store_queries),
        ('qdr:m3', new_store_queries[7:]),
    ]
    for tbs, queries in time_configs:
        if len(all_urls) >= 300:
            break
        for q in queries:
            if len(all_urls) >= 300:
                break
            try:
                res = requests.get('https://serpapi.com/search', params={
                    'api_key': serpapi_key, 'engine': 'google',
                    'q': q, 'num': 100, 'tbs': tbs, 'gl': 'us', 'hl': 'en',
                }, timeout=20)
                if res.status_code == 200:
                    data = res.json()
                    if not data.get('error'):
                        before = len(all_urls)
                        for item in data.get('organic_results', []):
                            m = MYSHOPIFY_RE.match(item.get('link', ''))
                            if m:
                                all_urls.add(f"https://{m.group(1)}.myshopify.com")
                        new = len(all_urls) - before
                        if new > 0:
                            log(f"   +{new} [{tbs}]: {q[:55]}...", "INFO")
            except Exception as e:
                log(f"   SerpAPI error: {e}", "WARN")
            time.sleep(1.2)
    log(f"   SerpAPI total: {len(all_urls)} stores", "INFO")

    # CommonCrawl
    log(f"   [CommonCrawl] searching...", "INFO")
    cc_before = len(all_urls)
    for cc_idx in ['CC-MAIN-2024-51', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42']:
        if len(all_urls) >= 350:
            break
        try:
            r = requests.get(f"https://index.commoncrawl.org/{cc_idx}-index", params={
                'url': f'*.myshopify.com/*', 'output': 'json', 'limit': 500,
                'fl': 'url', 'filter': f'=url:.*{keyword.replace(" ", ".*")}.*',
            }, timeout=20)
            if r.status_code == 200:
                for line in r.text.strip().split('\n'):
                    try:
                        m = MYSHOPIFY_RE.search(json.loads(line).get('url', ''))
                        if m:
                            all_urls.add(f"https://{m.group(1)}.myshopify.com")
                    except: continue
        except Exception as e:
            log(f"   CommonCrawl error: {e}", "WARN")
        time.sleep(0.5)
    log(f"   CommonCrawl: +{len(all_urls)-cc_before} stores", "INFO")

    # Bing
    log(f"   [Bing] searching...", "INFO")
    bing_before = len(all_urls)
    for bq in [
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com {keyword} {country}',
        f'site:myshopify.com {keyword}',
    ]:
        if len(all_urls) >= 400:
            break
        try:
            res = requests.get('https://serpapi.com/search', params={
                'api_key': serpapi_key, 'engine': 'bing', 'q': bq, 'count': 50,
            }, timeout=20)
            if res.status_code == 200:
                data = res.json()
                if not data.get('error'):
                    for item in data.get('organic_results', []):
                        m = MYSHOPIFY_RE.match(item.get('link', ''))
                        if m:
                            all_urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   Bing error: {e}", "WARN")
        time.sleep(1)
    log(f"   Bing: +{len(all_urls)-bing_before} stores", "INFO")

    total = list(all_urls)
    log(f"📦 Total: {len(total)} unique stores to test", "INFO")
    return total


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT DETECTION — 2-step approach
# Step 1: Check homepage/cart.js for payment SDK (fast)
# Step 2: If unclear, do full checkout test (slower but accurate)
# ─────────────────────────────────────────────────────────────────────────────

# Real payment SDK scripts — definitive proof payment IS configured
PAYMENT_SDK = [
    'js.stripe.com/v3', 'js.stripe.com/v2',
    'paypal.com/sdk/js',
    'cdn.shopify.com/shopifycloud/shop-js',  # Shop Pay
    'pay.shopify.com',
    'js.klarna.com',
    'js.afterpay.com', 'portal.afterpay.com',
    'cdn1.affirm.com',
    'checkout.sezzle.com',
    'js.squareup.com',
    'js.braintreegateway.com',
]

# Explicit messages that payment is NOT set up
NO_PAYMENT_MESSAGES = [
    "this store isn't accepting payments",
    "isn't accepting payments",
    "not accepting payments",
    "no payment methods are available",
    "no payment providers",
    "payment provider hasn't been set up",
    "this store is not accepting orders",
]

def check_store_target(base_url, session):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}

    try:
        # ── Step 1: Homepage quick check ─────────────────────────────────────
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}

        html = r.text
        html_lower = html.lower()

        # Not Shopify at all — skip fast
        if 'shopify' not in html_lower and 'cdn.shopify.com' not in html_lower:
            return {"is_shopify": False, "is_lead": False}

        # Payment SDK found on homepage = definitely has payment
        for sdk in PAYMENT_SDK:
            if sdk in html:
                return {"is_shopify": True, "is_lead": False, "reason": f"SDK: {sdk[:30]}"}

        # ── Step 2: cart.js check (fast, no cart manipulation needed) ────────
        try:
            cj = session.get(f"{base_url}/cart.js", headers=headers, timeout=8)
            if cj.status_code == 200:
                cart_text = cj.text.lower()
                # payment_gateway field in cart = has payment
                if '"payment_gateway"' in cart_text:
                    # Check if it's empty or has value
                    m = re.search(r'"payment_gateway"\s*:\s*"([^"]*)"', cart_text)
                    if m and m.group(1):  # non-empty = has payment
                        return {"is_shopify": True, "is_lead": False,
                                "reason": f"cart.js payment_gateway: {m.group(1)[:20]}"}
        except: pass

        # ── Step 3: Get products for checkout test ────────────────────────────
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1",
                                   headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}

            prod_data = prod_req.json()
            products = prod_data.get('products', [])

            # ── Step 4a: 0 products — check checkout directly ─────────────────
            if not products:
                chk = session.get(f"{base_url}/checkout", headers=headers,
                                  timeout=12, allow_redirects=True)
                chk_lower = chk.text.lower()
                for msg in NO_PAYMENT_MESSAGES:
                    if msg in chk_lower:
                        return {"is_shopify": True, "is_lead": True,
                                "reason": "0 products + no payment message"}
                # Password page with 0 products = brand new store
                if '/password' in r.url or 'password-page' in html_lower:
                    return {"is_shopify": True, "is_lead": True,
                            "reason": "Password protected + 0 products (brand new)"}
                return {"is_shopify": True, "is_lead": False, "reason": "0 products, no clear signal"}

            # ── Step 4b: Add to cart → checkout ──────────────────────────────
            variant_id = products[0]['variants'][0]['id']

            # Add to cart
            session.post(f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'},
                timeout=10)

            # Go to checkout
            chk_req = session.get(f"{base_url}/checkout", headers=headers,
                                   timeout=15, allow_redirects=True)
            chk_html = chk_req.text
            chk_lower = chk_html.lower()

            # Check for payment SDK in checkout page too
            for sdk in PAYMENT_SDK:
                if sdk in chk_html:
                    return {"is_shopify": True, "is_lead": False,
                            "reason": f"Checkout SDK: {sdk[:30]}"}

            # Explicit no-payment message = CONFIRMED LEAD
            for msg in NO_PAYMENT_MESSAGES:
                if msg in chk_lower:
                    return {"is_shopify": True, "is_lead": True,
                            "reason": f"No payment: '{msg[:40]}'"}

            # Payment form elements = has payment
            payment_form_signals = [
                'card-number', 'cardnumber', 'card_number',
                'data-card-fields', 'braintree-hosted-field',
                'stripe-card', 'id="card-', 'name="card_',
                '"payment_method_type"', 'payment-form',
            ]
            for sig in payment_form_signals:
                if sig in chk_lower:
                    return {"is_shopify": True, "is_lead": False,
                            "reason": f"Payment form: {sig}"}

            # Visible payment method names in checkout
            checkout_payment_words = [
                'visa', 'mastercard', 'american express', 'paypal',
                'shop pay', 'apple pay', 'google pay', 'klarna',
                'afterpay', 'affirm', 'pay with card',
            ]
            for word in checkout_payment_words:
                if word in chk_lower:
                    return {"is_shopify": True, "is_lead": False,
                            "reason": f"Payment word: {word}"}

            # If we reached checkout page but found NONE of the above = no payment
            # Verify we actually reached a real checkout (not redirected away)
            checkout_page_signals = [
                'contact information', 'email or mobile',
                'shipping address', 'order summary',
                'express checkout',
            ]
            reached_checkout = any(sig in chk_lower for sig in checkout_page_signals)

            if reached_checkout:
                return {"is_shopify": True, "is_lead": True,
                        "reason": "Checkout reached, no payment options found"}

            return {"is_shopify": True, "is_lead": False, "reason": "Could not confirm checkout"}

        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"Checkout error: {e}"}

    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL + PHONE EXTRACTION — checks 9 pages + JSON-LD
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example.com', 'sentry.io', 'wixpress.com', 'shopify.com',
              'noreply', 'no-reply', 'donotreply', '@2x.', '.png', '.jpg', '.svg',
              'schema.org', 'w3.org', 'domain.com']
PHONE_PATTERNS = [
    re.compile(r'\+\d{1,3}[\s\-\.]?\(?\d{1,4}\)?[\s\-\.]?\d{3,5}[\s\-\.]?\d{3,5}'),
    re.compile(r'\(\d{3}\)\s*\d{3}[\s\-\.]\d{4}'),
    re.compile(r'\b\d{3}[\s\-\.]\d{3}[\s\-\.]\d{4}\b'),
]

def is_valid_email(email):
    e = email.lower().strip()
    if any(s in e for s in SKIP_EMAIL):
        return False
    parts = e.split('@')
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return False
    if '.' not in parts[1]:
        return False
    tld = parts[1].split('.')[-1]
    return 2 <= len(tld) <= 6

def extract_email_from_html(html, soup):
    # Priority 1: mailto links
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if is_valid_email(e):
                return e
    # Priority 2: visible text that looks like email
    for match in EMAIL_RE.findall(html):
        if is_valid_email(match):
            return match.lower()
    return None

def extract_phone_from_html(html):
    for pattern in PHONE_PATTERNS:
        m = pattern.search(html)
        if m:
            return m.group(0).strip()
    return None

def get_store_info(base_url, session):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8'}
    result = {
        'store_name': base_url.replace('https://', '').split('.')[0],
        'email': None, 'phone': None,
    }

    pages = [
        '', '/pages/contact', '/pages/contact-us', '/contact',
        '/pages/about-us', '/pages/about', '/pages/faq',
        '/pages/help', '/pages/support',
    ]

    for path in pages:
        if result['email'] and result['phone']:
            break
        try:
            r = session.get(base_url + path, headers=headers, timeout=10)
            if r.status_code not in [200]:
                continue
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')

            # Store name from homepage
            if path == '':
                title = soup.find('title')
                if title and title.text.strip():
                    name = title.text.strip()
                    for sfx in [' – Shopify', ' | Shopify', ' - Powered by Shopify',
                                 ' – Online Store', ' | Online Store']:
                        name = name.replace(sfx, '')
                    result['store_name'] = name.strip()[:80]

            if not result['email']:
                e = extract_email_from_html(html, soup)
                if e:
                    result['email'] = e
                    log(f"   📧 Email on '{path or '/'}': {e}", "INFO")

            if not result['phone']:
                p = extract_phone_from_html(html)
                if p:
                    result['phone'] = p

        except Exception:
            continue

    # JSON-LD structured data (often has email not visible on page)
    if not result['email']:
        try:
            r = session.get(base_url, headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '{}')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        # Direct email field
                        e = item.get('email', '')
                        if e and is_valid_email(e):
                            result['email'] = e.lower()
                            log(f"   📧 Email from JSON-LD: {e}", "INFO")
                            break
                        # contactPoint
                        cp = item.get('contactPoint', {})
                        if isinstance(cp, dict):
                            e = cp.get('email', '')
                            if e and is_valid_email(e):
                                result['email'] = e.lower()
                                break
                except: continue
        except: pass

    return result


# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: Their store has NO payment gateway — cannot accept payments.
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, helpful tone, 1 soft CTA, HTML <p> tags
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
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
        log(f"❌ Apps Script error: {cfg_resp['error']}", "ERROR")
        log("👉 Set APPS_SCRIPT_URL in Render → Environment", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    if not serpapi_key:
        log("❌ SerpAPI Key missing", "ERROR"); return
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
    session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — HUNTING NEW STORES WITHOUT PAYMENT", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = rej_payment = rej_other = 0

        log(f"\n🎯 [{keyword}] [{country}]", "INFO")

        try:
            store_urls = find_new_shopify_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search failed: {e}", "WARN"); store_urls = []

        if not store_urls:
            log("⚠️  No URLs found", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Testing {len(store_urls)} stores...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                result = check_store_target(url, session)

                if not result.get("is_shopify"):
                    continue

                if not result.get("is_lead"):
                    reason = result.get('reason', '')
                    if any(w in reason.lower() for w in ['sdk', 'payment', 'visa', 'stripe', 'paypal', 'cart.js']):
                        rej_payment += 1
                    else:
                        rej_other += 1
                    if (idx + 1) % 10 == 0:
                        log(f"   [{idx+1}/{len(store_urls)}] leads:{kw_leads} paid:{rej_payment} other:{rej_other}", "INFO")
                    time.sleep(0.5)
                    continue

                # ✅ QUALIFIED LEAD
                log(f"   🎯 MATCH: {result.get('reason')}", "SUCCESS")
                info = get_store_info(url, session)

                save_resp = call_sheet({
                    'action': 'save_lead',
                    'store_name': info['store_name'],
                    'url': url,
                    'email': info['email'] or '',
                    'phone': info['phone'] or '',
                    'country': country,
                    'keyword': keyword
                })

                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️  Duplicate", "INFO"); continue

                total_leads += 1
                kw_leads += 1
                email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                phone_str = f"📞 {info['phone']}" if info['phone'] else ""
                log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {email_str} {phone_str}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                log(f"   Error: {e}", "WARN"); continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"\n✅ '{keyword}' — leads:{kw_leads} paid:{rej_payment} other:{rej_other}", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
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
            lr   = call_sheet({'action': 'get_leads'})
            leads = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr   = call_sheet({'action': 'get_keywords'})
            kws  = kr.get('keywords', [])
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
