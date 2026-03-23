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
import io
import csv
import os

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

# ── Apps Script API call ───────────────────────────────────────────────────────
def call_sheet(script_url, payload):
    """POST to Apps Script and return parsed JSON."""
    try:
        r = requests.post(script_url, json=payload, timeout=20)
        data = r.json()
        return data
    except Exception as e:
        log(f"Sheet API error: {e}", "WARN")
        return {'error': str(e)}

def call_sheet_get(script_url, action):
    try:
        r = requests.get(script_url, params={'action': action}, timeout=20)
        return r.json()
    except Exception as e:
        log(f"Sheet GET error: {e}", "WARN")
        return {'error': str(e)}

# ── Logging ────────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': message}
    log_queue.put(json.dumps(entry))

# ── Payment gateway detection — comprehensive ─────────────────────────────────
# Strong indicators — any ONE of these = has payment gateway
PAYMENT_STRONG = [
    'js.stripe.com', 'stripe.com/v3', 'stripe.js',
    'paypal.com/sdk', 'paypal.com/js', 'paypalobjects.com',
    'data-paypal', 'paypal-button',
    'shopify_payments', 'shop_pay', 'shop-pay',
    'cdn.shopify.com/shopifycloud/shop-js',
    'pay.shopify.com',
    'js.klarna.com', 'klarna-payments',
    'js.afterpay.com', 'clearpay.co.uk',
    'cdn1.affirm.com', 'affirm.js',
    'checkout.sezzle.com',
    'apple-pay-button', 'google-pay',
    '"shopify_payments"', "'shopify_payments'",
    'Shopify.Checkout',
    '"payment_gateway":[',
    '"paymentGateway"',
]

def check_payment_gateway(base_url, session):
    """
    Returns True if store HAS a payment gateway (skip).
    Returns False if NO payment gateway (this is our target!).
    Checks: homepage HTML, window.Shopify JSON, /cart.js, /cart page
    """
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
    try:
        # 1. Check homepage
        r = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        hl = html.lower()
        for ind in PAYMENT_STRONG:
            if ind.lower() in hl:
                return True
        # Check window.Shopify embedded object
        m = re.search(r'window\.Shopify\s*=\s*(\{.*?\});', html, re.DOTALL)
        if m:
            try:
                sd = json.loads(m.group(1))
                if sd.get('paymentButton') or sd.get('Checkout'):
                    return True
            except:
                pass
        if '"payment_gateway"' in html or '"paymentGateway"' in html:
            return True
        if 'shop-pay-button' in hl or 'shopify-payment-button' in hl:
            return True
        # 2. Check /cart page
        try:
            cr = session.get(base_url + '/cart', headers=headers, timeout=10)
            ch = cr.text.lower()
            for ind in PAYMENT_STRONG:
                if ind.lower() in ch:
                    return True
            if ('visa' in ch and 'mastercard' in ch) or 'shop pay' in ch or 'shoppay' in ch:
                return True
        except:
            pass
        # 3. Check /cart.js
        try:
            cjr = session.get(base_url + '/cart.js', headers=headers, timeout=8)
            if cjr.status_code == 200:
                cjl = cjr.text.lower()
                if 'payment' in cjl and ('stripe' in cjl or 'paypal' in cjl or 'shopify_pay' in cjl):
                    return True
        except:
            pass
        return False
    except:
        return True  # can't access = skip

# ── Shopify detection ──────────────────────────────────────────────────────────
def is_shopify_store(url, session):
    """Check if URL is a Shopify store using /products.json endpoint."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    }
    try:
        # Shopify stores always have /products.json
        r = session.get(url + '/products.json?limit=1', headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if 'products' in data:
                return True
        # Fallback: check HTML for Shopify signature
        r2 = session.get(url, headers=headers, timeout=12)
        if 'cdn.shopify.com' in r2.text or 'Shopify.theme' in r2.text:
            return True
        return False
    except:
        return False

# ── Store info extraction ──────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', 'png', 'jpg', 'svg', 'gif', 'woff']

def extract_email(soup, html):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_DOMAINS):
                return email
    for match in EMAIL_REGEX.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_DOMAINS):
            return m
    return None

def extract_phone(html):
    for pat in [r'\+\d{1,3}[\s\-]?\d{6,12}',
                r'\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}']:
        found = re.search(pat, html)
        if found:
            return found.group(0).strip()
    return None

def get_store_info(base_url, session):
    """Get store name, email, phone from a confirmed no-payment Shopify store."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        store_name = title.text.strip()[:80] if title else base_url.replace('https://','').split('.')[0]
        email = extract_email(soup, html)
        phone = extract_phone(html)
        # Try contact/about pages for email
        if not email:
            for path in ['/pages/contact', '/contact', '/pages/about-us', '/pages/about']:
                try:
                    cr = session.get(base_url + path, headers=headers, timeout=10)
                    if cr.status_code == 200:
                        cs = BeautifulSoup(cr.text, 'html.parser')
                        email = extract_email(cs, cr.text)
                        if not phone:
                            phone = extract_phone(cr.text)
                        if email:
                            break
                except:
                    continue
        return {'store_name': store_name, 'email': email, 'phone': phone}
    except:
        return {'store_name': base_url.replace('https://','').split('.')[0], 'email': None, 'phone': None}

# ── Google Custom Search API (free 100/day, no IP blocks) ───────────────────
MYSHOPIFY_REGEX = re.compile(r'https?://([a-zA-Z0-9\-]+)\.myshopify\.com')

def search_google_cse(query, api_key, cx_id, start=1):
    """
    Google Custom Search API — always works from any server.
    Free: 100 queries/day. cx_id should target *.myshopify.com
    """
    urls = []
    try:
        params = {
            'key': api_key,
            'cx': cx_id,
            'q': query,
            'num': 10,
            'start': start,
        }
        r = requests.get('https://www.googleapis.com/customsearch/v1',
                         params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            items = data.get('items', [])
            for item in items:
                link = item.get('link', '')
                m = MYSHOPIFY_REGEX.match(link)
                if m:
                    url = f"https://{m.group(1)}.myshopify.com"
                    if url not in urls:
                        urls.append(url)
            log(f"   Google CSE: {len(items)} results → {len(urls)} myshopify", "INFO")
        elif r.status_code == 429:
            log("⚠️  Google CSE daily limit reached (100/day)", "WARN")
        elif r.status_code == 400:
            log(f"⚠️  Google CSE error: {r.json().get('error',{}).get('message','bad request')}", "WARN")
        else:
            log(f"⚠️  Google CSE: {r.status_code}", "WARN")
    except Exception as e:
        log(f"⚠️  CSE error: {e}", "WARN")
    return urls

def search_shopify_stores(keyword, country, google_api_key, cx_id):
    """
    Search for *.myshopify.com stores using Google Custom Search.
    CSE should be configured to search only myshopify.com sites.
    """
    all_urls = []

    queries = [
        f'{keyword} {country}',
        f'new {keyword} store {country}',
        f'{keyword} shop {country} 2024',
        f'{keyword} {country} online',
    ]

    for i, query in enumerate(queries):
        if len(all_urls) >= 80:
            break
        log(f"🔍 Query {i+1}/{len(queries)}: {query}", "INFO")
        # Get pages 1, 11, 21 (each = 10 results)
        for start_idx in [1, 11, 21]:
            found = search_google_cse(query, google_api_key, cx_id, start=start_idx)
            new = [u for u in found if u not in all_urls]
            all_urls.extend(new)
            if not found:
                break
            time.sleep(0.5)
        log(f"   Total so far: {len(all_urls)}", "INFO")
        time.sleep(1)

    log(f"📦 Total myshopify stores found: {len(all_urls)}", "INFO")
    return all_urls

# ── AI Email generation ────────────────────────────────────────────────────────
def generate_email(template_subject, template_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a short, personalized, professional cold email to a Shopify store owner.

Store Name: {lead.get('store_name', 'your store')}
Store URL: {lead.get('url', '')}
Country: {lead.get('country', '')}

Their problem: They have a Shopify store but NO payment gateway set up, so they cannot accept any payments right now.

Use this as base:
Subject: {template_subject}
Body: {template_body}

Rules:
- Max 100 words
- No spam words (FREE, GUARANTEED, LIMITED TIME, etc.)
- Mention their store name once naturally
- Be genuinely helpful, not salesy
- End with a simple question or soft CTA
- HTML format with <p> tags

Return ONLY this JSON (no markdown, no extra text):
{{"subject": "...", "body": "<p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400, temperature=0.7
        )
        text = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(text)
        return data.get('subject', template_subject), data.get('body', template_body)
    except Exception as e:
        log(f"Groq error: {e} — using template directly", "WARN")
        return template_subject, f"<p>{template_body}</p>"

# ── Main automation ────────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _automation_inner()
    except Exception as e:
        import traceback
        log(f"💥 FATAL: {e}", "ERROR")
        log(traceback.format_exc()[:400], "ERROR")
    finally:
        automation_running = False

def _automation_inner():
    global automation_running

    # Get script URL from env or settings file
    script_url = os.environ.get('APPS_SCRIPT_URL', '')

    if not script_url:
        log("❌ Apps Script URL not set — add APPS_SCRIPT_URL to Render environment", "ERROR")
        return

    # Load config from Google Sheet
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet(script_url, {'action': 'get_config'})
    cfg = cfg_resp.get('config', {})

    groq_key = cfg.get('groq_api_key', '')
    google_api_key = cfg.get('google_api_key', '')
    cx_id = cfg.get('cx_id', '')
    min_leads = int(cfg.get('min_leads', '50'))

    if not groq_key:
        log("❌ Groq API Key missing — add in CFG screen", "ERROR"); return
    if not google_api_key:
        log("❌ Google API Key missing — add in CFG screen", "ERROR"); return
    if not cx_id:
        log("❌ Google CX ID missing — add in CFG screen", "ERROR"); return

    # Load keywords
    kw_resp = call_sheet(script_url, {'action': 'get_keywords'})
    all_keywords = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']

    if not all_keywords:
        log("❌ No ready keywords — add keywords in sheet or reset", "ERROR"); return

    # Load template
    tpl_resp = call_sheet(script_url, {'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email templates — add one in Templates sheet", "ERROR"); return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER STARTED", "SUCCESS")
    log(f"🎯 Will collect until {min_leads} leads found | {len(all_keywords)} keywords ready", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    # ── PHASE 1: SCRAPING ────────────────────────────────────────────────────
    log("📡 PHASE 1 — FINDING STORES WITHOUT PAYMENT", "INFO")

    for kw_row in all_keywords:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row['keyword']
        country = kw_row['country']
        kw_id = kw_row['id']
        kw_leads = 0

        log(f"🎯 [{keyword}] in [{country}]", "INFO")

        try:
            candidate_urls = search_shopify_stores(keyword, country, google_api_key, cx_id)
        except Exception as e:
            log(f"Search error: {e}", "WARN")
            candidate_urls = []

        if not candidate_urls:
            log("⚠️  No URLs found", "WARN")
            call_sheet(script_url, {'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 {len(candidate_urls)} URLs to check...", "INFO")

        for url in candidate_urls:
            if not automation_running or total_leads >= min_leads:
                break
            try:
                log(f"🔎 {url}", "INFO")

                # Step 1: Is it Shopify?
                if not is_shopify_store(url, session):
                    log(f"   ❌ Not Shopify", "INFO")
                    time.sleep(0.5)
                    continue

                log(f"   ✅ Shopify confirmed", "INFO")

                # Step 2: Does it have NO payment gateway?
                has_payment = check_payment_gateway(url, session)
                if has_payment:
                    log(f"   💳 Has payment gateway — skip", "INFO")
                    time.sleep(0.5)
                    continue

                log(f"   🎯 NO payment gateway found!", "SUCCESS")

                # Step 3: Get store info
                info = get_store_info(url, session)
                store_name = info['store_name']
                email = info['email']
                phone = info['phone']

                # Step 4: Save to Google Sheet
                save_resp = call_sheet(script_url, {
                    'action': 'save_lead',
                    'store_name': store_name,
                    'url': url,
                    'email': email or '',
                    'phone': phone or '',
                    'country': country,
                    'keyword': keyword
                })

                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️  Already in sheet", "INFO")
                    continue

                total_leads += 1
                kw_leads += 1
                log(f"✅ Lead #{total_leads} — {store_name} | {email or '⚠ no email'}", "SUCCESS")
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                log(f"⚠️  Error on {url[:40]}: {e}", "WARN")
                continue

        call_sheet(script_url, {'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"🏷️  '{keyword}' done — {kw_leads} leads", "SUCCESS")

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    if total_leads < min_leads:
        log(f"⚠️  Only {total_leads}/{min_leads} leads found — keywords exhausted!", "WARN")
        log("💡 Add more keywords in Custom Leads screen and restart", "INFO")
    else:
        log(f"🎯 SCRAPING DONE — {total_leads} leads found!", "SUCCESS")

    # ── PHASE 2: EMAIL OUTREACH ──────────────────────────────────────────────
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")

    leads_resp = call_sheet(script_url, {'action': 'get_leads'})
    all_leads = leads_resp.get('leads', [])
    pending = [l for l in all_leads if l.get('email') and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            break
        try:
            email_to = lead.get('email', '')
            if not email_to or '@' not in email_to:
                continue

            log(f"✉️  {i+1}/{len(pending)} → {email_to}", "INFO")

            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)

            send_resp = call_sheet(script_url, {
                'action': 'send_email',
                'to': email_to,
                'subject': subject,
                'body': body,
                'lead_id': lead.get('id')
            })

            if send_resp.get('status') == 'ok':
                log(f"✅ Sent → {email_to}", "SUCCESS")
            else:
                log(f"❌ Failed → {send_resp.get('message', 'unknown error')}", "ERROR")

            delay = random.randint(90, 150)
            log(f"⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)

        except Exception as e:
            log(f"⚠️  Email error: {e}", "WARN")
            continue

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE!", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask routes ───────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = 0
    emails_sent = 0
    kw_total = 0
    kw_used = 0
    if script_url:
        try:
            lr = call_sheet(script_url, {'action': 'get_leads'})
            leads = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet(script_url, {'action': 'get_keywords'})
            kws = kr.get('keywords', [])
            kw_total = len(kws)
            kw_used = sum(1 for k in kws if k.get('status') == 'used')
        except:
            pass
    return jsonify({
        'running': automation_running,
        'total_leads': total_leads,
        'emails_sent': emails_sent,
        'kw_total': kw_total,
        'kw_used': kw_used,
        'script_connected': bool(script_url),
    })

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

# Proxy all Sheet API calls through Flask
@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return jsonify({'error': 'APPS_SCRIPT_URL not set in Render environment'})
    result = call_sheet(script_url, request.json)
    return jsonify(result)

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
    d = request.json
    t = d.get('time', '')
    try:
        from datetime import datetime
        run_time = datetime.fromisoformat(t)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='sched', replace_existing=True
        )
        log(f"📅 Scheduled for {t}", "INFO")
        return jsonify({'status': 'scheduled'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
