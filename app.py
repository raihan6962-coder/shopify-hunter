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
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

# ── Apps Script ───────────────────────────────────────────────────────────────
def call_sheet(payload):
    url = os.environ.get('APPS_SCRIPT_URL', '')
    if not url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except Exception as e:
            time.sleep(3)
    return {'error': 'Sheet API failed'}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ─────────────────────────────────────────────────────────────────────────────
# CORE STRATEGY:
# 1. crt.sh → Get recent *.myshopify.com domains (new stores, free, no API)
# 2. Filter by keyword
# 3. Add product to cart → go to checkout
# 4. If Shopify shows "isn't accepting payments" → CONFIRMED LEAD!
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# ── Step 1: Get recent myshopify.com domains from SSL logs ───────────────────
def get_recent_stores_from_crtsh(keyword, days_back=30):
    """
    crt.sh is a free SSL certificate transparency log.
    Every new myshopify.com store gets an SSL cert = shows up here.
    We search for keyword in subdomain names.
    Returns list of recently registered store URLs.
    """
    stores = []
    try:
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        # Search for keyword in myshopify subdomain
        query = f"%25{keyword.lower().replace(' ', '')}%25.myshopify.com"
        url = f"https://crt.sh/?q={query}&output=json"
        
        r = requests.get(url, timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            log(f"crt.sh returned {r.status_code}", "WARN")
            return stores
            
        entries = r.json()
        seen = set()
        
        for entry in entries:
            # Only recent certs
            not_before = entry.get('not_before', '')
            if not_before and not_before[:10] < cutoff:
                continue
                
            name = entry.get('name_value', '').lower()
            for domain in name.split('\n'):
                domain = domain.strip().replace('*.', '')
                if (domain.endswith('.myshopify.com') and 
                    '*' not in domain and 
                    domain not in seen):
                    seen.add(domain)
                    stores.append(f"https://{domain}")
                    
        log(f"   crt.sh found {len(stores)} recent stores for '{keyword}'", "INFO")
    except Exception as e:
        log(f"   crt.sh error: {e}", "WARN")
    return stores

def get_recent_stores_broad(days_back=7):
    """
    Get ALL recent myshopify.com stores (no keyword filter).
    Good for finding stores from last 7 days.
    """
    stores = []
    try:
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        url = f"https://crt.sh/?q=%25.myshopify.com&output=json"
        r = requests.get(url, timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return stores
        entries = r.json()
        seen = set()
        for entry in entries:
            not_before = entry.get('not_before', '')
            if not_before and not_before[:10] < cutoff:
                continue
            name = entry.get('name_value', '').lower()
            for domain in name.split('\n'):
                domain = domain.strip().replace('*.', '')
                if domain.endswith('.myshopify.com') and '*' not in domain and domain not in seen:
                    seen.add(domain)
                    stores.append(f"https://{domain}")
        log(f"   crt.sh broad search: {len(stores)} stores from last {days_back} days", "INFO")
    except Exception as e:
        log(f"   crt.sh broad error: {e}", "WARN")
    return stores

# ── Step 2: The ONLY check that matters — checkout test ──────────────────────
# Shopify's exact messages when no payment gateway:
NO_PAYMENT_PHRASES = [
    "isn't accepting payments right now",
    "is not accepting payments right now", 
    "not accepting payments",
    "isn't accepting payments",
    "store is currently unavailable",
    "no payment methods available",
]

def check_has_no_payment_gateway(base_url, session):
    """
    THE DEFINITIVE TEST:
    Add a product to cart → go to checkout.
    If Shopify shows "isn't accepting payments" = no gateway = LEAD!
    
    Returns: 
        'no_payment'  → confirmed no payment gateway (LEAD!)
        'has_payment' → has payment gateway (skip)
        'skip'        → not shopify, password protected, no products, etc.
    """
    try:
        # Quick homepage check — must be Shopify
        r = session.get(base_url, headers=HEADERS, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return 'skip'
        html = r.text
        if 'cdn.shopify.com' not in html and 'shopify' not in html.lower():
            return 'skip'
        
        # Skip password protected / coming soon stores
        if ('/password' in r.url or 
            'password-page' in html.lower() or 
            'opening soon' in html.lower() or
            'coming soon' in html.lower()):
            return 'skip'

        # Get a product to add to cart
        prod_r = session.get(f"{base_url}/products.json?limit=1", 
                             headers=HEADERS, timeout=8)
        if prod_r.status_code != 200:
            return 'skip'
            
        prod_data = prod_r.json()
        products = prod_data.get('products', [])
        if not products:
            return 'skip'  # Empty store
            
        # Add first product to cart
        variant_id = products[0]['variants'][0]['id']
        session.post(
            f"{base_url}/cart/add.js",
            json={"id": variant_id, "quantity": 1},
            headers={**HEADERS, 'Content-Type': 'application/json'},
            timeout=8
        )
        
        # Go to checkout
        chk_r = session.get(f"{base_url}/checkout", headers=HEADERS, timeout=12)
        chk_html = chk_r.text.lower()
        
        # THE DEFINITIVE CHECK — Shopify's exact no-payment error
        for phrase in NO_PAYMENT_PHRASES:
            if phrase in chk_html:
                return 'no_payment'  # ✅ CONFIRMED NO PAYMENT GATEWAY!
        
        # Has payment — check for payment method indicators
        payment_indicators = [
            'visa', 'mastercard', 'paypal', 'credit card', 'card number',
            'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay',
            'debit card', 'payment method'
        ]
        for ind in payment_indicators:
            if ind in chk_html:
                return 'has_payment'
        
        # Checkout loaded but no payment info found either way
        # Check if it's actually a checkout page
        if 'checkout' in chk_r.url or 'order' in chk_html:
            return 'has_payment'  # Assume has payment if checkout works normally
            
        return 'skip'
        
    except Exception:
        return 'skip'

# ── Step 3: Extract contact info ──────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'shopify', '.png', '.jpg', 'noreply', 'domain.com', 'wixpress']

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL):
                return email
    for m in EMAIL_RE.findall(html):
        m = m.lower()
        if not any(d in m for d in SKIP_EMAIL):
            return m
    return None

def extract_phone(html):
    m = re.search(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})', html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    info = {'store_name': base_url.replace('https://', '').split('.')[0], 
            'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=HEADERS, timeout=10)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title:
            info['store_name'] = title.text.strip()[:80]
        info['email'] = extract_email(html, soup)
        info['phone'] = extract_phone(html)
        if not info['email']:
            for path in ['/pages/contact', '/contact', '/pages/about-us']:
                try:
                    pr = session.get(base_url + path, headers=HEADERS, timeout=7)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text, 'html.parser')
                        email = extract_email(pr.text, ps)
                        if email:
                            info['email'] = email
                            break
                        if not info['phone']:
                            info['phone'] = extract_phone(pr.text)
                except:
                    continue
    except:
        pass
    return info

# ── Email generation ──────────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a short cold email to a Shopify store owner.

Store: {lead.get('store_name', 'the store')}
URL: {lead.get('url', '')}
Problem: Their store has NO payment gateway. Customers cannot pay!

Base template — Subject: {tpl_subject} | Body: {tpl_body}

Rules: 80-100 words, no spam words, mention store name once, helpful tone, end with soft question, use HTML <p> tags.
Respond ONLY with JSON: {{"subject": "...", "body": "<p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400, temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
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
        log(traceback.format_exc()[:400], "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation finished", "INFO")

def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — add in CFG screen", "ERROR")
        return

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords — add keywords in Leads screen", "ERROR")
        return

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template — add one in Email screen", "ERROR")
        return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0
    checked = 0
    rejected = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER — NO-PAYMENT STORE FINDER", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Strategy: crt.sh SSL scan", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    # ── PHASE 1: Find & Verify ───────────────────────────────────────────────
    log("📡 PHASE 1 — SCANNING FOR STORES WITH NO PAYMENT GATEWAY", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🔍 Keyword: [{keyword}] | Country: [{country}]", "INFO")
        log(f"   Scanning SSL certificate logs for recent '{keyword}' stores...", "INFO")

        # Get stores from crt.sh — keyword-based + broad recent
        stores_kw = get_recent_stores_from_crtsh(keyword, days_back=60)
        stores_broad = get_recent_stores_broad(days_back=14)
        
        # Merge, keyword stores first
        all_stores = list(dict.fromkeys(stores_kw + stores_broad))
        
        # Filter: if country specified, try to match domain name hints
        # (crt.sh doesn't have country info, so we check all)
        
        if not all_stores:
            log("   ⚠️  No stores found in SSL logs. Try different keyword.", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"   📦 {len(all_stores)} stores to test for payment gateway...", "INFO")
        log(f"   🧪 Testing checkout on each store (this is accurate!)", "INFO")

        for url in all_stores:
            if not automation_running or total_leads >= min_leads:
                break

            checked += 1
            try:
                result = check_has_no_payment_gateway(url, session)
                
                if result == 'skip':
                    continue
                    
                if result == 'has_payment':
                    rejected += 1
                    log(f"   💳 Has payment — skip ({url[:45]})", "INFO")
                    time.sleep(0.3)
                    continue
                    
                if result == 'no_payment':
                    # ✅ CONFIRMED: No payment gateway!
                    log(f"   🎯 NO PAYMENT FOUND! Collecting info...", "SUCCESS")
                    
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
                        log(f"   ⏭️  Already collected", "INFO")
                        continue
                        
                    total_leads += 1
                    kw_leads += 1
                    log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                    log(f"   📊 Stats: checked={checked} | leads={total_leads} | skipped={rejected}", "INFO")
                    time.sleep(random.uniform(1, 2))

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping complete! Leads: {total_leads} | Checked: {checked}", "SUCCESS")

    # ── PHASE 2: Email Outreach ──────────────────────────────────────────────
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads = leads_resp.get('leads', [])
    pending = [l for l in all_leads 
               if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            break
        try:
            email_to = lead['email']
            log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
            send_resp = call_sheet({
                'action': 'send_email',
                'to': email_to,
                'subject': subject,
                'body': body,
                'lead_id': lead.get('id', '')
            })
            if send_resp.get('status') == 'ok':
                log(f"   ✅ Sent!", "SUCCESS")
            else:
                log(f"   ❌ Failed: {send_resp.get('message', '')}", "ERROR")
            delay = random.randint(90, 150)
            log(f"   ⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except Exception as e:
            log(f"   Error: {e}", "WARN")
            continue

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check your Google Sheet for leads.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask Routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    total_leads = emails_sent = kw_total = kw_used = 0
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if script_url:
        try:
            lr = call_sheet({'action': 'get_leads'})
            leads = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
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

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL'):
        return jsonify({'error': 'APPS_SCRIPT_URL not set in Render environment'})
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
    d = request.json
    try:
        run_time = datetime.fromisoformat(d.get('time', ''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='scheduled_run', replace_existing=True
        )
        log(f"📅 Scheduled for {d.get('time')}", "INFO")
        return jsonify({'status': 'scheduled'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
