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
# PHASE 1: STORE DISCOVERY
# Sources: Brute-force + crt.sh + URLScan
# All return real myshopify.com stores
# ─────────────────────────────────────────────────────────────────────────────
def get_store_candidates(keyword):
    urls = set()
    kw_clean = keyword.lower().replace(' ', '').replace('-', '')
    kw_words = keyword.lower().split()

    # ── Source 1: Brute-force name generation ────────────────────────────────
    log(f"   [1/3] Generating store names...", "INFO")
    prefixes = ['', 'my', 'the', 'shop', 'buy', 'best', 'new', 'official',
                'top', 'pro', 'all', 'get', 'try', 'true', 'real']
    suffixes = ['', 'shop', 'store', 'online', 'co', 'boutique', 'hub',
                'spot', 'deals', 'mart', 'hq', 'lab', 'us', 'uk', 'world']
    for p in prefixes:
        for s in suffixes:
            if p or s:
                urls.add(f"https://{p}{kw_clean}{s}.myshopify.com")
            if p and s:
                urls.add(f"https://{p}-{kw_clean}-{s}.myshopify.com")
    # Numbers
    for n in range(1, 30):
        urls.add(f"https://{kw_clean}{n}.myshopify.com")
    # Word combinations
    if len(kw_words) > 1:
        for w in kw_words:
            for s in suffixes[:6]:
                urls.add(f"https://{w}{s}.myshopify.com")
    log(f"   Brute-force: {len(urls)} candidates", "INFO")

    # ── Source 2: crt.sh SSL Certificate Log ─────────────────────────────────
    log(f"   [2/3] crt.sh SSL scan...", "INFO")
    try:
        r = requests.get(
            f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json",
            timeout=15, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            try:
                certs = r.json()
                before = len(urls)
                for cert in certs:
                    name = cert.get('common_name', '') or cert.get('name_value', '')
                    for n in name.split('\n'):
                        n = n.strip().replace('*.', '').lower()
                        if n.endswith('.myshopify.com') and '*' not in n:
                            urls.add(f"https://{n}")
                log(f"   crt.sh: +{len(urls)-before} stores", "INFO")
            except ValueError:
                log(f"   crt.sh: no JSON response (skipped)", "WARN")
    except Exception as e:
        log(f"   crt.sh: {e} (skipped)", "WARN")

    # ── Source 3: URLScan.io ──────────────────────────────────────────────────
    log(f"   [3/3] URLScan scan...", "INFO")
    try:
        r = requests.get(
            f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com+AND+{kw_clean}&size=1000&sort=time",
            timeout=12, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            before = len(urls)
            for res in r.json().get('results', []):
                page_url = res.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m:
                    urls.add(f"https://{m.group(1)}.myshopify.com")
            log(f"   URLScan: +{len(urls)-before} stores", "INFO")
    except Exception as e:
        log(f"   URLScan: {e} (skipped)", "WARN")

    result = list(urls)
    random.shuffle(result)
    log(f"📦 Total candidates: {len(result)}", "INFO")
    return result

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2: ALIVE CHECK + CHECKOUT TEST
# Step 1: Is it a real, live Shopify store?
# Step 2: Does it have NO payment gateway?
# ─────────────────────────────────────────────────────────────────────────────

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
HEADERS = {'User-Agent': UA, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}

# Shopify no-payment messages (multi-language)
NO_PAYMENT_PHRASES = [
    # English
    "isn't accepting payments right now",
    "is not accepting payments right now",
    "not accepting payments",
    "no payment methods are available",
    "payment provider hasn't been set up",
    "this store is unavailable",
    "cannot accept payments",
    "checkout is disabled",
    # German
    "dieser shop kann zurzeit keine zahlungen akzeptieren",
    "keine zahlungen akzeptieren",
    # French
    "n'accepte pas les paiements",
    "aucun moyen de paiement",
    # Spanish
    "no acepta pagos",
    "ningún método de pago",
    # Italian
    "non accetta pagamenti",
    # Dutch
    "accepteert momenteel geen betalingen",
]

# If ANY of these appear → has payment → skip
PAYMENT_KEYWORDS = [
    'visa', 'mastercard', 'amex', 'american express',
    'paypal', 'credit card', 'debit card', 'card number',
    'stripe', 'klarna', 'afterpay', 'shop pay', 'shoppay',
    'apple pay', 'google pay', 'discover',
    'card-fields', 'payment-method', 'pay with',
]

def check_store(base_url, session, keyword):
    """
    Returns:
      'lead'        → Shopify store + NO payment gateway ✅
      'has_payment' → has payment, skip
      'skip'        → not Shopify / dead / no products
    """
    kw_lower = keyword.lower().strip()

    try:
        # Step 1: Is it alive and is it Shopify?
        r = session.get(base_url, headers=HEADERS, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return 'skip'
        html = r.text
        html_lower = html.lower()

        if 'cdn.shopify.com' not in html and 'shopify' not in html_lower[:3000]:
            return 'skip'

        # Step 2: Keyword in homepage or URL? (niche filter)
        if kw_lower and kw_lower not in html_lower and kw_lower not in base_url.lower():
            return 'skip'

        # Step 3: Password protected? Skip (can't test checkout)
        if '/password' in r.url or 'password-page' in html_lower:
            return 'skip'

        # Step 4: Get a product to add to cart
        pr = session.get(f"{base_url}/products.json?limit=1", headers=HEADERS, timeout=8)
        if pr.status_code != 200:
            return 'skip'
        products = pr.json().get('products', [])
        if not products:
            return 'skip'

        # Step 5: Add to cart
        vid = products[0]['variants'][0]['id']
        session.post(
            f"{base_url}/cart/add.js",
            json={"id": vid, "quantity": 1},
            headers={**HEADERS, 'Content-Type': 'application/json'},
            timeout=8
        )

        # Step 6: Go to checkout and analyze HTML
        cr = session.get(f"{base_url}/checkout", headers=HEADERS, timeout=12, allow_redirects=True)
        chk = cr.text.lower()

        # Shopify's explicit no-payment message (any language) = CONFIRMED LEAD!
        for phrase in NO_PAYMENT_PHRASES:
            if phrase in chk:
                return 'lead'

        # Payment found = skip
        for kw in PAYMENT_KEYWORDS:
            if kw in chk:
                return 'has_payment'

        # Reached checkout page but no payment indicators = likely no payment
        checkout_signals = ['contact information', 'shipping address',
                            'order summary', 'express checkout',
                            'kontaktinformationen', 'informazioni di contatto']
        if any(s in chk for s in checkout_signals):
            return 'lead'

        return 'skip'

    except:
        return 'skip'

# ─────────────────────────────────────────────────────────────────────────────
# STORE INFO EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
              '.svg', 'noreply', 'domain.com', 'no-reply', 'schema.org', 'w3.org']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def is_valid_email(e):
    e = e.lower()
    if any(s in e for s in SKIP_EMAIL): return False
    parts = e.split('@')
    if len(parts) != 2 or not parts[0] or '.' not in parts[1]: return False
    return 2 <= len(parts[1].split('.')[-1]) <= 6

def get_store_info(base_url, session):
    result = {'store_name': base_url.replace('https://','').split('.')[0],
              'email': None, 'phone': None}
    headers = {'User-Agent': UA}
    pages = ['', '/pages/contact', '/pages/contact-us', '/contact',
             '/pages/about-us', '/pages/about', '/policies/refund-policy']
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
                for tag in soup.find_all('a', href=True):
                    href = tag.get('href', '')
                    if href.startswith('mailto:'):
                        e = href[7:].split('?')[0].strip().lower()
                        if is_valid_email(e):
                            result['email'] = e; break
            if not result['email']:
                for m in EMAIL_RE.findall(html):
                    if is_valid_email(m):
                        result['email'] = m.lower(); break
            if not result['phone']:
                pm = PHONE_RE.search(html)
                if pm: result['phone'] = pm.group(0).strip()
        except: continue
    return result

# ─────────────────────────────────────────────────────────────────────────────
# AI EMAIL GENERATION
# ─────────────────────────────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: NO payment gateway configured — customers cannot checkout!
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
            data = json.loads(raw.replace('\n', ' '), strict=False)
            return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq fallback: {e}", "WARN")
    return tpl_subject, f'<p>{tpl_body}</p>'

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AUTOMATION
# ─────────────────────────────────────────────────────────────────────────────
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
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key = cfg.get('groq_api_key', '').strip()
    # min_leads is just for email phase reference, NOT a hard stop for scraping
    min_leads_for_email = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    log(f"✅ Config loaded | Email phase starts after: {min_leads_for_email} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"✅ {len(ready_kws)} keywords | Template: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — FINDING NO-PAYMENT STORES", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id   = kw_row.get('id', '')
        kw_leads = rej_pay = rej_other = checked = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # Get all candidate URLs
        candidates = get_store_candidates(keyword)
        if not candidates:
            log("⚠️  No candidates found", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(candidates)} candidates...", "INFO")

        for idx, url in enumerate(candidates):
            if not automation_running: break

            try:
                result = check_store(url, session, keyword)
                checked += 1

                if result == 'lead':
                    log(f"   [{idx+1}] 🎯 NO PAYMENT! → {url}", "SUCCESS")
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

                    if save_resp.get('error'):
                        log(f"   Sheet error: {save_resp['error']}", "WARN"); continue
                    if save_resp.get('status') == 'duplicate':
                        log(f"   ⏭️  Duplicate", "INFO"); continue

                    total_leads += 1
                    kw_leads += 1
                    email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                    phone_str = f"| 📞 {info['phone']}" if info['phone'] else ""
                    log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_str} {phone_str}", "SUCCESS")
                    time.sleep(random.uniform(1, 2))

                elif result == 'has_payment':
                    rej_pay += 1
                    # Silent skip for payment stores (keeps log clean)

                # 'skip' = silent

                # Progress update every 20 checks
                if checked % 20 == 0:
                    log(f"   Progress: {checked}/{len(candidates)} checked | leads:{kw_leads} paid:{rej_pay}", "INFO")

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads | {rej_pay} had payment | {checked} checked", "SUCCESS")

    log(f"\n📊 PHASE 1 DONE! Total leads collected: {total_leads}", "SUCCESS")

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2: EMAIL OUTREACH
    # ─────────────────────────────────────────────────────────────────────────
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email', ''))
                  and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads with emails to contact", "INFO")

    if not pending:
        log("⚠️  No leads with emails yet — check your Google Sheet", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
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
            log(f"   ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ Next in {delay}s...", "INFO")
        time.sleep(delay)

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check your Google Sheet for leads.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

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
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
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
    if automation_running: return jsonify({'status': 'already_running'})
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
