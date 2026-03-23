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
    url = os.environ.get('APPS_SCRIPT_URL', '')
    if not url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except Exception as e:
            time.sleep(3)
    return {'error': 'Sheet API failed'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY — 4 methods, each is a fallback for the previous
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def extract_myshopify_urls(text):
    urls = set()
    for m in MYSHOPIFY_RE.finditer(text):
        urls.add(f"https://{m.group(1)}.myshopify.com")
    return list(urls)

# ── Method 1: crt.sh keyword search ──────────────────────────────────────────
def from_crtsh(keyword):
    stores = set()
    kw = keyword.lower().replace(' ', '')
    try:
        url = f"https://crt.sh/?q=%25{kw}%25.myshopify.com&output=json"
        r = requests.get(url, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            for entry in r.json():
                name = entry.get('name_value', '')
                for domain in name.split('\n'):
                    domain = domain.strip().replace('*.', '').lower()
                    if domain.endswith('.myshopify.com') and '*' not in domain:
                        stores.add(f"https://{domain}")
        log(f"   crt.sh: {len(stores)} stores", "INFO")
    except Exception as e:
        log(f"   crt.sh failed: {e}", "WARN")
    return list(stores)

# ── Method 2: CertSpotter (alternative SSL log) ───────────────────────────────
def from_certspotter(keyword):
    stores = set()
    kw = keyword.lower().replace(' ', '')
    try:
        url = f"https://api.certspotter.com/v1/issuances?domain={kw}.myshopify.com&include_subdomains=false&expand=dns_names"
        r = requests.get(url, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            for entry in r.json():
                for name in entry.get('dns_names', []):
                    name = name.replace('*.', '').lower()
                    if name.endswith('.myshopify.com'):
                        stores.add(f"https://{name}")
        # Also search wildcard
        url2 = f"https://api.certspotter.com/v1/issuances?domain=myshopify.com&include_subdomains=true&expand=dns_names&after=2024-01-01"
        r2 = requests.get(url2, timeout=12, headers={'User-Agent': 'Mozilla/5.0'})
        if r2.status_code == 200:
            for entry in r2.json()[:200]:
                for name in entry.get('dns_names', []):
                    name = name.replace('*.', '').lower()
                    if name.endswith('.myshopify.com') and kw in name:
                        stores.add(f"https://{name}")
        log(f"   CertSpotter: {len(stores)} stores", "INFO")
    except Exception as e:
        log(f"   CertSpotter failed: {e}", "WARN")
    return list(stores)

# ── Method 3: Wayback Machine CDX API ────────────────────────────────────────
def from_wayback(keyword):
    stores = set()
    kw = keyword.lower().replace(' ', '')
    try:
        url = (f"http://web.archive.org/cdx/search/cdx"
               f"?url=*.myshopify.com&matchType=domain&output=text"
               f"&fl=original&filter=statuscode:200&limit=500"
               f"&from=20240101&collapse=urlkey")
        r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            for line in r.text.strip().split('\n'):
                line = line.strip()
                if '.myshopify.com' in line and kw in line.lower():
                    found = extract_myshopify_urls(line)
                    stores.update(found)
        log(f"   Wayback: {len(stores)} stores", "INFO")
    except Exception as e:
        log(f"   Wayback failed: {e}", "WARN")
    return list(stores)

# ── Method 4: Generate & probe (keyword-based guessing) ──────────────────────
def from_probe(keyword):
    """Generate likely store names from keyword and probe if they exist."""
    stores = []
    kw = keyword.lower().replace(' ', '')
    words = keyword.lower().split()

    # Generate candidate store names
    candidates = set()
    suffixes = ['store', 'shop', 'boutique', 'market', 'goods', 'co', 
                'official', 'online', 'hub', 'world', 'zone', 'place',
                'hq', 'direct', 'plus', 'pro', 'studio', 'lab', 'us', 'uk']
    
    for suffix in suffixes:
        candidates.add(f"{kw}{suffix}")
        candidates.add(f"{kw}-{suffix}")
        for w in words:
            candidates.add(f"{w}{suffix}")
            candidates.add(f"{w}-{suffix}")
    
    # Also add some common patterns
    for i in range(1, 6):
        candidates.add(f"{kw}{i}")
        candidates.add(f"{kw}0{i}")

    log(f"   Probing {len(candidates)} generated store names...", "INFO")
    
    found = 0
    for candidate in list(candidates)[:50]:  # limit to 50 probes
        try:
            url = f"https://{candidate}.myshopify.com"
            r = requests.get(url, headers=HEADERS, timeout=5, allow_redirects=True)
            if r.status_code == 200 and 'shopify' in r.text.lower():
                stores.append(url)
                found += 1
        except:
            pass
    
    log(f"   Probe: {found} real stores found", "INFO")
    return stores

# ── Combine all methods ───────────────────────────────────────────────────────
def find_stores(keyword, country):
    all_urls = []
    seen = set()

    def add(urls):
        for u in urls:
            if u not in seen:
                seen.add(u)
                all_urls.append(u)

    log(f"🔍 Method 1: crt.sh SSL scan for '{keyword}'...", "INFO")
    add(from_crtsh(keyword))

    if len(all_urls) < 20:
        log(f"🔍 Method 2: CertSpotter scan...", "INFO")
        add(from_certspotter(keyword))

    if len(all_urls) < 20:
        log(f"🔍 Method 3: Wayback Machine scan...", "INFO")
        add(from_wayback(keyword))

    if len(all_urls) < 10:
        log(f"🔍 Method 4: Direct probe (generating store names)...", "INFO")
        add(from_probe(keyword))

    log(f"📦 Total: {len(all_urls)} candidate stores", "INFO")
    return all_urls

# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT CHECK — The ONLY thing that matters
# Shopify shows exact message: "isn't accepting payments right now"
# ─────────────────────────────────────────────────────────────────────────────

NO_PAYMENT_PHRASES = [
    "isn't accepting payments right now",
    "is not accepting payments right now",
    "not accepting payments",
    "isn't accepting payments",
    "no payment methods available",
]

PAYMENT_INDICATORS = [
    'visa', 'mastercard', 'paypal', 'credit card', 'card number',
    'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'debit card',
]

def check_payment(base_url, session):
    """
    Add product to cart → go to checkout.
    Look for Shopify's 'isn't accepting payments' message.
    Returns: 'no_payment' | 'has_payment' | 'skip'
    """
    try:
        r = session.get(base_url, headers=HEADERS, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return 'skip'
        html = r.text
        if 'cdn.shopify.com' not in html and 'shopify' not in html.lower():
            return 'skip'
        if ('/password' in r.url or 'password-page' in html.lower()):
            return 'skip'

        # Get a product
        pr = session.get(f"{base_url}/products.json?limit=1", headers=HEADERS, timeout=8)
        if pr.status_code != 200:
            return 'skip'
        products = pr.json().get('products', [])
        if not products:
            return 'skip'

        # Add to cart
        vid = products[0]['variants'][0]['id']
        session.post(f"{base_url}/cart/add.js",
                     json={"id": vid, "quantity": 1},
                     headers={**HEADERS, 'Content-Type': 'application/json'},
                     timeout=8)

        # Check checkout
        cr = session.get(f"{base_url}/checkout", headers=HEADERS, timeout=12)
        chk = cr.text.lower()

        # Shopify's exact no-payment message
        for phrase in NO_PAYMENT_PHRASES:
            if phrase in chk:
                return 'no_payment'

        # Payment found
        for ind in PAYMENT_INDICATORS:
            if ind in chk:
                return 'has_payment'

        return 'skip'
    except:
        return 'skip'

# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'shopify', '.png', '.jpg', 'noreply', 'domain.com']

def get_store_info(base_url, session):
    info = {'store_name': base_url.replace('https://', '').split('.')[0],
            'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=HEADERS, timeout=10)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        t = soup.find('title')
        if t:
            info['store_name'] = t.text.strip()[:80]
        for tag in soup.find_all('a', href=True):
            href = tag.get('href', '')
            if href.startswith('mailto:'):
                email = href[7:].split('?')[0].strip().lower()
                if '@' in email and not any(d in email for d in SKIP_EMAIL):
                    info['email'] = email
                    break
        if not info['email']:
            for m in EMAIL_RE.findall(html):
                m = m.lower()
                if not any(d in m for d in SKIP_EMAIL):
                    info['email'] = m
                    break
        phone_m = re.search(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})', html)
        if phone_m:
            info['phone'] = phone_m.group(0).strip()
        if not info['email']:
            for path in ['/pages/contact', '/contact', '/pages/about-us']:
                try:
                    pr = session.get(base_url + path, headers=HEADERS, timeout=7)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text, 'html.parser')
                        for tag in ps.find_all('a', href=True):
                            href = tag.get('href', '')
                            if href.startswith('mailto:'):
                                email = href[7:].split('?')[0].strip().lower()
                                if '@' in email and not any(d in email for d in SKIP_EMAIL):
                                    info['email'] = email
                                    break
                        if info['email']:
                            break
                except:
                    continue
    except:
        pass
    return info

def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name')} | URL: {lead.get('url')}
Problem: Their store has NO payment gateway — customers cannot pay!
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, helpful tone, soft CTA, HTML <p> tags.
Return ONLY JSON: {{"subject":"...","body":"<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400, temperature=0.7)
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
        return tpl_subject, f'<p>{tpl_body}</p>'

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
        log("🔴 Automation finished", "INFO")

def _run():
    global automation_running

    log("📋 Loading config...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script error: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — add in CFG screen", "ERROR"); return

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No keywords — add in Leads screen", "ERROR"); return

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template — add in Email screen", "ERROR"); return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0
    checked = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER — NO-PAYMENT FINDER", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | {len(ready_kws)} keywords", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🔍 [{keyword}] | [{country}]", "INFO")

        try:
            stores = find_stores(keyword, country)
        except Exception as e:
            log(f"Search error: {e}", "WARN")
            stores = []

        if not stores:
            log("⚠️  No stores found — moving to next keyword", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🧪 Testing {len(stores)} stores for payment gateway...", "INFO")

        for url in stores:
            if not automation_running or total_leads >= min_leads:
                break
            checked += 1
            try:
                result = check_payment(url, session)
                if result == 'no_payment':
                    log(f"   🎯 NO PAYMENT! → {url}", "SUCCESS")
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
                        continue
                    total_leads += 1
                    kw_leads += 1
                    log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                    time.sleep(random.uniform(1, 2))
                elif result == 'has_payment':
                    log(f"   💳 Has payment — {url[:50]}", "INFO")
            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads | checked: {checked}", "SUCCESS")

    log(f"\n📊 Done! Leads: {total_leads} | Checked: {checked}", "SUCCESS")

    # Phase 2: Email
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    leads_resp = call_sheet({'action': 'get_leads'})
    pending = [l for l in leads_resp.get('leads', [])
               if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            break
        try:
            email_to = lead['email']
            log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
            resp = call_sheet({'action': 'send_email', 'to': email_to,
                               'subject': subject, 'body': body, 'lead_id': lead.get('id')})
            if resp.get('status') == 'ok':
                log(f"   ✅ Sent!", "SUCCESS")
            else:
                log(f"   ❌ Failed: {resp.get('message', '')}", "ERROR")
            delay = random.randint(90, 150)
            log(f"   ⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except:
            continue

    log("🎉 ALL DONE!", "SUCCESS")

# ── Flask ─────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    total_leads = emails_sent = kw_total = kw_used = 0
    if os.environ.get('APPS_SCRIPT_URL'):
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
    return jsonify({'running': automation_running, 'total_leads': total_leads,
                    'emails_sent': emails_sent, 'kw_total': kw_total, 'kw_used': kw_used,
                    'script_connected': bool(os.environ.get('APPS_SCRIPT_URL'))})

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
    d = request.json
    try:
        run_time = datetime.fromisoformat(d.get('time', ''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time, id='scheduled_run', replace_existing=True)
        log(f"📅 Scheduled for {d.get('time')}", "INFO")
        return jsonify({'status': 'scheduled'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
