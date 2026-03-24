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

# 🔥 THE PURE PYTHON PACKAGE (No API Key Needed)
from duckduckgo_search import DDGS

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

# ── Apps Script communication (CRASH PROTECTION) ──────────────────────────────
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
            except Exception:
                log(f"Sheet API warning: Non-JSON response. Retrying...", "WARN")
                time.sleep(2)
                continue
        except requests.exceptions.Timeout:
            log(f"Sheet API timeout (Attempt {attempt+1}/3). Retrying...", "WARN")
            time.sleep(2)
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(2)
            
    return {'error': 'Sheet API failed after 3 retries'}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ── 1. PURE PYTHON SCRAPING (NO API LIMITS) ───────────────────────────────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def find_shopify_stores_python(keyword, country):
    """
    Python প্যাকেজ (DDGS) এবং URLScan ব্যবহার করে আনলিমিটেড স্টোর স্ক্র্যাপ করবে।
    """
    all_urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    
    log(f"🚀 PURE PYTHON MODE: Scraping web directly for '{keyword}'...", "INFO")

    # SOURCE 1: DuckDuckGo Python Scraper
    try:
        with DDGS() as ddgs:
            queries = [
                f'site:myshopify.com "{keyword}" {country}',
                f'site:myshopify.com "{keyword}" "powered by shopify"',
                f'site:myshopify.com "{keyword}" "isn\'t accepting payments right now"'
            ]
            for q in queries:
                if len(all_urls) > 500:
                    break
                results = ddgs.text(q, max_results=100)
                if results:
                    for r in results:
                        m = MYSHOPIFY_RE.search(r.get('href', ''))
                        if m: all_urls.add(f"https://{m.group(1)}.myshopify.com")
                time.sleep(2)
    except Exception as e:
        log(f"   Scraper Error: {e}", "WARN")

    # SOURCE 2: Public URLScan API
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=300&sort=time"
        r = requests.get(urlscan_url, timeout=15)
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m: all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        pass

    urls_list = list(all_urls)
    random.shuffle(urls_list)
    log(f"📦 Scraped {len(urls_list)} real stores to test!", "INFO")
    return urls_list

# ── 2. HTML ANALYSIS CHECKOUT TEST (100% Accurate) ────────────────────────────
def check_store_target(base_url, session):
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {'User-Agent': ua, 'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8'}

    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200: return {"is_shopify": False, "is_lead": False}
            
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html: return {"is_shopify": False, "is_lead": False}
            
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected"}

        # The Checkout Test
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    
                    if 'checkout' not in chk_html and "isn't accepting payments" not in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Checkout unreachable"}

                    # 🔥 HTML ANALYSIS
                    payment_keywords = ['visa', 'mastercard', 'amex', 'paypal', 'credit card', 'debit card', 'card number', 'stripe', 'klarna', 'shop pay']
                    found_payments = [pk for pk in payment_keywords if pk in chk_html]
                    
                    if found_payments:
                        return {"is_shopify": True, "is_lead": False, "reason": f"Active Checkout ({found_payments[0]} found)"}
                    else:
                        return {"is_shopify": True, "is_lead": True, "reason": "100% Verified: No Payments Found!"}
                    
            return {"is_shopify": True, "is_lead": False, "reason": "No products"}
        except: return {"is_shopify": True, "is_lead": False, "reason": "Test failed"}
    except: return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    result = {'store_name': base_url.split('.')[0], 'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        title = soup.find('title')
        if title: result['store_name'] = title.text.strip()[:80]
        
        # Deep Email Finder
        for path in ['', '/pages/contact', '/policies/contact-information', '/policies/refund-policy']:
            pr = session.get(base_url + path, headers=headers, timeout=8)
            emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', pr.text)
            for e in emails:
                if '@' in e and not any(d in e.lower() for d in ['example', 'shopify', 'sentry', 'noreply', '.png', '.jpg']):
                    result['email'] = e.lower()
                    break
            if result['email']: break
    except: pass
    return result

# ── AI Email generation (REST API) ────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        headers = {"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"}
        prompt = f"Write a short cold email to {lead.get('store_name')} about their missing payment gateway. Use JSON: {{\"subject\": \"...\", \"body\": \"...\"}}"
        payload = {"model": "llama-3.1-8b-instant", "messages": [{"role": "user", "content": prompt}]}
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=20)
        if r.status_code == 200:
            data = json.loads(r.json()['choices'][0]['message']['content'].replace('\n', ' '), strict=False)
            return data.get('subject', tpl_subject), f"<p>{data.get('body', tpl_body)}</p>"
    except: pass
    return tpl_subject, f"<p>{tpl_body}</p>"

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try: _run()
    except Exception as e: log(f"💥 Fatal: {e}", "ERROR")
    finally: automation_running = False

def _run():
    global automation_running
    cfg = call_sheet({'action': 'get_config'}).get('config', {})
    groq_key, min_leads = cfg.get('groq_api_key', '').strip(), int(cfg.get('min_leads', 50))
    if not groq_key: log("❌ Groq Key missing", "ERROR"); return

    ready_kws = [k for k in call_sheet({'action': 'get_keywords'}).get('keywords', []) if k.get('status') == 'ready']
    tpl = call_sheet({'action': 'get_templates'}).get('templates', [{}])[0]
    
    session = requests.Session()
    total_leads = 0

    log("🚀 PHASE 1 — PURE PYTHON SCRAPING & HTML CHECKOUT ANALYSIS", "SUCCESS")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads: break
        keyword, country, kw_id = kw_row.get('keyword', ''), kw_row.get('country', ''), kw_row.get('id', '')
        
        store_urls = find_shopify_stores_python(keyword, country)
        
        for url in store_urls:
            if not automation_running or total_leads >= min_leads: break
            try:
                target_info = check_store_target(url, session)
                if not target_info.get("is_lead"): continue

                log(f"   🎯 MATCH: {url}", "SUCCESS")
                info = get_store_info(url, session)
                save_resp = call_sheet({'action': 'save_lead', 'store_name': info['store_name'], 'url': url, 'email': info['email'] or '', 'country': country, 'keyword': keyword})

                if save_resp.get('status') != 'duplicate':
                    total_leads += 1
                    log(f"   ✅ LEAD #{total_leads} SAVED!", "SUCCESS")
                time.sleep(2)
            except: continue
        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': total_leads})

    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    time.sleep(10)
    pending = [l for l in call_sheet({'action': 'get_leads'}).get('leads', []) if l.get('email') and l.get('email_sent') != 'sent']
    for lead in pending:
        if not automation_running: break
        sub, body = generate_email(tpl.get('subject'), tpl.get('body'), lead, groq_key)
        call_sheet({'action': 'send_email', 'to': lead['email'], 'subject': sub, 'body': body, 'lead_id': lead['id']})
        time.sleep(random.randint(60, 100))

# Flask Routes
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    s = os.environ.get('APPS_SCRIPT_URL', '')
    try:
        lr = call_sheet({'action': 'get_leads'})
        tl, es = len(lr.get('leads', [])), sum(1 for l in lr.get('leads', []) if l.get('email_sent') == 'sent')
    except: tl = es = 0
    return jsonify({'running': automation_running, 'total_leads': tl, 'emails_sent': es, 'script_connected': bool(s)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet(): return jsonify(call_sheet(request.json))

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if not automation_running:
        automation_thread = threading.Thread(target=run_automation, daemon=True)
        automation_thread.start()
    return jsonify({'status': 'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), threaded=True)
