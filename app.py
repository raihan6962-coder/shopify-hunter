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

# ── Apps Script communication (Full Original with Retry Logic) ────────────────
def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
   
    # গুগল শিট স্লো থাকলে ৩ বার চেষ্টা করবে
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except Exception as e:
            if attempt == 2:
                log(f"Sheet API failed after 3 retries: {e}", "ERROR")
            time.sleep(3)
           
    return {'error': 'Sheet API failed'}

# ── Logging (Full Original Structure) ─────────────────────────────────────────
def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ── 1. ADVANCED DISCOVERY (Bypassing Google Search Entirely) ──────────────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def search_with_serpapi(query, api_key):
    """Function kept for structure, but never used anymore."""
    return []

def find_shopify_stores(keyword, country=''):
    """
    গুগল সার্চ বাদ দিয়ে সরাসরি SSL Logs এবং Subdomain Guessing ব্যবহার করে
    একদম ফ্রেশ স্টোর খুঁজে বের করার মেথড।
    """
    all_urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    
    # METHOD 1: Global SSL Certificate Logs (Finding stores created TODAY)
    log(f"🌐 Scanning Global SSL Logs for brand new '{keyword}' stores...", "INFO")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json", 
                         headers=headers, timeout=25)
        if r.status_code == 200:
            data = r.json()
            for entry in data:
                name = entry.get('name_value', '').lower()
                for domain in name.split('\n'):
                    if domain.endswith('.myshopify.com') and '*' not in domain:
                        all_urls.add(f"https://{domain}")
            log(f" ✅ SSL Logs found {len(all_urls)} potential subdomains", "SUCCESS")
    except Exception as e:
        log(f" ⚠️ SSL Log scan timed out, moving to guessing...", "WARN")

    # METHOD 2: Smart Subdomain Guessing (Brute-force for unindexed stores)
    log(f"🧠 Guessing unindexed store names for '{keyword}'...", "INFO")
    prefixes = ['', 'shop', 'the', 'my', 'get', 'buy', 'official', 'new', 'top', 'best']
    suffixes = ['', 'store', 'shop', 'boutique', 'online', 'co', 'apparel', 'deals', 'official', 'hub']
   
    for p in prefixes:
        for s in suffixes:
            if len(all_urls) > 250: 
                break
            name1 = f"{p}{kw_clean}{s}"
            name2 = f"{p}-{kw_clean}-{s}".strip('-')
            for n in [name1, name2]:
                if len(n) > 3:
                    all_urls.add(f"https://{n}.myshopify.com")

    urls_list = list(all_urls)
    random.shuffle(urls_list)
    log(f"📦 Total {len(urls_list)} fresh URLs ready for strict checkout test", "INFO")
    return urls_list

# ── 2. STRICT CHECKOUT TEST (No Password Pages, No Payment Gateways) ──────────
def check_store_target(base_url, session):
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {'User-Agent': ua}
    try:
        # Step 1: Check if store exists and is Shopify
        r = session.get(base_url, headers=headers, timeout=12, allow_redirects=True)
        if r.status_code != 200: 
            return {"is_shopify": False, "is_lead": False}
           
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
           
        # 🚨 REJECT PASSWORD PROTECTED STORES
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected (Skipped)"}

        # Step 2: The Checkout Test
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
               
                if not prod_data.get('products'):
                    return {"is_shopify": True, "is_lead": True, "reason": "Brand New Store (No products yet)"}
                
                variant_id = prod_data['products'][0]['variants'][0]['id']
                session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, 
                             headers=headers, timeout=10)
               
                chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                chk_html = chk_req.text.lower()
               
                payment_keywords = [
                    'visa', 'mastercard', 'amex', 'paypal', 'credit card',
                    'card number', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay'
                ]
                for pk in payment_keywords:
                    if pk in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": f"Active Checkout ('{pk}' found)"}
               
                return {"is_shopify": True, "is_lead": True, "reason": "Live Store -> No Payment Gateway Found!"}
                   
            return {"is_shopify": True, "is_lead": True, "reason": "Potential New Store (Products hidden)"}
           
        except Exception:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
           
    except Exception:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction (Full Original) ─────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL_DOMAINS):
                return email
    for match in EMAIL_RE.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_EMAIL_DOMAINS):
            return m
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0'}
    result = {'store_name': base_url.replace('https://', '').split('.')[0], 'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title: result['store_name'] = title.text.strip()[:80]
        result['email'] = extract_email(html, soup)
        result['phone'] = extract_phone(html)
       
        if not result['email']:
            for path in ['/pages/contact', '/contact', '/pages/about-us']:
                try:
                    pr = session.get(base_url + path, headers=headers, timeout=8)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text, 'html.parser')
                        email = extract_email(pr.text, ps)
                        if email:
                            result['email'] = email
                            break
                        if not result['phone']:
                            result['phone'] = extract_phone(pr.text)
                except: 
                    continue
    except Exception: 
        pass
    return result

# ── AI Email generation (Full Original) ───────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""You are writing a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
URL: {lead.get('url', '')}
Country: {lead.get('country', '')}
Problem: This store has NO payment gateway — customers cannot pay!
Base template:
Subject: {tpl_subject}
Body: {tpl_body}
Rules: 80-100 words MAX, no spam words, mention store name, end with ONE soft question. Use HTML <p> tags.
Respond ONLY with valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception:
        return tpl_subject, f'<p>{tpl_body}</p>'

# ── Main automation (Full Original Structure) ─────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc()[:600], "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'): return
    cfg = cfg_resp.get('config', {})
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)
    if not groq_key:
        log("❌ Groq API Key missing", "ERROR")
        return

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR")
        return

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR")
        return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — REAL-TIME DISCOVERY (NO GOOGLE)", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads from {len(ready_kws)} keywords", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads: 
            break
        keyword, kw_id = kw_row.get('keyword', ''), kw_row.get('id', '')
        kw_leads = 0
        log(f"\n🎯 Target Keyword: [{keyword}]", "INFO")
        
        store_urls = find_shopify_stores(keyword, '')  # ← এখানে serpapi_key নেই

        for url in store_urls:
            if not automation_running or total_leads >= min_leads: 
                break
            try:
                log(f" 🌐 Checking: {url}", "INFO")
                target_info = check_store_target(url, session)
                if not target_info.get("is_shopify") or not target_info.get("is_lead"):
                    if target_info.get("reason"): 
                        log(f" 🚫 REJECTED: {target_info['reason']}", "WARN")
                    continue
                log(f" 🎯 100% MATCH: {target_info['reason']}", "SUCCESS")
                info = get_store_info(url, session)
               
                save_resp = call_sheet({
                    'action': 'save_lead',
                    'store_name': info['store_name'],
                    'url': url,
                    'email': info['email'] or '',
                    'phone': info['phone'] or '',
                    'country': 'Global',
                    'keyword': keyword
                })
                if save_resp.get('status') != 'duplicate':
                    total_leads += 1
                    kw_leads += 1
                    log(f" ✅ LEAD #{total_leads} SAVED! | {info['email'] or 'no email'}", "SUCCESS")
                time.sleep(2)
            except: 
                continue
        
        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})

    # Phase 2: Email outreach
    if total_leads > 0:
        log("\n📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
        leads_resp = call_sheet({'action': 'get_leads'})
        pending = [l for l in leads_resp.get('leads', []) 
                   if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']
       
        for i, lead in enumerate(pending):
            if not automation_running: 
                break
            email_to = lead['email']
            log(f"✉️ [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
           
            send_resp = call_sheet({
                'action': 'send_email',
                'to': email_to,
                'subject': subject,
                'body': body,
                'lead_id': lead.get('id', '')
            })
            if send_resp.get('status') == 'ok': 
                log(f" ✅ Email sent", "SUCCESS")
            time.sleep(random.randint(60, 120))

    log("🎉 ALL TASKS COMPLETED!", "SUCCESS")

# ── Flask routes (Full Original - All Routes Restored) ────────────────────────
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
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return jsonify({'error': 'APPS_SCRIPT_URL not set'})
    result = call_sheet(request.json)
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
    data = request.json
    run_time_str = data.get('time', '')
    try:
        run_time = datetime.fromisoformat(run_time_str)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='scheduled_run', replace_existing=True
        )
        log(f"📅 Scheduled for {run_time_str}", "INFO")
        return jsonify({'status': 'scheduled', 'time': run_time_str})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
