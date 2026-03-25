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

# Dashboard Caching Variables
last_status_fetch = 0
cached_status = {'total_leads': 0, 'emails_sent': 0, 'kw_total': 0, 'kw_used': 0}

# ── Apps Script communication ─────────────────────────────────────────────────
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
            log(f"Sheet API timeout (Attempt {attempt+1}/3). Retrying...", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(3)
            
    return {'error': 'Sheet API failed after 3 retries'}

def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# 1. ULTIMATE STORE DISCOVERY (Best Sources for Live & New Stores)
# ─────────────────────────────────────────────────────────────────────────────

def source_crtsh_targeted(keyword):
    urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    log(f"   [crt.sh] Searching SSL logs for '{kw_clean}' in domain...", "INFO")
    try:
        r = requests.get("https://crt.sh/", params={'q': f'%{kw_clean}%.myshopify.com', 'output': 'json'}, timeout=30)
        if r.status_code == 200:
            for cert in r.json():
                name = cert.get('common_name', '') or cert.get('name_value', '')
                m = MYSHOPIFY_RE.search(name)
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    log(f"   crt.sh (Targeted): {len(urls)} stores", "INFO")
    return urls

def source_commoncrawl(keyword):
    urls = set()
    log(f"   [CommonCrawl] Searching massive live database for '{keyword}'...", "INFO")
    indexes = ['CC-MAIN-2025-08', 'CC-MAIN-2024-51', 'CC-MAIN-2024-46']
    for idx in indexes:
        if len(urls) >= 300: break
        for kv in [keyword.replace(' ', '-'), keyword.replace(' ', ''), keyword]:
            try:
                r = requests.get(
                    f"https://index.commoncrawl.org/{idx}-index",
                    params={'url': f'*.myshopify.com/*{kv}*', 'output': 'json', 'limit': 500, 'fl': 'url'},
                    timeout=20)
                if r.status_code == 200 and r.text.strip():
                    for line in r.text.strip().split('\n'):
                        try:
                            m = MYSHOPIFY_RE.search(json.loads(line).get('url', ''))
                            if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                        except: continue
            except: pass
            time.sleep(0.5)
    log(f"   CommonCrawl: {len(urls)} stores", "INFO")
    return urls

def source_urlscan(keyword):
    urls = set()
    log(f"   [URLScan] Finding recently active stores for '{keyword}'...", "INFO")
    try:
        query = f'domain:myshopify.com AND "{keyword}"'
        r = requests.get(f"https://urlscan.io/api/v1/search/?q={query}&size=300&sort=time", timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                m = MYSHOPIFY_RE.search(res.get('page', {}).get('url', ''))
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    log(f"   URLScan: {len(urls)} stores", "INFO")
    return urls

def source_duckduckgo(keyword):
    urls = set()
    log(f"   [DuckDuckGo] Finding niche stores indexed in the last 7 days...", "INFO")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        data = f'q=site:myshopify.com "{keyword}"&df=w'
        r = requests.post("https://lite.duckduckgo.com/lite/", headers=headers, data=data, timeout=15)
        for m in MYSHOPIFY_RE.finditer(r.text):
            urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    log(f"   DuckDuckGo: {len(urls)} stores", "INFO")
    return urls

def source_bing(keyword):
    urls = set()
    log(f"   [Bing] Searching for '{keyword}'...", "INFO")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36'}
    queries = [f'site:myshopify.com "{keyword}" "opening soon"', f'site:myshopify.com "{keyword}"']
    session = requests.Session()
    for q in queries:
        if len(urls) >= 150: break
        for first in [1, 11, 21]:
            try:
                r = session.get('https://www.bing.com/search', params={'q': q, 'first': first, 'count': 10}, headers=headers, timeout=10)
                if r.status_code == 200:
                    for m in MYSHOPIFY_RE.finditer(r.text):
                        urls.add(f"https://{m.group(1)}.myshopify.com")
            except: pass
            time.sleep(1)
    log(f"   Bing: {len(urls)} stores", "INFO")
    return urls

def source_crtsh_recent():
    urls = set()
    log(f"   [crt.sh] Fetching recent 2000 stores as fallback...", "INFO")
    try:
        r = requests.get("https://crt.sh/", params={'q': '%.myshopify.com', 'output': 'json'}, timeout=30)
        if r.status_code == 200:
            certs = r.json()
            recent = sorted(certs, key=lambda x: x.get('id', 0), reverse=True)[:2000]
            for cert in recent:
                name = cert.get('common_name', '') or cert.get('name_value', '')
                m = MYSHOPIFY_RE.search(name)
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    log(f"   crt.sh (Fallback): {len(urls)} stores", "INFO")
    return urls

def find_shopify_stores(keyword, country):
    all_urls = set()
    all_urls.update(source_duckduckgo(keyword))
    all_urls.update(source_bing(keyword))
    all_urls.update(source_urlscan(keyword))
    all_urls.update(source_commoncrawl(keyword))
    all_urls.update(source_crtsh_targeted(keyword))
    
    if len(all_urls) < 300:
        all_urls.update(source_crtsh_recent())

    total = list(all_urls)
    random.shuffle(total)
    log(f"📦 Total: {len(total)} High-Quality stores found! Checking checkout...", "SUCCESS")
    return total

# ─────────────────────────────────────────────────────────────────────────────
# 2. STRICT CHECKOUT TEST
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session, keyword):
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        r = session.get(base_url, headers=headers, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
            
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
            
        kw_lower = keyword.lower().strip()
        kw_clean = kw_lower.replace(' ', '')
        in_url = kw_clean in base_url.lower()
        in_html = kw_lower in html
        
        if not (in_url or in_html):
            return {"is_shopify": True, "is_lead": False, "reason": "Keyword missing"}

        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected"}

        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    
                    session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                    
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    
                    if 'checkout' not in chk_html and 'contact information' not in chk_html and "isn't accepting payments" not in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Could not reach valid checkout page"}

                    payment_keywords =[
                        'visa', 'mastercard', 'amex', 'paypal', 'credit card', 
                        'debit card', 'card number', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay'
                    ]
                    
                    for pk in payment_keywords:
                        if pk in chk_html:
                            return {"is_shopify": True, "is_lead": False, "reason": f"Active Checkout ('{pk}' found)"}
                    
                    if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html:
                        return {"is_shopify": True, "is_lead": True, "reason": "Live Store -> Checkout Disabled (Explicit Error)!"}
                    
                    return {"is_shopify": True, "is_lead": True, "reason": "No Payment Options Found on Checkout!"}
                    
            return {"is_shopify": True, "is_lead": False, "reason": "No products to add"}
            
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
            
    except Exception as e:
        return {"is_shopify": False, "is_lead": False}

# ─────────────────────────────────────────────────────────────────────────────
# 3. STORE INFO EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0',
        'Accept': 'text/html,*/*;q=0.8',
    }
    result = {
        'store_name': base_url.replace('https://', '').split('.')[0],
        'email': None,
        'phone': None,
    }
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title:
            result['store_name'] = title.text.strip()[:80]
        result['email'] = extract_email(html, soup)
        result['phone'] = extract_phone(html)
        
        if not result['email']:
            for path in['/pages/contact', '/contact', '/pages/about-us']:
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
    except Exception as e:
        pass
    return result

# ─────────────────────────────────────────────────────────────────────────────
# 4. AI EMAIL GENERATION
# ─────────────────────────────────────────────────────────────────────────────
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

Rules:
- 80-100 words MAX
- Zero spam trigger words (FREE, GUARANTEED, ACT NOW, etc.)
- Mention store name once, naturally
- Helpful tone, not pushy
- End with ONE soft question
- Use HTML <p> tags

Respond ONLY with valid JSON, nothing else:
{{"subject": "...", "body": "<p>...</p><p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        return tpl_subject, f'<p>{tpl_body}</p>'

# ─────────────────────────────────────────────────────────────────────────────
# 5. MAIN AUTOMATION FLOW
# ─────────────────────────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL ERROR: {e}", "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})

    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR")
        return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates',[])
    if not templates:
        log("❌ No email template!", "ERROR")
        return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — FINDING FRESH STORES & CHECKING CHECKOUT", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads: break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id   = kw_row.get('id', '')
        
        kw_leads = rej_pay = rej_pass_noprod = rej_keyword = rej_not_shopify = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country)
        except Exception as e:
            store_urls =[]

        if not store_urls:
            log("⚠️  No URLs found. Moving to next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                target_info = check_store_target(url, session, keyword)

                if not target_info.get("is_shopify"):
                    rej_not_shopify += 1
                    processed_count = idx + 1
                    if processed_count % 10 == 0:
                        log(f"   ⚡ Progress: [{processed_count}/{len(store_urls)}] | Dead/Not Shopify: {rej_not_shopify} | No Niche: {rej_keyword} | Pass/NoProd: {rej_pass_noprod} | Paid: {rej_pay}", "INFO")
                    continue 

                if not target_info.get("is_lead"):
                    reason = target_info.get('reason', '').lower()
                    
                    if "keyword missing" in reason:
                        rej_keyword += 1
                    elif "active checkout" in reason or "payment" in reason:
                        rej_pay += 1
                    elif "password" in reason or "no products" in reason:
                        rej_pass_noprod += 1
                        
                    processed_count = idx + 1
                    if processed_count % 10 == 0:
                        log(f"   ⚡ Progress: [{processed_count}/{len(store_urls)}] | Dead/Not Shopify: {rej_not_shopify} | No Niche: {rej_keyword} | Pass/NoProd: {rej_pass_noprod} | Paid: {rej_pay}", "INFO")
                    continue

                # ✅ LEAD FOUND!
                log(f"   🎯 100% MATCH: {target_info.get('reason')} — collecting info...", "SUCCESS")

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
                    log(f"   ⏭️  Duplicate", "INFO")
                    continue

                total_leads += 1
                kw_leads += 1
                email_display = info['email'] or '⚠ no email found'
                log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_display}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(5)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
    pending    =[l for l in all_leads if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break

        email_to = lead['email']
        log(f"✉️[{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")

        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)

        send_resp = call_sheet({
            'action': 'send_email',
            'to': email_to,
            'subject': subject,
            'body': body,
            'lead_id': lead.get('id', '')
        })

        if send_resp.get('status') == 'ok':
            log(f"   ✅ Email sent", "SUCCESS")
        else:
            log(f"   ❌ Send failed", "ERROR")

        delay = random.randint(90, 150)
        log(f"   ⏳ Waiting {delay}s...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE!", "SUCCESS")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    global last_status_fetch, cached_status
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    
    if script_url and (time.time() - last_status_fetch > 60):
        try:
            r1 = requests.post(script_url, json={'action': 'get_leads'}, timeout=15)
            if r1.status_code == 200:
                leads = r1.json().get('leads', [])
                cached_status['total_leads'] = len(leads)
                cached_status['emails_sent'] = sum(1 for l in leads if l.get('email_sent') == 'sent')
            
            r2 = requests.post(script_url, json={'action': 'get_keywords'}, timeout=15)
            if r2.status_code == 200:
                kws = r2.json().get('keywords', [])
                cached_status['kw_total'] = len(kws)
                cached_status['kw_used'] = sum(1 for k in kws if k.get('status') == 'used')
                
            last_status_fetch = time.time()
        except:
            pass 

    return jsonify({
        'running': automation_running, 
        'total_leads': cached_status['total_leads'],
        'emails_sent': cached_status['emails_sent'], 
        'kw_total': cached_status['kw_total'],
        'kw_used': cached_status['kw_used'], 
        'script_connected': bool(script_url)
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
        return jsonify({'status': 'scheduled', 'time': run_time_str})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
