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

# ── Apps Script communication ─────────────────────────────────────────────────
def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            try:
                return r.json()
            except Exception:
                log(f"Sheet API warning: Google returned non-JSON data. Retrying...", "WARN")
                time.sleep(3)
                continue
        except requests.exceptions.Timeout:
            log(f"Sheet API timeout (Attempt {attempt+1}/3). Retrying...", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(3)
            
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

# ── 1. DNS & SECURITY SCRAPING (100% GOOGLE FREE, NO CRT.SH TIMEOUTS) ─────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def find_shopify_stores():
    """
    গুগল এবং crt.sh সম্পূর্ণ বাদ! 
    AlienVault, Anubis এবং URLScan থেকে হাজার হাজার রিয়েল-টাইম শপিফাই ডোমেইন আনবে।
    """
    all_urls = set()

    log(f"🚀 BYPASSING GOOGLE: Tapping into Global DNS & Cyber Security APIs...", "INFO")

    # SOURCE 1: AlienVault OTX (Passive DNS - Huge database of active domains)
    log(f"🔍 [Source 1] AlienVault: Fetching passive DNS records...", "INFO")
    try:
        r = requests.get("https://otx.alienvault.com/api/v1/indicators/domain/myshopify.com/passive_dns", timeout=15)
        if r.status_code == 200:
            for entry in r.json().get('passive_dns', []):
                hostname = entry.get('hostname', '').lower()
                if hostname.endswith('.myshopify.com') and '*' not in hostname:
                    all_urls.add(f"https://{hostname}")
    except Exception as e:
        log(f"   AlienVault timeout", "WARN")

    # SOURCE 2: Anubis (Subdomain enumeration - Extremely fast)
    log(f"🔍 [Source 2] Anubis: Enumerating myshopify subdomains...", "INFO")
    try:
        r = requests.get("https://jldc.me/anubis/subdomains/myshopify.com", timeout=15)
        if r.status_code == 200:
            for hostname in r.json():
                hostname = str(hostname).lower()
                if hostname.endswith('.myshopify.com') and '*' not in hostname:
                    all_urls.add(f"https://{hostname}")
    except Exception as e:
        log(f"   Anubis timeout", "WARN")

    # SOURCE 3: URLScan.io (Stores scanned in the last 24-48 hours)
    log(f"🔍 [Source 3] URLScan: Finding recently scanned stores...", "INFO")
    try:
        urlscan_url = "https://urlscan.io/api/v1/search/?q=domain:myshopify.com&size=500&sort=time"
        r = requests.get(urlscan_url, timeout=15)
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m:
                    all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        log(f"   URLScan timeout", "WARN")

    urls_list = list(all_urls)
    random.shuffle(urls_list) # শাফেল করে নিচ্ছি যাতে প্রতিবার নতুন স্টোর চেক করে
    log(f"📦 Found {len(urls_list)} RAW stores from DNS logs! Ready to filter by keyword.", "INFO")
    return urls_list

# ── 2. ON-SITE KEYWORD & STRICT CHECKOUT TEST ─────────────────────────────────
def check_store_target(base_url, session, keyword):
    """
    ১. স্টোরের হোমপেজে যাবে।
    ২. হোমপেজে আপনার কিওয়ার্ড (যেমন: clothing) আছে কিনা চেক করবে। না থাকলে রিজেক্ট!
    ৩. Password Page থাকলে সোজা রিজেক্ট করবে।
    ৪. Cart এ প্রোডাক্ট অ্যাড করে Checkout পেজে যাবে।
    ৫. যদি "isn't accepting payments" লেখা থাকে, শুধু তখনই লিড হিসেবে নিবে!
    """
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
            
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
            
        # 🚨 KEYWORD CHECK: ওয়েবসাইটের ভেতরে কিওয়ার্ড আছে কিনা চেক করবে
        kw_lower = keyword.lower().strip()
        if kw_lower and kw_lower not in html:
            return {"is_shopify": True, "is_lead": False, "reason": f"Keyword '{kw_lower}' not found on site"}

        # 🚨 STRICT RULE: REJECT PASSWORD PROTECTED STORES
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected (Skipping)"}

        # The Checkout Test (Add to cart -> Checkout)
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    
                    # Add to Cart
                    session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                    
                    # Go to Checkout Page
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    
                    if 'checkout' not in chk_html and 'contact information' not in chk_html and "isn't accepting payments" not in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Could not reach valid checkout page"}

                    # 🔥 THE ULTIMATE STRICT FILTER 🔥
                    error_messages = [
                        "isn't accepting payments",
                        "not accepting payments",
                        "can't accept payments",
                        "checkout is disabled"
                    ]
                    
                    for error in error_messages:
                        if error in chk_html:
                            return {"is_shopify": True, "is_lead": True, "reason": "100% Verified: Checkout Disabled Error Found!"}

                    # 🚫 যদি উপরের এরর মেসেজ না থাকে, তারমানে পেমেন্ট আছে। সোজা রিজেক্ট!
                    return {"is_shopify": True, "is_lead": False, "reason": "Active Checkout (No explicit error found)"}
                    
            return {"is_shopify": True, "is_lead": False, "reason": "Could not test checkout (No products)"}
            
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
            
    except Exception as e:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
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
        log(f"Info extraction error: {e}", "WARN")
    return result

# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
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
- Zero spam trigger words
- Mention store name once
- End with ONE soft question
- CRITICAL: Do NOT use newline characters (\\n). Use <br> for line breaks.
- Respond ONLY with valid JSON.

{{"subject": "...", "body": "..."}}"""

        headers = {
            "Authorization": f"Bearer {groq_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 500,
            "temperature": 0.7
        }
        
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=20)
        
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            raw = raw.replace('\n', ' ').replace('\r', '')
            data = json.loads(raw, strict=False)
            return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
        else:
            return tpl_subject, f'<p>{tpl_body}</p>'
            
    except Exception as e:
        return tpl_subject, f'<p>{tpl_body}</p>'

# ── Main automation ───────────────────────────────────────────────────────────
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
        log("🔴 Automation stopped (All tasks finished)", "INFO")

def _run():
    global automation_running

    # ── Load config ──────────────────────────────────────────────────────────
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})

    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR")
        return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    # ── Load keywords ────────────────────────────────────────────────────────
    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords! Add keywords in Leads screen or click Reset Used", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    # ── Load template ────────────────────────────────────────────────────────
    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates',[])
    if not templates:
        log("❌ No email template! Add one in Email screen first", "ERROR")
        return
    tpl = templates[0]
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    # ── Phase 1: Lead collection ─────────────────────────────────────────────
    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — DNS SCRAPING & ON-SITE KEYWORD CHECK", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads from {len(ready_kws)} keywords", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    # Get a massive list of raw domains ONCE to save time
    raw_store_urls = find_shopify_stores()

    for kw_row in ready_kws:
        if not automation_running:
            break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS")
            break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id   = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        if not raw_store_urls:
            log("⚠️  No raw URLs found from DNS. API might be blocked.", "WARN")
            break

        log(f"🔍 Filtering {len(raw_store_urls)} stores for keyword '{keyword}' and checking payments...", "INFO")

        for url in raw_store_urls:
            if not automation_running:
                break
            if total_leads >= min_leads:
                break

            try:
                # Step 1 & 2: Verify Shopify, Check Keyword on site, Check Checkout Page
                target_info = check_store_target(url, session, keyword)

                if not target_info.get("is_shopify"):
                    continue 

                if not target_info.get("is_lead"):
                    # Only log if it actually had the keyword but failed checkout test
                    if "Keyword" not in target_info.get('reason', ''):
                        log(f"   🚫 REJECTED: {target_info.get('reason')} - {url}", "WARN")
                    time.sleep(0.2)
                    continue

                # ✅ 100% VERIFIED NO PAYMENT FOUND!
                log(f"   🎯 {target_info.get('reason')} — collecting info...", "SUCCESS")

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
                    log(f"   ⏭️  Duplicate — already collected", "INFO")
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

    # ── Phase 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
    pending    =[l for l in all_leads
                  if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    if not pending:
        log("⚠️  No leads with emails found — check your collected leads", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN")
            break

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
            leads = lr.get('leads',[])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            kws = kr.get('keywords',[])
            kw_total = len(kws)
            kw_used  = sum(1 for k in kws if k.get('status') == 'used')
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
        return jsonify({'error': 'APPS_SCRIPT_URL not set in Render environment'})
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
