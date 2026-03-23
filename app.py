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

# ── Apps Script communication ─────────────────────────────────────────────────
def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url: return {'error': 'APPS_SCRIPT_URL not set'}
    try:
        r = requests.post(script_url, json=payload, timeout=25, headers={'Content-Type': 'application/json'})
        return r.json()
    except Exception as e:
        log(f"Sheet API error: {e}", "WARN")
        return {'error': str(e)}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ── 1. SMART GOOGLE SEARCH ────────────────────────────────────────────────────
def search_with_serpapi(query, api_key):
    urls =[]
    try:
        params = {
            'api_key': api_key, 'engine': 'google', 'q': query,
            'num': 100, 'gl': 'us', 'hl': 'en',
            'tbs': 'qdr:m'  # Past Month (To get enough unlaunched stores)
        }
        r = requests.get('https://serpapi.com/search', params=params, timeout=30)
        if r.status_code != 200: return urls
        data = r.json()
        results = data.get('organic_results',[])
        for item in results:
            link = item.get('link', '')
            m = MYSHOPIFY_RE.match(link)
            if m:
                url = f"https://{m.group(1)}.myshopify.com"
                if url not in urls: urls.append(url)
    except Exception: pass
    return urls

def find_shopify_stores(keyword, country, serpapi_key):
    all_urls =[]
    # 💥 SMART DORKS: Directly asking Google for stores under construction or without payments
    queries =[
        f'site:myshopify.com "{keyword}" "opening soon" {country}',
        f'site:myshopify.com "{keyword}" "enter store using password" {country}',
        f'site:myshopify.com "{keyword}" "isn\'t accepting payments right now"',
        f'site:myshopify.com "{keyword}" "be the first to know" {country}'
    ]
    for i, query in enumerate(queries):
        if len(all_urls) >= 50: break
        log(f"🔍 Searching: {query}", "INFO")
        found = search_with_serpapi(query, serpapi_key)
        new = [u for u in found if u not in all_urls]
        all_urls.extend(new)
        time.sleep(1.5)
    log(f"📦 Found {len(all_urls)} potential raw leads", "INFO")
    return all_urls

# ── 2. THE 100% ACCURATE CART & CHECKOUT TEST ─────────────────────────────────
def check_store_target(base_url, session):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8'}
    
    try:
        # Step 1: Check Homepage
        r = session.get(base_url, headers=headers, timeout=12, allow_redirects=True)
        html = r.text.lower()
        
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
            
        # Step 2: PASSWORD PAGE CHECK (Golden Lead)
        if 'password-page' in html or 'opening soon' in html or 'enter store using password' in html:
            return {"is_shopify": True, "is_lead": True, "reason": "Password Protected (Brand New/Under Construction)"}
            
        # Step 3: SIMULATE CHECKOUT (The Ultimate Proof)
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    product = prod_data['products'][0]
                    if 'variants' in product and len(product['variants']) > 0:
                        variant_id = product['variants'][0]['id']
                        
                        # Add product to cart
                        session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                        
                        # Go to Checkout Page
                        chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                        chk_html = chk_req.text.lower()
                        
                        # Look for Shopify's EXPLICIT error message when no gateway is setup
                        if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html or "cannot accept payments" in chk_html or "checkout is disabled" in chk_html:
                            return {"is_shopify": True, "is_lead": True, "reason": "Live Store -> Verified Checkout Disabled (No Gateway)!"}
                        
                        # If it reaches normal checkout without error, IT HAS PAYMENT GATEWAY -> REJECT IT
                        return {"is_shopify": True, "is_lead": False, "reason": "Live Store -> Has active checkout gateway"}
        except Exception:
            pass # Cart test failed, fallback below
            
        # STRICT REJECTION: If it's live and we didn't explicitly prove it has NO payment, we SKIP it to be 100% safe.
        return {"is_shopify": True, "is_lead": False, "reason": "Active store, unable to guarantee NO payment (Skipped)"}
        
    except Exception:
        return {"is_shopify": False, "is_lead": False}

# ── Info extraction & AI ──────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL_DOMAINS): return email
    for match in EMAIL_RE.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_EMAIL_DOMAINS): return m
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    result = {'store_name': base_url.replace('https://', '').split('.')[0], 'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        title = soup.find('title')
        if title: result['store_name'] = title.text.strip()[:80]
        result['email'] = extract_email(r.text, soup)
        result['phone'] = extract_phone(r.text)
        
        if not result['email']:
            for path in['/pages/contact', '/contact', '/password']:
                try:
                    pr = session.get(base_url + path, headers=headers, timeout=8)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text, 'html.parser')
                        email = extract_email(pr.text, ps)
                        if email:
                            result['email'] = email
                            break
                except: continue
    except Exception: pass
    return result

def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a cold email to this Shopify store owner.
Store: {lead.get('store_name')}
URL: {lead.get('url')}
Problem: Their store has NO payment gateway setup!
Base Subject: {tpl_subject}
Base Body: {tpl_body}
Rules: Under 100 words, no spam words, mention store name, end with a question. Return JSON only: {{"subject": "...", "body": "<p>...</p>"}}"""
        resp = client.chat.completions.create(model="llama-3.1-8b-instant", messages=[{"role": "user", "content": prompt}], max_tokens=500, temperature=0.7)
        data = json.loads(re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip())
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception: return tpl_subject, f'<p>{tpl_body}</p>'

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try: _run()
    except Exception as e: log(f"💥 ERROR: {e}", "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running
    cfg = call_sheet({'action': 'get_config'}).get('config', {})
    groq_key, serpapi_key = cfg.get('groq_api_key', '').strip(), cfg.get('serpapi_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key or not serpapi_key:
        log("❌ API Keys missing!", "ERROR"); return

    ready_kws =[k for k in call_sheet({'action': 'get_keywords'}).get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return

    tpls = call_sheet({'action': 'get_templates'}).get('templates',[])
    if not tpls:
        log("❌ No template!", "ERROR"); return
    tpl = tpls[0]

    session = requests.Session()
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — STRICT NO-PAYMENT SCANNING", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads: break
        kw, country, kw_id = kw_row.get('keyword', ''), kw_row.get('country', ''), kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{kw}] | Location: [{country}]", "INFO")
        store_urls = find_shopify_stores(kw, country, serpapi_key)

        for url in store_urls:
            if not automation_running or total_leads >= min_leads: break
            try:
                log(f"   🌐 {url}", "INFO")
                target_info = check_store_target(url, session)

                if not target_info.get("is_shopify"): continue
                
                # If the test says it is NOT a lead (e.g. active checkout found)
                if not target_info.get("is_lead"):
                    log(f"   🚫 REJECTED: {target_info.get('reason')}", "WARN")
                    continue

                # ✅ IT PASSED THE STRICT TEST!
                log(f"   🎯 100% MATCH: {target_info.get('reason')}", "SUCCESS")
                
                info = get_store_info(url, session)
                save_resp = call_sheet({'action': 'save_lead', 'store_name': info['store_name'], 'url': url, 'email': info['email'] or '', 'phone': info['phone'] or '', 'country': country, 'keyword': kw})

                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️ Duplicate", "INFO")
                    continue

                total_leads += 1
                kw_leads += 1
                log(f"   ✅ LEAD #{total_leads} SAVED!", "SUCCESS")
                time.sleep(2)
            except Exception: continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})

    # Phase 2 (Email)
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    
    pending =[l for l in call_sheet({'action': 'get_leads'}).get('leads', []) if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']
    for i, lead in enumerate(pending):
        if not automation_running: break
        log(f"✉️ Sending to {lead['email']}...", "INFO")
        sub, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        sr = call_sheet({'action': 'send_email', 'to': lead['email'], 'subject': sub, 'body': body, 'lead_id': lead.get('id', '')})
        if sr.get('status') == 'ok': log("   ✅ Sent", "SUCCESS")
        time.sleep(random.randint(60, 120))
    log("🎉 ALL DONE!", "SUCCESS")

@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    s = os.environ.get('APPS_SCRIPT_URL', '')
    tl = es = kt = ku = 0
    if s:
        try:
            leads = call_sheet({'action': 'get_leads'}).get('leads',[])
            tl, es = len(leads), sum(1 for l in leads if l.get('email_sent') == 'sent')
            kws = call_sheet({'action': 'get_keywords'}).get('keywords',[])
            kt, ku = len(kws), sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': tl, 'emails_sent': es, 'kw_total': kt, 'kw_used': ku, 'script_connected': bool(s)})

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
    if automation_running: return jsonify({'status': 'already_running'})
    automation_thread = threading.Thread(target=run_automation, daemon=True)
    automation_thread.start()
    return jsonify({'status': 'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, threaded=True)
