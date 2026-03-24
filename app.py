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
# PHASE 1: MASSIVE DISCOVERY (NEW STORES ONLY - LAST 7 DAYS)
# ─────────────────────────────────────────────────────────────────────────────

def source_crtsh():
    """
    crt.sh থেকে একদম নতুন তৈরি হওয়া (Brand New) ৩০০০-৫০০০ স্টোর কালেক্ট করবে।
    """
    urls = set()
    log(f"   [crt.sh] Fetching massive list of BRAND NEW stores (Last 7 Days)...", "INFO")
    try:
        r = requests.get("https://crt.sh/?q=%.myshopify.com&output=json", timeout=45)
        if r.status_code == 200:
            certs = r.json()
            # Sort by ID descending (newest first) and take top 4000
            recent = sorted(certs, key=lambda x: x.get('id', 0), reverse=True)[:4000]
            for cert in recent:
                name = cert.get('common_name', '') or cert.get('name_value', '')
                m = MYSHOPIFY_RE.search(name)
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        log(f"   crt.sh error: {e}", "WARN")
    
    log(f"   crt.sh: {len(urls)} new stores collected", "INFO")
    return urls

def source_search_recent(keyword):
    """
    DuckDuckGo ব্যবহার করে গত ১ সপ্তাহে (df=w) ইনডেক্স হওয়া নির্দিষ্ট নিশের স্টোর খুঁজবে।
    """
    urls = set()
    log(f"   [Search] Finding niche '{keyword}' stores indexed in the last 7 days...", "INFO")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        data = f'q=site:myshopify.com "{keyword}"&df=w'
        r = requests.post("https://lite.duckduckgo.com/lite/", headers=headers, data=data, timeout=15)
        for m in MYSHOPIFY_RE.findall(r.text):
            urls.add(f"https://{m}.myshopify.com")
    except: pass
    log(f"   Search: {len(urls)} recent niche stores", "INFO")
    return urls

def source_urlscan_recent(keyword):
    """
    URLScan থেকে গত ৭ দিনে স্ক্যান হওয়া নির্দিষ্ট নিশের স্টোর খুঁজবে।
    """
    urls = set()
    log(f"   [URLScan] Finding recent scans (Last 7 Days) for '{keyword}'...", "INFO")
    try:
        query = f'domain:myshopify.com AND date:>now-7d AND "{keyword}"'
        r = requests.get(f"https://urlscan.io/api/v1/search/?q={query}&size=100&sort=time", timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                m = MYSHOPIFY_RE.search(res.get('page', {}).get('url', ''))
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    log(f"   URLScan: {len(urls)} stores", "INFO")
    return urls

def find_shopify_stores(keyword, country):
    all_urls = set()
    all_urls.update(source_search_recent(keyword))
    all_urls.update(source_urlscan_recent(keyword))
    all_urls.update(source_crtsh())

    total = list(all_urls)
    random.shuffle(total)
    log(f"📦 Total: {len(total)} NEW stores. Filtering by niche '{keyword}' & checking checkout...", "SUCCESS")
    return total

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2: NICHE FILTER & CHECKOUT HTML ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session, keyword):
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
    try:
        # Fast Niche Check
        r = session.get(base_url, headers=headers, timeout=5, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
            
        html_lower = r.text.lower()
        if 'shopify' not in html_lower and 'cdn.shopify.com' not in html_lower:
            return {"is_shopify": False, "is_lead": False}

        # 🚨 NICHE CHECK: হোমপেজে কিওয়ার্ড না থাকলে স্কিপ করবে
        kw_lower = keyword.lower().strip()
        if kw_lower and kw_lower not in html_lower:
            return {"is_shopify": True, "is_lead": False, "reason": "Keyword missing"}

        is_password = '/password' in r.url or 'password-page' in html_lower

        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}

            products = prod_req.json().get('products', [])

            if not products:
                chk = session.get(f"{base_url}/checkout", headers=headers, timeout=10, allow_redirects=True)
                chk_lower = chk.text.lower()
                for phrase in ["isn't accepting payments", "not accepting payments",
                                "no payment methods", "payment provider hasn't been set up",
                                "this store is unavailable", "unable to process payment"]:
                    if phrase in chk_lower:
                        return {"is_shopify": True, "is_lead": True, "reason": f"0 products + '{phrase}'"}
                if is_password:
                    return {"is_shopify": True, "is_lead": True, "reason": "Password + 0 products = new store"}
                return {"is_shopify": True, "is_lead": False, "reason": "0 products, unclear"}

            variant_id = products[0]['variants'][0]['id']
            session.post(f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'}, timeout=10)

            chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15, allow_redirects=True)
            chk_html = chk_req.text
            chk_lower = chk_html.lower()

            for phrase in ["isn't accepting payments", "not accepting payments",
                           "no payment methods", "payment provider hasn't been set up",
                           "this store is unavailable"]:
                if phrase in chk_lower:
                    return {"is_shopify": True, "is_lead": True, "reason": f"CONFIRMED: '{phrase}'"}

            payment_kws = ['visa', 'mastercard', 'amex', 'american express',
                'paypal', 'credit card', 'debit card', 'card number',
                'stripe', 'klarna', 'afterpay', 'shop pay', 'shoppay',
                'apple pay', 'google pay', 'discover', 'diners',
                'card-fields', 'payment-method', 'pay with']
            found_pay = [kw for kw in payment_kws if kw in chk_lower]
            if found_pay:
                return {"is_shopify": True, "is_lead": False, "reason": f"has: {found_pay[:2]}"}

            if base_url.replace('https://', '') in chk_req.url and '/checkout' not in chk_req.url:
                return {"is_shopify": True, "is_lead": True, "reason": "Redirected from checkout = no payment"}

            if any(s in chk_lower for s in ['contact information', 'shipping address',
                                              'order summary', 'express checkout', 'your email']):
                return {"is_shopify": True, "is_lead": True, "reason": "Checkout OK, no payment options"}

            return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive"}

        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"error: {e}"}
    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL + PHONE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
              '.svg', 'noreply', 'domain.com', 'no-reply', 'schema.org', 'w3.org']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def is_valid_email(email):
    e = email.lower()
    if any(s in e for s in SKIP_EMAIL): return False
    parts = e.split('@')
    if len(parts) != 2 or not parts[0] or '.' not in parts[1]: return False
    return 2 <= len(parts[1].split('.')[-1]) <= 6

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if is_valid_email(e): return e
    for match in EMAIL_RE.findall(html):
        if is_valid_email(match): return match.lower()
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    result = {'store_name': base_url.replace('https://', '').split('.')[0],
               'email': None, 'phone': None}
    pages = ['', '/pages/contact', '/pages/contact-us', '/contact',
             '/pages/about-us', '/pages/about', '/pages/faq',
             '/pages/help', '/pages/support',
             '/policies/contact-information', '/policies/refund-policy']
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
                e = extract_email(html, soup)
                if e:
                    result['email'] = e
            if not result['phone']:
                result['phone'] = extract_phone(html)
        except: continue

    if not result['email']:
        try:
            r = session.get(base_url, headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or '{}')
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        e = item.get('email', '') or item.get('contactPoint', {}).get('email', '')
                        if e and is_valid_email(e):
                            result['email'] = e.lower()
                            break
                except: continue
        except: pass
    return result


# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: NO payment gateway configured.
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
            raw = raw.replace('\n', ' ').replace('\r', '')
            data = json.loads(raw, strict=False)
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
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key  = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

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
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — HUNTING NEW NO-PAYMENT STORES", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Filtering strictly by Niche", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        
        kw_leads = rej_pay = rej_other = rej_keyword = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country)
        except Exception as e:
            log(f"Search failed: {e}", "WARN"); store_urls = []

        if not store_urls:
            log("⚠️  No URLs found for this keyword. Moving to next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                result = check_store_target(url, session, keyword)
                if not result.get("is_shopify"): 
                    continue

                if not result.get("is_lead"):
                    reason = result.get('reason', '')
                    
                    if "Keyword missing" in reason:
                        rej_keyword += 1
                    elif any(w in reason.lower() for w in ['has:', 'payment', 'visa', 'stripe']):
                        rej_pay += 1
                    else:
                        rej_other += 1
                        
                    processed_count = idx + 1
                    if processed_count % 10 == 0:
                        log(f"   ⚡ Progress: [{processed_count}/{len(store_urls)}] Checked | Niche Mismatch: {rej_keyword} | Paid: {rej_pay}", "INFO")
                    continue

                # ✅ LEAD FOUND!
                log(f"   🎯 100% MATCH: {result.get('reason')} — collecting info...", "SUCCESS")
                info = get_store_info(url, session)
                
                save_resp = call_sheet({
                    'action': 'save_lead', 'store_name': info['store_name'],
                    'url': url, 'email': info['email'] or '',
                    'phone': info['phone'] or '', 'country': country, 'keyword': keyword
                })
                
                if save_resp.get('error'):
                    log(f"   Sheet error: {save_resp['error']}", "WARN"); continue
                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️  Duplicate", "INFO"); continue

                total_leads += 1; kw_leads += 1
                email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                phone_str = f"| 📞 {info['phone']}" if info['phone'] else ""
                log(f"   ✅ LEAD #{total_leads} SAVED → {info['store_name']} | {email_str} {phone_str}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 3 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(10)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email',''))
                  and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    if not pending:
        log("⚠️  No leads with emails found — check your collected leads", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to,
            'subject': subject, 'body': body, 'lead_id': lead.get('id', '')
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
    global last_status_fetch, cached_status
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    
    # 🚨 FIX: Dashboard will only fetch data from Google Sheets once every 60 seconds
    if script_url and (time.time() - last_status_fetch > 60):
        try:
            # Using direct requests instead of call_sheet to prevent "Sheet timeout" logs in terminal
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
            pass # Fail silently, keep old cache so it doesn't spam logs

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
