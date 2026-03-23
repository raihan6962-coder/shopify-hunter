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
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    try:
        r = requests.post(script_url, json=payload, timeout=25,
                          headers={'Content-Type': 'application/json'})
        return r.json()
    except Exception as e:
        log(f"Sheet API error: {e}", "WARN")
        return {'error': str(e)}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ── SerpAPI Search ────────────────────────────────────────────────────────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def search_with_serpapi(query, api_key):
    """Search Google via SerpAPI — works from any IP, 100 free/month."""
    urls =[]
    try:
        params = {
            'api_key': api_key,
            'engine': 'google',
            'q': query,
            'num': 100,
            'gl': 'us',
            'hl': 'en',
            'tbs': 'qdr:m'  # Past Month: শুধু গত ১ মাসের নতুন স্টোরগুলো আনবে
        }
        r = requests.get('https://serpapi.com/search', params=params, timeout=30)
        if r.status_code != 200:
            log(f"SerpAPI HTTP {r.status_code}: {r.text[:120]}", "WARN")
            return urls
        data = r.json()
        
        # Check for API-level error
        if data.get('error'):
            log(f"SerpAPI error: {data['error']}", "WARN")
            return urls
            
        results = data.get('organic_results',[])
        log(f"   SerpAPI returned {len(results)} results", "INFO")
        
        for item in results:
            link = item.get('link', '')
            m = MYSHOPIFY_RE.match(link)
            if m:
                url = f"https://{m.group(1)}.myshopify.com"
                if url not in urls:
                    urls.append(url)
    except Exception as e:
        log(f"SerpAPI exception: {e}", "WARN")
    return urls

def find_shopify_stores(keyword, country, serpapi_key):
    """Run multiple SerpAPI queries to collect myshopify.com URLs."""
    all_urls =[]
    # ব্রড সার্চ কোয়েরি যাতে বট পর্যাপ্ত ওয়েবসাইট পায় এবং হুট করে বন্ধ না হয়
    queries =[
        f'site:myshopify.com {keyword} {country}',
        f'site:myshopify.com "{keyword}" "password" {country}',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com {keyword}'
    ]
    for i, query in enumerate(queries):
        if len(all_urls) >= 80:
            break
        log(f"🔍 Query {i+1}/{len(queries)}: {query}", "INFO")
        found = search_with_serpapi(query, serpapi_key)
        new = [u for u in found if u not in all_urls]
        all_urls.extend(new)
        log(f"   +{len(new)} new stores (total: {len(all_urls)})", "INFO")
        time.sleep(1.5)
    log(f"📦 Total: {len(all_urls)} myshopify stores to check", "INFO")
    return all_urls

# ── ADVANCED Payment gateway detection (CART & CHECKOUT TEST) ─────────────────
def check_store_target(base_url, session):
    """
    Returns a dictionary: {"is_shopify": bool, "is_lead": bool, "reason": str}
    ১. Password Page চেক করবে।
    ২. Cart এ প্রোডাক্ট অ্যাড করে Checkout পেজে যাবে।
    ৩. Checkout পেজে Credit Card এর ফর্ম থাকলে রিজেক্ট করবে।
    """
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        # Step 1: Homepage Check & Shopify Verification
        r = session.get(base_url, headers=headers, timeout=15, allow_redirects=True)
        html = r.text.lower()
        
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False, "reason": "Not a Shopify store"}
            
        # Step 2: STRICT Password Page Check (100% New Store)
        if '/password' in r.url or 'action="/password"' in html or 'password-page' in html:
            return {"is_shopify": True, "is_lead": True, "reason": "Password Protected (Brand New Store)"}
            
        # Step 3: Quick Homepage Payment Check (To save time)
        strong_payment_markers =['shopify-payment-button', 'paypal.com/sdk', 'stripe.com', 'klarna.com', 'afterpay.com']
        if any(marker in html for marker in strong_payment_markers):
            return {"is_shopify": True, "is_lead": False, "reason": "Payment gateway found on homepage"}

        # Step 4: THE CHECKOUT TEST (Add to cart & check for Credit Card forms)
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    
                    # Add to Cart
                    session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                    
                    # Go to Checkout
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    
                    # 🚨 CHECK FOR CREDIT CARD FORMS (Like the Aimé Leon Dore screenshot)
                    if 'credit or debit card' in chk_html or 'card number' in chk_html or 'payment-gateway' in chk_html or 'stripe' in chk_html or 'paypal' in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Active Checkout (Credit Card/PayPal found)"}
                    
                    # ✅ CHECK FOR NO-PAYMENT ERROR
                    if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html or "cannot accept payments" in chk_html:
                        return {"is_shopify": True, "is_lead": True, "reason": "Live Store -> Checkout Disabled (No Gateway)!"}
                    
                    # If checkout loads normally but we didn't see the error, assume it has payment
                    return {"is_shopify": True, "is_lead": False, "reason": "Active Checkout (Gateway assumed)"}
        except Exception as e:
            pass # Cart test failed, fallback below
            
        # STRICT RULE: If we couldn't prove it has NO payment, we REJECT it to be 100% safe.
        return {"is_shopify": True, "is_lead": False, "reason": "Could not verify absence of payment (Skipped)"}
        
    except Exception as e:
        return {"is_shopify": False, "is_lead": False, "reason": f"Connection Error: {e}"}


# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    # Priority: mailto: links
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL_DOMAINS):
                return email
    # Fallback: regex scan
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
        
        # Try contact/about pages if no email found
        if not result['email']:
            for path in['/pages/contact', '/contact', '/pages/about-us', '/password']:
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
        log(f"Groq error ({e}) — using template", "WARN")
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
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running

    # ── Load config ──────────────────────────────────────────────────────────
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})

    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        log("👉 Make sure APPS_SCRIPT_URL is set in Render → Environment", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR"); return
    if not serpapi_key:
        log("❌ SerpAPI Key missing — go to CFG screen → save", "ERROR"); return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    # ── Load keywords ────────────────────────────────────────────────────────
    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws =[k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
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
    log("🚀 PHASE 1 — STRICT NO-PAYMENT SCANNING", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads from {len(ready_kws)} keywords", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

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

        log(f"\n🎯 Keyword:[{keyword}] | Country: [{country}]", "INFO")

        # Search for stores
        try:
            store_urls = find_shopify_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search failed: {e}", "WARN")
            store_urls =[]

        if not store_urls:
            log("⚠️  No myshopify.com URLs found for this keyword", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores for payment gateways...", "INFO")

        for url in store_urls:
            if not automation_running:
                break
            if total_leads >= min_leads:
                break

            try:
                log(f"   🌐 {url}", "INFO")

                # Step 1 & 2: Verify Shopify & Check Payment Gateway (The New Strict Logic)
                target_info = check_store_target(url, session)

                if not target_info.get("is_shopify"):
                    log(f"   ❌ Not a live Shopify store — skip", "INFO")
                    time.sleep(0.5)
                    continue

                if not target_info.get("is_lead"):
                    log(f"   🚫 REJECTED: {target_info.get('reason')}", "WARN")
                    time.sleep(0.5)
                    continue

                # ✅ NO payment found & Verified!
                log(f"   🎯 100% MATCH: {target_info.get('reason')} — collecting info...", "SUCCESS")

                # Step 3: Extract contact info
                info = get_store_info(url, session)

                # Step 4: Save to Google Sheet
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
                log(f"   ⚠️  Error: {e}", "WARN")
                continue

        # Mark keyword as used
        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── Phase 2: Email outreach ───────────────────────────────────────────────
    if total_leads > 0:
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
                log(f"   ✅ Email sent to {email_to}", "SUCCESS")
            else:
                log(f"   ❌ Send failed: {send_resp.get('message', send_resp)}", "ERROR")

            delay = random.randint(30, 60) # Email sending delay
            log(f"   ⏳ Waiting {delay}s before next email...", "INFO")
            time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL TASKS COMPLETED! Check your Google Sheet for leads.", "SUCCESS")
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
