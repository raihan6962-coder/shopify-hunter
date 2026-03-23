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
    """Search Google via SerpAPI — Filtered for Past 7 Days (Fresh stores only)"""
    urls =[]
    try:
        params = {
            'api_key': api_key,
            'engine': 'google',
            'q': query,
            'num': 100,
            'gl': 'us',
            'hl': 'en',
            'tbs': 'qdr:w',  # 🔥 MAGIC TRICK 1: শুধু গত ১ সপ্তাহের রেজাল্ট আনবে (w = week)
        }
        r = requests.get('https://serpapi.com/search', params=params, timeout=30)
        if r.status_code != 200:
            log(f"SerpAPI HTTP {r.status_code}: {r.text[:120]}", "WARN")
            return urls
        data = r.json()
        if data.get('error'):
            log(f"SerpAPI error: {data['error']}", "WARN")
            return urls
        results = data.get('organic_results',[])
        log(f"   SerpAPI returned {len(results)} fresh results", "INFO")
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
    all_urls = []
    queries =[
        f'site:myshopify.com {keyword} {country}',
        f'site:myshopify.com {keyword} {country} store',
        f'site:myshopify.com "{keyword}" {country}',
        f'site:myshopify.com {keyword}',
    ]
    for i, query in enumerate(queries):
        if len(all_urls) >= 80:
            break
        log(f"🔍 Query {i+1}/{len(queries)}: {query}", "INFO")
        found = search_with_serpapi(query, serpapi_key)
        new =[u for u in found if u not in all_urls]
        all_urls.extend(new)
        log(f"   +{len(new)} new stores (total: {len(all_urls)})", "INFO")
        time.sleep(1.5)
    log(f"📦 Total: {len(all_urls)} fresh myshopify stores to check", "INFO")
    return all_urls

# ── New Smart Payment & Store Detection ───────────────────────────────────────
def check_store_target(base_url, session):
    """
    ১০০% কার্যকরী চেকার:
    ১. স্টোরটি Shopify কিনা নিশ্চিত করবে।
    ২. Password Protected/Opening Soon পেজ চেক করবে।
    ৩. পেমেন্ট মেথড ফাঁকা কিনা চেক করবে।
    """
    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    headers = {'User-Agent': ua, 'Accept': 'text/html,*/*;q=0.8'}
    
    try:
        r = session.get(base_url, headers=headers, timeout=15, allow_redirects=True)
        html = r.text.lower()
        html_no_space = html.replace(" ", "")

        # 1. Is it Shopify?
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}

        is_lead = False
        reason = ""

        # 2. Password Protected / Opening Soon Check (The absolute best leads)
        if 'password-page' in html or 'opening soon' in html or 'store password' in html or 'enter store using password' in html:
            is_lead = True
            reason = "Under Construction / Password Page (Brand New Store)"

        # 3. Empty Payment Method Check (Live but no gateway)
        elif '"payment_methods":[]' in html_no_space or '"payment_gateway":[]' in html_no_space:
            is_lead = True
            reason = "Live but No Payment Methods Set"

        # 4. Check for existing payment gateways
        payment_keywords =[
            'shopifypay', 'paypal.com/sdk', 'stripe.com/v3', 'apple_pay', 'google_pay', 
            'klarna', 'afterpay', 'sezzle', 'affirm', '"payment_gateway":["'
        ]
        has_payment = any(pk in html_no_space for pk in payment_keywords)

        if has_payment and not is_lead:
            return {"is_shopify": True, "is_lead": False, "reason": "Already has payment gateway"}
        
        if is_lead or not has_payment:
            return {"is_shopify": True, "is_lead": True, "reason": reason or "No Payment Gateway Found"}

        return {"is_shopify": True, "is_lead": False, "reason": "Other payment signs found"}

    except Exception as e:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag['href']
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
    result = {'store_name': base_url.replace('https://', '').split('.')[0], 'email': None, 'phone': None}
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
    except Exception:
        pass
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

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key or not serpapi_key:
        log("❌ API Keys missing — check CFG screen", "ERROR")
        return

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws =[k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR")
        return

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
    log("🚀 PHASE 1 — FINDING FRESH NO-PAYMENT STORES", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id   = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}] (Past 7 Days filter)", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search failed: {e}", "WARN")
            store_urls =[]

        if not store_urls:
            log("⚠️  No NEW stores found in the past 7 days for this keyword. Trying next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} fresh stores...", "INFO")

        for url in store_urls:
            if not automation_running or total_leads >= min_leads:
                break

            try:
                log(f"   🌐 {url}", "INFO")
                target_info = check_store_target(url, session)

                if not target_info.get("is_shopify"):
                    log(f"   ❌ Not a live Shopify store — skip", "INFO")
                    time.sleep(0.5)
                    continue

                if not target_info.get("is_lead"):
                    reason = target_info.get("reason", "Already has payment gateway")
                    log(f"   💳 {reason} → skip", "INFO")
                    time.sleep(0.5)
                    continue

                # ✅ NEW & NO payment found!
                reason = target_info.get("reason")
                log(f"   🎯 TARGET MATCH: {reason} — collecting info...", "SUCCESS")

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
                log(f"   ⚠️  Error: {e}", "WARN")
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── Phase 2 ──
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total fresh leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    pending    = [l for l in leads_resp.get('leads', [])
                  if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads ready to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({'action': 'send_email', 'to': email_to, 'subject': subject, 'body': body, 'lead_id': lead.get('id', '')})
        if send_resp.get('status') == 'ok':
            log(f"   ✅ Email sent", "SUCCESS")
        else:
            log(f"   ❌ Send failed: {send_resp.get('message')}", "ERROR")
        delay = random.randint(60, 120)
        log(f"   ⏳ Wait {delay}s...", "INFO")
        time.sleep(delay)

    log("🎉 ALL DONE!", "SUCCESS")

# Flask Routes
@app.route('/')
def index(): return render_template('index.html')

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
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads, 'emails_sent': emails_sent, 'kw_total': kw_total, 'kw_used': kw_used, 'script_connected': bool(script_url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try:
                msg = log_queue.get(timeout=25)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
