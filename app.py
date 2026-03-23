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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        except:
            time.sleep(3)
    return {'error': 'Sheet API failed'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY — Generate thousands of candidate names & probe
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# Common prefixes/suffixes used in Shopify store names
PREFIXES = [
    '', 'the', 'my', 'best', 'top', 'get', 'buy', 'shop', 'new',
    'all', 'pure', 'true', 'real', 'just', 'pro', 'go', 'try',
    'we', 'our', 'your', 'its', 'the-', 'my-', 'get-', 'buy-',
    'shop-', 'new-', 'best-', 'top-',
]

SUFFIXES = [
    'store', 'shop', 'co', 'us', 'uk', 'hq', 'hub', 'lab', 'pro',
    'plus', 'direct', 'online', 'world', 'zone', 'place', 'spot',
    'market', 'goods', 'depot', 'central', 'studio', 'boutique',
    'official', 'brand', 'deals', 'mart', 'box', 'house', 'space',
    'point', 'base', 'club', 'life', 'wear', 'supply', 'style',
    '-store', '-shop', '-co', '-us', '-hub', '-lab', '-pro',
    '-plus', '-online', '-world', '-zone', '-market', '-goods',
    '-depot', '-central', '-studio', '-boutique', '-official',
    '-brand', '-deals', '-mart', '-box', '-house', '-space',
    '1', '2', 'nyc', 'la', 'atl', 'chi', 'tx', 'ca',
]

def generate_store_names(keyword):
    """Generate hundreds of plausible myshopify subdomains for a keyword."""
    kw = keyword.lower().strip()
    words = kw.split()
    kw_nospace = kw.replace(' ', '')
    kw_dash = kw.replace(' ', '-')
    
    candidates = set()
    
    # Base keyword variations
    bases = [kw_nospace, kw_dash] + words
    if len(words) > 1:
        # Abbreviation
        abbr = ''.join(w[0] for w in words)
        bases.append(abbr)
        # First word only
        bases.append(words[0])
        # Last word only  
        bases.append(words[-1])
    
    for base in bases:
        # No prefix/suffix
        candidates.add(base)
        # With suffixes only
        for s in SUFFIXES:
            candidates.add(f"{base}{s}")
        # With prefixes only
        for p in PREFIXES:
            if p:
                candidates.add(f"{p}{base}")
    
    # Common number patterns
    for base in [kw_nospace, kw_dash, words[0] if words else kw_nospace]:
        for n in range(1, 11):
            candidates.add(f"{base}{n}")
            candidates.add(f"{base}0{n}")
        for year in ['2023', '2024', '2025']:
            candidates.add(f"{base}{year}")
    
    # Filter: valid subdomain format
    valid = []
    for c in candidates:
        c = c.lower().strip('-')
        if (2 <= len(c) <= 60 and 
            re.match(r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$', c)):
            valid.append(c)
    
    return list(set(valid))

def probe_store(subdomain, timeout=5):
    """
    Check if a myshopify.com subdomain is a real active store.
    Returns URL if valid Shopify store, None otherwise.
    """
    url = f"https://{subdomain}.myshopify.com"
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, 
                        allow_redirects=True)
        if r.status_code == 200:
            html = r.text
            # Must have Shopify signature
            if 'cdn.shopify.com' in html or 'Shopify.theme' in html or 'shopify' in html.lower()[:2000]:
                # Skip password protected
                if 'password-page' not in html.lower() and '/password' not in r.url:
                    return url
    except:
        pass
    return None

def find_stores_by_probe(keyword, max_workers=20, batch_size=500):
    """
    Generate store name candidates and probe them in parallel.
    Returns list of confirmed Shopify store URLs.
    """
    candidates = generate_store_names(keyword)
    random.shuffle(candidates)  # randomize order
    
    log(f"   Generated {len(candidates)} candidate names to probe", "INFO")
    
    found_stores = []
    checked = 0
    
    # Process in batches
    for batch_start in range(0, min(len(candidates), batch_size), batch_size // 5):
        if not automation_running:
            break
        batch = candidates[batch_start:batch_start + batch_size // 5]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(probe_store, c): c for c in batch}
            for future in as_completed(futures):
                checked += 1
                result = future.result()
                if result:
                    found_stores.append(result)
                    log(f"   ✓ Found store: {result}", "INFO")
        
        log(f"   Probed {checked}/{len(candidates)} | Found: {len(found_stores)} stores", "INFO")
        
        if len(found_stores) >= 30:  # enough to work with
            break
    
    return found_stores

# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT CHECK — Add to cart → Checkout → Check for payment error
# ─────────────────────────────────────────────────────────────────────────────

NO_PAYMENT_PHRASES = [
    "isn't accepting payments right now",
    "is not accepting payments right now",
    "not accepting payments",
    "no payment methods available",
    "store isn't accepting payments",
]

PAYMENT_INDICATORS = [
    'visa', 'mastercard', 'paypal', 'credit card', 'card number',
    'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'debit card',
    'payment method', 'pay with',
]

def check_payment(base_url, session):
    """
    The definitive test: does this store have a payment gateway?
    Returns: 'no_payment' | 'has_payment' | 'skip'
    """
    try:
        # Get products
        pr = session.get(f"{base_url}/products.json?limit=1",
                         headers=HEADERS, timeout=8)
        if pr.status_code != 200:
            return 'skip'
        products = pr.json().get('products', [])
        if not products:
            return 'skip'  # Empty store, no point

        # Add to cart
        vid = products[0]['variants'][0]['id']
        session.post(f"{base_url}/cart/add.js",
                     json={"id": vid, "quantity": 1},
                     headers={**HEADERS, 'Content-Type': 'application/json'},
                     timeout=8)

        # Go to checkout
        cr = session.get(f"{base_url}/checkout", headers=HEADERS, timeout=12)
        chk = cr.text.lower()

        # Check for Shopify's no-payment message
        for phrase in NO_PAYMENT_PHRASES:
            if phrase in chk:
                return 'no_payment'  # ✅ CONFIRMED!

        # Check for payment indicators
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
        # Email from mailto links first
        for tag in soup.find_all('a', href=True):
            href = tag.get('href', '')
            if href.startswith('mailto:'):
                email = href[7:].split('?')[0].strip().lower()
                if '@' in email and not any(d in email for d in SKIP_EMAIL):
                    info['email'] = email
                    break
        # Email from text
        if not info['email']:
            for m in EMAIL_RE.findall(html):
                m = m.lower()
                if not any(d in m for d in SKIP_EMAIL):
                    info['email'] = m
                    break
        # Phone
        phone_m = re.search(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})', html)
        if phone_m:
            info['phone'] = phone_m.group(0).strip()
        # Try contact page
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
        prompt = f"""Write a cold email to a Shopify store owner.
Store: {lead.get('store_name')} | URL: {lead.get('url')}
Problem: Their store has NO payment gateway — customers cannot buy anything!
Template — Subject: {tpl_subject} | Body: {tpl_body}
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
        log("❌ Groq API Key missing", "ERROR"); return

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No keywords — add in Leads screen", "ERROR"); return

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template", "ERROR"); return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0
    total_checked = 0

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

        log(f"\n🔍 Keyword: [{keyword}]", "INFO")
        log(f"   Generating & probing store names...", "INFO")

        # Find stores by probing
        try:
            stores = find_stores_by_probe(keyword)
        except Exception as e:
            log(f"Probe error: {e}", "WARN")
            stores = []

        if not stores:
            log("⚠️  No stores found — try different keyword", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"✅ Found {len(stores)} real Shopify stores", "SUCCESS")
        log(f"🧪 Testing each for payment gateway...", "INFO")

        for url in stores:
            if not automation_running or total_leads >= min_leads:
                break
            total_checked += 1
            try:
                result = check_payment(url, session)
                
                if result == 'no_payment':
                    log(f"   🎯 NO PAYMENT GATEWAY! → {url}", "SUCCESS")
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
                    log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                    time.sleep(random.uniform(1, 2))
                elif result == 'has_payment':
                    log(f"   💳 Has payment — {url[:50]}", "INFO")
                # 'skip' = silent
            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads", "SUCCESS")

    log(f"\n📊 Done! Leads: {total_leads} | Checked: {total_checked}", "SUCCESS")

    # Phase 2: Email
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    leads_resp = call_sheet({'action': 'get_leads'})
    pending = [l for l in leads_resp.get('leads', [])
               if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break
        try:
            email_to = lead['email']
            log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
            resp = call_sheet({'action': 'send_email', 'to': email_to,
                               'subject': subject, 'body': body, 'lead_id': lead.get('id')})
            if resp.get('status') == 'ok':
                log(f"   ✅ Sent!", "SUCCESS")
            else:
                log(f"   ❌ {resp.get('message','')}", "ERROR")
            delay = random.randint(90, 150)
            log(f"   ⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except: continue

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
        except: pass
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
