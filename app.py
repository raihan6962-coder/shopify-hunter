from flask import Flask, render_template, request, jsonify, Response
import threading, queue, time, json, re, random, requests, logging, os
from bs4 import BeautifulSoup
from groq import Groq
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)
log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

def call_sheet(payload):
    url = os.environ.get('APPS_SCRIPT_URL', '')
    if not url: return {'error': 'APPS_SCRIPT_URL not set'}
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except: time.sleep(3)
    return {'error': 'Sheet API failed'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# ─────────────────────────────────────────────────────────────────────────────
# SEARCH — directly find no-payment stores from Google
# KEY INSIGHT: We search for Shopify's exact "isn't accepting payments" message
# Google has indexed these pages → results are ALREADY confirmed no-payment!
# No checkout verification needed!
# ─────────────────────────────────────────────────────────────────────────────
def search_no_payment_stores(keyword, country, serpapi_key):
    found = {}  # url -> source query

    queries = [
        # Direct no-payment message queries (highest quality)
        (f'site:myshopify.com "isn\'t accepting payments right now" {keyword}', 'no-pay-msg'),
        (f'site:myshopify.com "isn\'t accepting payments right now"', 'no-pay-msg'),
        (f'site:myshopify.com "not accepting payments right now" {keyword}', 'no-pay-msg'),
        (f'site:myshopify.com "this store isn\'t accepting payments" {keyword}', 'no-pay-msg'),
        (f'site:myshopify.com "this store isn\'t accepting payments"', 'no-pay-msg'),
        # New stores (often no payment yet)
        (f'site:myshopify.com "{keyword}" "enter using password" {country}', 'new-store'),
        (f'site:myshopify.com "{keyword}" "be the first to know" {country}', 'new-store'),
        (f'site:myshopify.com "{keyword}" "coming soon" {country}', 'new-store'),
        (f'site:myshopify.com "{keyword}" "launching soon"', 'new-store'),
        (f'site:myshopify.com "{keyword}" "opening soon" {country}', 'new-store'),
        # Recent stores
        (f'site:myshopify.com {keyword} {country} "powered by shopify"', 'recent'),
        (f'site:myshopify.com {keyword} {country}', 'recent'),
        (f'site:myshopify.com {keyword}', 'recent'),
    ]

    time_filters = ['qdr:w', 'qdr:m', 'qdr:m3', '']

    for tbs in time_filters:
        if len(found) >= 200: break
        for query, qtype in queries:
            if len(found) >= 200: break
            try:
                params = {
                    'api_key': serpapi_key, 'engine': 'google',
                    'q': query, 'num': 100, 'gl': 'us', 'hl': 'en',
                }
                if tbs: params['tbs'] = tbs
                r = requests.get('https://serpapi.com/search', params=params, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    if not data.get('error'):
                        new = 0
                        for item in data.get('organic_results', []):
                            link = item.get('link', '')
                            m = MYSHOPIFY_RE.search(link)
                            if m:
                                url = f"https://{m.group(1)}.myshopify.com"
                                if url not in found:
                                    found[url] = qtype
                                    new += 1
                        if new > 0:
                            log(f"   +{new} [{tbs or 'all'}] {qtype}: {query[:55]}", "INFO")
                elif r.status_code == 429:
                    log("   ⚠️  SerpAPI rate limit — waiting 30s", "WARN")
                    time.sleep(30)
            except Exception as e:
                log(f"   Query error: {e}", "WARN")
            time.sleep(1.2)

    log(f"📦 Found {len(found)} candidate stores", "INFO")
    return found  # {url: source_type}

# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT STORE INFO
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example.com','sentry.io','shopify.com','noreply','no-reply',
              '.png','.jpg','.svg','schema.org','domain.com','w3.org']

def is_valid_email(e):
    e = e.lower().strip()
    if any(s in e for s in SKIP_EMAIL): return False
    parts = e.split('@')
    if len(parts) != 2 or not parts[0] or '.' not in parts[1]: return False
    return 2 <= len(parts[1].split('.')[-1]) <= 6

def get_store_info(base_url, session):
    info = {'store_name': base_url.replace('https://','').split('.')[0],
            'email': None, 'phone': None}
    try:
        r = session.get(base_url, headers=HEADERS, timeout=12)
        if r.status_code != 200: return info
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        t = soup.find('title')
        if t:
            name = t.text.strip()
            for s in [' – Shopify',' | Shopify',' - Powered by Shopify',
                       ' – Online Store',' | Online Store']:
                name = name.replace(s,'')
            info['store_name'] = name.strip()[:80]
        # Email from mailto
        for tag in soup.find_all('a', href=True):
            href = tag.get('href','')
            if href.startswith('mailto:'):
                e = href[7:].split('?')[0].strip().lower()
                if is_valid_email(e):
                    info['email'] = e; break
        # Email from text
        if not info['email']:
            for m in EMAIL_RE.findall(html):
                if is_valid_email(m):
                    info['email'] = m.lower(); break
        # Phone
        pm = re.search(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})', html)
        if pm: info['phone'] = pm.group(0).strip()
        # Try contact page if no email
        if not info['email']:
            for path in ['/pages/contact','/contact','/pages/about-us']:
                try:
                    pr = session.get(base_url+path, headers=HEADERS, timeout=8)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text,'html.parser')
                        for tag in ps.find_all('a', href=True):
                            href = tag.get('href','')
                            if href.startswith('mailto:'):
                                e = href[7:].split('?')[0].strip().lower()
                                if is_valid_email(e):
                                    info['email'] = e; break
                        if info['email']: break
                        for m in EMAIL_RE.findall(pr.text):
                            if is_valid_email(m):
                                info['email'] = m.lower(); break
                        if info['email']: break
                except: continue
    except: pass
    return info

# ─────────────────────────────────────────────────────────────────────────────
def generate_email_ai(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a cold email to a Shopify store owner.
Store: {lead.get('store_name')} | URL: {lead.get('url')}
Problem: Their store has NO payment gateway — customers cannot checkout!
Template — Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, helpful tone, soft CTA, HTML <p> tags.
Return ONLY JSON: {{"subject":"...","body":"<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role":"user","content":prompt}],
            max_tokens=400, temperature=0.7)
        raw = re.sub(r'```(?:json)?|```','',resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject',tpl_subject), data.get('body',f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
        return tpl_subject, f'<p>{tpl_body}</p>'

# ─────────────────────────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try: _run()
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
    cfg_resp = call_sheet({'action':'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script: {cfg_resp['error']}", "ERROR"); return
    cfg = cfg_resp.get('config',{})
    groq_key    = cfg.get('groq_api_key','').strip()
    serpapi_key = cfg.get('serpapi_key','').strip()
    min_leads   = int(cfg.get('min_leads',50) or 50)
    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    if not serpapi_key:
        log("❌ SerpAPI Key missing — get free at serpapi.com", "ERROR"); return
    log(f"✅ Config OK | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action':'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords',[]) if k.get('status')=='ready']
    if not ready_kws:
        log("❌ No keywords", "ERROR"); return

    tpl_resp = call_sheet({'action':'get_templates'})
    templates = tpl_resp.get('templates',[])
    if not templates:
        log("❌ No email template", "ERROR"); return
    tpl = templates[0]
    log(f"✅ {len(ready_kws)} keywords | Template: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER — DIRECT NO-PAYMENT SEARCH", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads: break
        keyword = kw_row.get('keyword','')
        country = kw_row.get('country','')
        kw_id   = kw_row.get('id','')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            stores_dict = search_no_payment_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search crashed: {e}", "WARN")
            stores_dict = {}

        if not stores_dict:
            log("⚠️  No stores found", "WARN")
            call_sheet({'action':'mark_keyword_used','id':kw_id,'leads_found':0})
            continue

        # Sort: no-pay-msg first (most reliable), then new-store, then recent
        priority = {'no-pay-msg': 0, 'new-store': 1, 'recent': 2}
        sorted_stores = sorted(stores_dict.items(), key=lambda x: priority.get(x[1], 3))

        log(f"🔍 Processing {len(sorted_stores)} stores...", "INFO")
        log(f"   Priority order: no-payment-msg first, then new stores, then recent", "INFO")

        for url, source in sorted_stores:
            if not automation_running or total_leads >= min_leads: break
            try:
                # For no-pay-msg: Google confirmed it, save directly
                # For others: quick check to avoid obvious payment stores
                if source == 'no-pay-msg':
                    # Already confirmed by Google search
                    log(f"   ✅ Google-confirmed no-payment: {url}", "SUCCESS")
                else:
                    # Quick homepage check for obvious payment SDKs
                    try:
                        r = session.get(url, headers=HEADERS, timeout=7, allow_redirects=True)
                        if r.status_code != 200:
                            continue
                        html = r.text.lower()
                        # Skip if obvious payment found
                        if any(s in html for s in ['js.stripe.com', 'paypal.com/sdk',
                               'cdn.shopify.com/shopifycloud/shop-js', 'js.klarna.com']):
                            log(f"   💳 Has payment SDK — {url[:50]}", "INFO")
                            continue
                        log(f"   🔍 Possible no-payment [{source}]: {url[:50]}", "INFO")
                    except:
                        continue

                # Save as lead
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
                    log(f"   ⏭️  Duplicate", "INFO"); continue

                total_leads += 1
                kw_leads += 1
                email_str = info['email'] or '⚠ no email'
                log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {email_str}", "SUCCESS")
                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                log(f"   Error on {url[:40]}: {e}", "WARN")
                continue

        call_sheet({'action':'mark_keyword_used','id':kw_id,'leads_found':kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads", "SUCCESS")

    log(f"\n📊 PHASE 1 DONE! Total leads: {total_leads}", "SUCCESS")

    # Phase 2: Email
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    leads_resp = call_sheet({'action':'get_leads'})
    pending = [l for l in leads_resp.get('leads',[])
               if l.get('email') and '@' in str(l.get('email',''))
               and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break
        try:
            email_to = lead['email']
            log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
            subject, body = generate_email_ai(tpl['subject'], tpl['body'], lead, groq_key)
            resp = call_sheet({'action':'send_email','to':email_to,
                               'subject':subject,'body':body,'lead_id':lead.get('id')})
            if resp.get('status') == 'ok':
                log(f"   ✅ Sent!", "SUCCESS")
            else:
                log(f"   ❌ {resp.get('message','unknown error')}", "ERROR")
            delay = random.randint(90,150)
            log(f"   ⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except Exception as e:
            log(f"   Email error: {e}", "WARN"); continue

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check Google Sheet for leads.", "SUCCESS")

# ── Flask ─────────────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    total_leads=emails_sent=kw_total=kw_used=0
    if os.environ.get('APPS_SCRIPT_URL'):
        try:
            lr=call_sheet({'action':'get_leads'})
            leads=lr.get('leads',[])
            total_leads=len(leads)
            emails_sent=sum(1 for l in leads if l.get('email_sent')=='sent')
            kr=call_sheet({'action':'get_keywords'})
            kws=kr.get('keywords',[])
            kw_total=len(kws)
            kw_used=sum(1 for k in kws if k.get('status')=='used')
        except: pass
    return jsonify({'running':automation_running,'total_leads':total_leads,
                    'emails_sent':emails_sent,'kw_total':kw_total,'kw_used':kw_used,
                    'script_connected':bool(os.environ.get('APPS_SCRIPT_URL'))})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try:
                msg=log_queue.get(timeout=25); yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping':True})}\n\n"
    return Response(gen(),mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL'):
        return jsonify({'error':'APPS_SCRIPT_URL not set'})
    return jsonify(call_sheet(request.json))

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if automation_running: return jsonify({'status':'already_running'})
    automation_thread=threading.Thread(target=run_automation,daemon=True)
    automation_thread.start()
    return jsonify({'status':'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running=False
    log("⛔ Stopped by user","WARN")
    return jsonify({'status':'stopped'})

@app.route('/api/schedule', methods=['POST'])
def api_schedule():
    d=request.json
    try:
        run_time=datetime.fromisoformat(d.get('time',''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation,daemon=True).start(),
            trigger='date',run_date=run_time,id='scheduled_run',replace_existing=True)
        log(f"📅 Scheduled for {d.get('time')}","INFO")
        return jsonify({'status':'scheduled'})
    except Exception as e:
        return jsonify({'status':'error','msg':str(e)}),400

if __name__=='__main__':
    port=int(os.environ.get('PORT',5000))
    app.run(host='0.0.0.0',port=port,debug=False,threaded=True)
