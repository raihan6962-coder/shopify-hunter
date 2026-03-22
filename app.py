from flask import Flask, render_template, request, jsonify, Response
import sqlite3
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
import io
import csv
import os

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

DB_PATH = 'agent.db'

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript('''
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            country TEXT NOT NULL,
            used INTEGER DEFAULT 0,
            leads_found INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_name TEXT,
            url TEXT UNIQUE,
            email TEXT,
            phone TEXT,
            country TEXT,
            keyword TEXT,
            email_sent INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subject TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    conn.close()

init_db()

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': message}
    log_queue.put(json.dumps(entry))
    conn = get_db()
    try:
        conn.execute("INSERT INTO logs (level, message) VALUES (?, ?)", (level, message))
        conn.commit()
    finally:
        conn.close()

# ── Payment gateway detection ─────────────────────────────────────────────────
PAYMENT_STRONG = [
    'shopify_payments', 'shop_pay', 'shopify-pay',
    'paypal.com/sdk', 'paypal.com/js',
    'stripe.com/v3', 'stripe.js',
    'klarna.com', 'afterpay', 'clearpay',
    'affirm.com', 'sezzle.com',
    '"payment_gateway":', "'payment_gateway':",
    'data-payment-button',
]

def has_payment_gateway(html, soup):
    html_lower = html.lower()
    # Only flag if STRONG payment indicators found
    strong_hits = sum(1 for ind in PAYMENT_STRONG if ind in html_lower)
    if strong_hits >= 2:
        return True
    if strong_hits == 1:
        # Double-check: look for actual checkout working
        if 'stripe.js' in html_lower or 'paypal.com/sdk' in html_lower:
            return True
    return False

# ── Email / phone extraction ──────────────────────────────────────────────────
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', 'png', 'jpg', 'svg', 'gif']

def extract_email(soup, html):
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_DOMAINS):
                return email
    for match in EMAIL_REGEX.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_DOMAINS):
            return m
    return None

def extract_phone(html):
    for pat in [r'\+\d{1,3}\s*[\-\s]?\d{6,12}',
                r'\+?1?\s*[\(\-\.]?\s*\d{3}\s*[\)\-\.]?\s*\d{3}\s*[\-\.]\s*\d{4}']:
        found = re.search(pat, html)
        if found:
            return found.group(0).strip()
    return None

# ── Store info scraper ────────────────────────────────────────────────────────
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

def get_store_info(url, session):
    try:
        # Normalize to homepage only
        parsed = re.match(r'(https?://[^/]+)', url)
        if not parsed:
            return None
        base_url = parsed.group(1)

        r = session.get(base_url, headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return None
        html = r.text
        if len(html) < 500:
            return None
        # Must be Shopify
        if 'cdn.shopify.com' not in html and 'Shopify.theme' not in html and 'shopify' not in html[:3000].lower():
            return None
        soup = BeautifulSoup(html, 'html.parser')
        if has_payment_gateway(html, soup):
            return None
        title = soup.find('title')
        store_name = title.text.strip()[:80] if title else base_url.replace('https://','').split('.')[0]
        email = extract_email(soup, html)
        phone = extract_phone(html)
        if not email:
            for path in ['/pages/contact', '/contact', '/pages/about']:
                try:
                    cr = session.get(base_url + path, headers=HEADERS, timeout=10)
                    if cr.status_code == 200:
                        cs = BeautifulSoup(cr.text, 'html.parser')
                        email = extract_email(cs, cr.text)
                        if email:
                            break
                except:
                    continue
        return {'store_name': store_name, 'url': base_url, 'email': email, 'phone': phone}
    except:
        return None

# ── Serper.dev — collect ALL result URLs ─────────────────────────────────────
def search_with_serper(query, serper_key, pages=5):
    """Get ALL URLs from Serper results — don't filter by myshopify.com here."""
    urls = []
    try:
        headers = {'X-API-KEY': serper_key, 'Content-Type': 'application/json'}
        for page in range(1, pages + 1):
            payload = {'q': query, 'num': 10, 'page': page, 'gl': 'us', 'hl': 'en'}
            r = requests.post('https://google.serper.dev/search',
                              headers=headers, json=payload, timeout=15)
            if r.status_code != 200:
                log(f"⚠️  Serper p{page}: {r.status_code} — {r.text[:80]}", "WARN")
                break
            data = r.json()
            organic = data.get('organic', [])
            if not organic:
                break
            for item in organic:
                link = item.get('link', '')
                if link and link.startswith('http') and link not in urls:
                    # Skip obvious non-store links
                    skip = ['youtube.com', 'facebook.com', 'instagram.com',
                            'twitter.com', 'reddit.com', 'wikipedia.org',
                            'amazon.com', 'ebay.com', 'etsy.com', 'pinterest.com',
                            'tiktok.com', 'linkedin.com', 'google.com']
                    if not any(s in link for s in skip):
                        urls.append(link)
            log(f"   📄 Page {page}: {len(organic)} results collected", "INFO")
            time.sleep(random.uniform(0.5, 1.0))
    except Exception as e:
        log(f"⚠️  Serper error: {e}", "WARN")
    return urls

# ── Main search ───────────────────────────────────────────────────────────────
def search_shopify_stores(keyword, country, serper_key):
    """
    Search broadly — then visit each URL and check:
    1. Is it a Shopify store?
    2. Does it NOT have a payment gateway?
    This way custom domain Shopify stores are also found.
    """
    all_urls = []
    queries = [
        f'{keyword} store {country} shopify',
        f'buy {keyword} online {country} shopify store',
        f'{keyword} {country} shopify shop new',
        f'{keyword} {country} online store powered by shopify',
        f'{keyword} {country} myshopify.com',
    ]
    for i, query in enumerate(queries):
        if len(all_urls) >= 200:
            break
        log(f"🔍 Search {i+1}/{len(queries)}: {query}", "INFO")
        found = search_with_serper(query, serper_key, pages=5)
        new = [u for u in found if u not in all_urls]
        all_urls.extend(new)
        log(f"✅ Query {i+1}: +{len(new)} URLs (total: {len(all_urls)})", "INFO")
        time.sleep(random.uniform(1, 2))
    log(f"📦 Total URLs to check: {len(all_urls)}", "INFO")
    return all_urls

# ── Apps Script integration ───────────────────────────────────────────────────
def call_apps_script(url, payload):
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code == 200
    except:
        return False

def save_lead_to_sheet(lead, url):
    if not url:
        return
    call_apps_script(url, {
        'action': 'save_lead',
        'store_name': lead.get('store_name', ''),
        'url': lead.get('url', ''),
        'email': lead.get('email', ''),
        'phone': lead.get('phone', ''),
        'country': lead.get('country', ''),
        'keyword': lead.get('keyword', '')
    })

def send_email_via_script(to, subject, body, url):
    if not url:
        return False
    return call_apps_script(url, {
        'action': 'send_email', 'to': to, 'subject': subject, 'body': body
    })

# ── AI email generation ───────────────────────────────────────────────────────
def generate_email(template_subject, template_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""You are a cold email expert. Write a personalized, spam-free email for this Shopify store owner who has NOT set up a payment gateway yet.

Store: {lead.get('store_name', 'this store')}
URL: {lead.get('url', '')}
Country: {lead.get('country', '')}

Base template — Subject: {template_subject} | Body: {template_body}

Rules: personalize store name, max 120 words, zero spam words, soft CTA, HTML with <p> tags.
Return ONLY valid JSON: {{"subject": "...", "body": "..."}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7
        )
        text = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(text)
        return data.get('subject', template_subject), data.get('body', template_body)
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
        return template_subject, template_body

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run_automation_inner()
    except Exception as e:
        log(f"💥 FATAL ERROR: {e}", "ERROR")
        import traceback
        log(traceback.format_exc()[:300], "ERROR")
    finally:
        automation_running = False

def _run_automation_inner():
    global automation_running

    conn = get_db()
    cfg = {r['key']: r['value'] for r in conn.execute("SELECT key, value FROM settings").fetchall()}
    # Convert to dicts BEFORE closing connection
    keywords = [dict(r) for r in conn.execute("SELECT * FROM keywords WHERE used=0 ORDER BY id").fetchall()]
    templates = [dict(r) for r in conn.execute("SELECT * FROM templates ORDER BY id LIMIT 1").fetchall()]
    conn.close()

    groq_key = cfg.get('groq_api_key', '')
    apps_url = cfg.get('apps_script_url', '')
    serper_key = cfg.get('serper_api_key', '')
    min_leads = int(cfg.get('min_leads', '5'))

    if not groq_key:
        log("❌ Groq API Key missing — go to Config", "ERROR"); return
    if not serper_key:
        log("❌ Serper API Key missing — get free key at serper.dev", "ERROR"); return
    if not keywords:
        log("❌ No keywords — add in Custom Leads screen", "ERROR"); return
    if not templates:
        log("❌ No email template — add in Email screen", "ERROR"); return

    tpl = templates[0]
    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER AUTOMATION STARTED", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Keywords: {len(keywords)}", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📡 PHASE 1 — LEAD GENERATION", "INFO")

    for kw_row in keywords:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row['keyword']
        country = kw_row['country']
        kw_id = kw_row['id']
        kw_leads = 0

        log(f"🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            store_urls = search_shopify_stores(keyword, country, serper_key)
        except Exception as e:
            log(f"⚠️  Search failed: {e}", "WARN")
            store_urls = []

        if not store_urls:
            log("⚠️  No URLs found — skipping keyword", "WARN")
            conn = get_db()
            conn.execute("UPDATE keywords SET used=1, leads_found=0 WHERE id=?", (kw_id,))
            conn.commit(); conn.close()
            continue

        log(f"🏪 {len(store_urls)} URLs to check...", "INFO")

        for url in store_urls:
            if not automation_running or total_leads >= min_leads:
                break
            try:
                conn = get_db()
                exists = conn.execute("SELECT 1 FROM leads WHERE url=?", (url,)).fetchone()
                conn.close()
                if exists:
                    continue

                log(f"🔎 {url[:60]}", "INFO")
                info = get_store_info(url, session)

                if info:
                    info['country'] = country
                    info['keyword'] = keyword
                    conn = get_db()
                    try:
                        conn.execute(
                            "INSERT INTO leads (store_name,url,email,phone,country,keyword) VALUES (?,?,?,?,?,?)",
                            (info['store_name'], info['url'], info['email'], info['phone'], country, keyword)
                        )
                        conn.commit()
                        total_leads += 1
                        kw_leads += 1
                        log(f"✅ Lead #{total_leads} — {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                        save_lead_to_sheet(info, apps_url)
                    except Exception as db_e:
                        log(f"DB error: {db_e}", "WARN")
                    finally:
                        conn.close()
                else:
                    log(f"⏭️  Not Shopify / has payment", "INFO")

                time.sleep(random.uniform(1, 2))

            except Exception as url_e:
                log(f"⚠️  Error on {url[:40]}: {url_e}", "WARN")
                continue

        conn = get_db()
        conn.execute("UPDATE keywords SET used=1, leads_found=? WHERE id=?", (kw_leads, kw_id))
        conn.commit(); conn.close()
        log(f"🏷️  '{keyword}' done — {kw_leads} leads collected", "SUCCESS")

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"🎯 LEAD GENERATION DONE — {total_leads} total leads", "SUCCESS")

    if not apps_url:
        log("⚠️  No Apps Script URL — email phase skipped", "WARN")
        return

    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    conn = get_db()
    outreach_leads = [dict(r) for r in conn.execute(
        "SELECT * FROM leads WHERE email IS NOT NULL AND email!='' AND email_sent=0"
    ).fetchall()]
    conn.close()

    log(f"📨 {len(outreach_leads)} leads queued", "INFO")

    for i, ld in enumerate(outreach_leads):
        if not automation_running:
            break
        try:
            log(f"✉️  {i+1}/{len(outreach_leads)} → {ld['email']}", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], ld, groq_key)
            ok = send_email_via_script(ld['email'], subject, body, apps_url)
            if ok:
                conn = get_db()
                conn.execute("UPDATE leads SET email_sent=1 WHERE id=?", (ld['id'],))
                conn.commit(); conn.close()
                log(f"✅ Sent → {ld['email']}", "SUCCESS")
            else:
                log(f"❌ Failed → {ld['email']}", "ERROR")
            delay = random.randint(60, 120)
            log(f"⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except Exception as e:
            log(f"⚠️  Email error: {e}", "WARN")
            continue

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE — AUTOMATION COMPLETE!", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    conn = get_db()
    total_leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    emails_sent = conn.execute("SELECT COUNT(*) FROM leads WHERE email_sent=1").fetchone()[0]
    kw_total = conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    kw_used = conn.execute("SELECT COUNT(*) FROM keywords WHERE used=1").fetchone()[0]
    cfg = {r['key']: r['value'] for r in conn.execute("SELECT key,value FROM settings").fetchall()}
    conn.close()
    return jsonify({
        'running': automation_running,
        'total_leads': total_leads, 'emails_sent': emails_sent,
        'kw_total': kw_total, 'kw_used': kw_used,
        'scheduled': cfg.get('scheduled_time', ''),
        'min_leads': cfg.get('min_leads', '500'),
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

@app.route('/api/logs')
def api_logs():
    conn = get_db()
    logs = conn.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 150").fetchall()
    conn.close()
    return jsonify([dict(l) for l in reversed(logs)])

@app.route('/api/keywords', methods=['GET'])
def api_get_keywords():
    conn = get_db()
    rows = conn.execute("SELECT * FROM keywords ORDER BY id DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/keywords', methods=['POST'])
def api_add_keyword():
    d = request.json
    conn = get_db()
    conn.execute("INSERT INTO keywords (keyword, country) VALUES (?,?)", (d['keyword'], d['country']))
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/keywords/<int:kid>', methods=['DELETE'])
def api_del_keyword(kid):
    conn = get_db()
    conn.execute("DELETE FROM keywords WHERE id=?", (kid,))
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/keywords/reset', methods=['POST'])
def api_reset_keywords():
    conn = get_db()
    conn.execute("UPDATE keywords SET used=0")
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/templates', methods=['GET'])
def api_get_templates():
    conn = get_db()
    rows = conn.execute("SELECT * FROM templates ORDER BY id DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/templates', methods=['POST'])
def api_add_template():
    d = request.json
    conn = get_db()
    conn.execute("INSERT INTO templates (name, subject, body) VALUES (?,?,?)",
                 (d['name'], d['subject'], d['body']))
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/templates/<int:tid>', methods=['DELETE'])
def api_del_template(tid):
    conn = get_db()
    conn.execute("DELETE FROM templates WHERE id=?", (tid,))
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/leads', methods=['GET'])
def api_get_leads():
    page = int(request.args.get('page', 1))
    per = 100
    offset = (page - 1) * per
    conn = get_db()
    rows = conn.execute("SELECT * FROM leads ORDER BY id DESC LIMIT ? OFFSET ?", (per, offset)).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    conn.close()
    return jsonify({'leads': [dict(r) for r in rows], 'total': total})

@app.route('/api/leads/export')
def api_export_leads():
    conn = get_db()
    rows = conn.execute("SELECT * FROM leads ORDER BY id DESC").fetchall()
    conn.close()
    out = io.StringIO()
    fields = ['id','store_name','url','email','phone','country','keyword','email_sent','created_at']
    w = csv.DictWriter(out, fieldnames=fields)
    w.writeheader()
    for r in rows:
        w.writerow({k: dict(r).get(k,'') for k in fields})
    return Response(out.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=shopify_leads.csv'})

@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    conn = get_db()
    cfg = {r['key']: r['value'] for r in conn.execute("SELECT key,value FROM settings").fetchall()}
    conn.close()
    return jsonify(cfg)

@app.route('/api/settings', methods=['POST'])
def api_save_settings():
    d = request.json
    conn = get_db()
    for k, v in d.items():
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (k, v))
    conn.commit(); conn.close()
    return jsonify({'status': 'ok'})

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
    t = d.get('time', '')
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", ('scheduled_time', t))
    conn.commit(); conn.close()
    try:
        run_time = datetime.fromisoformat(t)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='automation_schedule', replace_existing=True
        )
        log(f"📅 Scheduled for {t}", "INFO")
        return jsonify({'status': 'scheduled'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
