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
import urllib.parse

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

PAYMENT_STRONG = [
    'shopify_payments', 'shop_pay', 'shopify-pay', 'shop-pay',
    'paypal.com/sdk', 'paypal.com/js', 'stripe.com/v3', 'stripe.js',
    'klarna', 'afterpay', 'clearpay', 'affirm.com', 'sezzle',
    'quadpay', 'amazon_payments', 'apple_pay', 'google_pay',
    'data-payment-button', '"payment_gateway"', "'payment_gateway'",
    'payment-gateway', 'payment_method',
]
PAYMENT_ICONS = [
    'visa', 'mastercard', 'amex', 'discover', 'jcb',
    'payment-icon', 'payment_icon', 'cc-visa', 'cc-mastercard',
    'icon-visa', 'icon-paypal', 'icon-mastercard',
]

def has_payment_gateway(html, soup):
    html_lower = html.lower()
    for indicator in PAYMENT_STRONG:
        if indicator in html_lower:
            return True
    footer = soup.find('footer')
    if footer:
        footer_html = str(footer).lower()
        for icon in PAYMENT_ICONS:
            if icon in footer_html:
                return True
    payment_divs = soup.find_all(
        ['div', 'ul', 'section'],
        class_=lambda x: x and any(
            p in ' '.join(x).lower()
            for p in ['payment', 'pay-icon', 'accepted']
        ) if x else False
    )
    if payment_divs:
        return True
    return False

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', 'png', 'jpg', 'svg', 'gif']

def extract_email(soup, html):
    emails = []
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL_DOMAINS):
                emails.append(email)
    if not emails:
        for match in EMAIL_REGEX.findall(html):
            m = match.lower()
            if not any(d in m for d in SKIP_EMAIL_DOMAINS):
                emails.append(m)
    return emails[0] if emails else None

def extract_phone(html):
    patterns = [
        r'\+?1?\s*[\(\-\.]?\s*\d{3}\s*[\)\-\.]?\s*\d{3}\s*[\-\.]\s*\d{4}',
        r'\+\d{1,3}\s*[\-\s]?\d{6,12}',
    ]
    for pat in patterns:
        found = re.search(pat, html)
        if found:
            return found.group(0).strip()
    return None

STORE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

def get_store_info(url, session):
    try:
        r = session.get(url, headers=STORE_HEADERS, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return None
        html = r.text
        if len(html) < 500:
            return None
        if 'cdn.shopify.com' not in html and 'shopify' not in html.lower():
            return None
        soup = BeautifulSoup(html, 'html.parser')
        if has_payment_gateway(html, soup):
            return None
        title = soup.find('title')
        store_name = title.text.strip()[:80] if title else url.replace('https://','').split('.')[0]
        email = extract_email(soup, html)
        phone = extract_phone(html)
        if not email:
            for path in ['/pages/contact', '/contact', '/pages/about']:
                try:
                    cr = session.get(url + path, headers=STORE_HEADERS, timeout=10)
                    if cr.status_code == 200 and len(cr.text) > 200:
                        cs = BeautifulSoup(cr.text, 'html.parser')
                        email = extract_email(cs, cr.text)
                        if email:
                            break
                except:
                    continue
        return {'store_name': store_name, 'url': url, 'email': email, 'phone': phone}
    except:
        return None

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
]

# ── Extract myshopify.com URLs from any HTML page ────────────────────────────
SHOPIFY_REGEX = re.compile(r'https?://([a-zA-Z0-9\-]+)\.myshopify\.com')

def extract_shopify_urls_from_html(html):
    """Find all myshopify.com URLs in raw HTML — handles Bing redirect wrappers."""
    urls = []
    # Direct matches in raw HTML text
    for m in SHOPIFY_REGEX.finditer(html):
        full = f"https://{m.group(1)}.myshopify.com"
        if full not in urls:
            urls.append(full)
    # Also decode percent-encoded URLs (Bing wraps links)
    decoded = urllib.parse.unquote(html)
    for m in SHOPIFY_REGEX.finditer(decoded):
        full = f"https://{m.group(1)}.myshopify.com"
        if full not in urls:
            urls.append(full)
    return urls

# ── Google scraper ────────────────────────────────────────────────────────────
def scrape_google(query, session):
    urls = []
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        params = {'q': query, 'num': 100, 'hl': 'en', 'gl': 'us'}
        r = session.get('https://www.google.com/search', params=params,
                        headers=headers, timeout=15)
        if r.status_code == 200:
            found = extract_shopify_urls_from_html(r.text)
            urls.extend(found)
            log(f"🌐 Google: {len(found)} stores found", "INFO")
        else:
            log(f"⚠️  Google returned {r.status_code}", "WARN")
    except Exception as e:
        log(f"⚠️  Google error: {e}", "WARN")
    return urls

# ── Bing scraper ──────────────────────────────────────────────────────────────
def scrape_bing(query, session):
    urls = []
    for offset in [0, 10, 20, 30]:
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            params = {'q': query, 'first': offset + 1, 'count': 10}
            r = session.get('https://www.bing.com/search', params=params,
                            headers=headers, timeout=15)
            if r.status_code == 200:
                found = extract_shopify_urls_from_html(r.text)
                new = [u for u in found if u not in urls]
                urls.extend(new)
                log(f"📄 Bing page {offset//10+1}: {len(new)} stores", "INFO")
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            log(f"⚠️  Bing error: {e}", "WARN")
            time.sleep(5)
    return urls

# ── MAIN SEARCH FUNCTION ──────────────────────────────────────────────────────
def search_shopify_stores(keyword, country):
    all_urls = []
    session = requests.Session()
    session.max_redirects = 5

    # Build queries — broad ones work better across search engines
    queries = [
        f'{keyword} {country} myshopify.com',
        f'{keyword} store {country} myshopify',
        f'shopify {keyword} {country}',
    ]

    log(f"🚀 Starting search: {keyword} | {country}", "INFO")

    for i, query in enumerate(queries):
        if len(all_urls) >= 80:
            break

        log(f"🔍 Query {i+1}: {query}", "INFO")

        # Try Google first
        found_g = scrape_google(query, session)
        for u in found_g:
            if u not in all_urls:
                all_urls.append(u)

        time.sleep(random.uniform(3, 6))

        # Then Bing
        found_b = scrape_bing(query, session)
        for u in found_b:
            if u not in all_urls:
                all_urls.append(u)

        log(f"✅ Query {i+1} total so far: {len(all_urls)} stores", "INFO")
        time.sleep(random.uniform(4, 8))

    log(f"📦 Total unique Shopify stores found: {len(all_urls)}", "INFO")
    return all_urls

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
        'action': 'send_email',
        'to': to,
        'subject': subject,
        'body': body
    })

def generate_email(template_subject, template_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""You are a cold email expert. Write a personalized, professional, spam-free email for this Shopify store owner who has NOT set up a payment gateway yet.

Store: {lead.get('store_name', 'this store')}
URL: {lead.get('url', '')}
Country: {lead.get('country', '')}

Base template:
Subject: {template_subject}
Body: {template_body}

Rules:
- Personalize using the store name naturally
- Maximum 120 words
- Zero spam trigger words
- Helpful and genuine tone
- Soft CTA at the end
- Do NOT start with "I hope this email finds you well"
- Return HTML body using <p> tags

Return ONLY valid JSON, no markdown:
{{"subject": "...", "body": "..."}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7
        )
        text = resp.choices[0].message.content.strip()
        text = re.sub(r'```(?:json)?|```', '', text).strip()
        data = json.loads(text)
        return data.get('subject', template_subject), data.get('body', template_body)
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
        return template_subject, template_body

def run_automation():
    global automation_running
    automation_running = True

    conn = get_db()
    cfg = {r['key']: r['value'] for r in conn.execute("SELECT key, value FROM settings").fetchall()}
    keywords = conn.execute("SELECT * FROM keywords WHERE used=0 ORDER BY id").fetchall()
    templates = conn.execute("SELECT * FROM templates ORDER BY id LIMIT 1").fetchall()
    conn.close()

    groq_key = cfg.get('groq_api_key', '')
    apps_url = cfg.get('apps_script_url', '')
    min_leads = int(cfg.get('min_leads', '500'))

    if not groq_key:
        log("❌ Groq API Key not set. Go to Config.", "ERROR")
        automation_running = False
        return
    if not keywords:
        log("❌ No keywords found. Add in Custom Leads.", "ERROR")
        automation_running = False
        return
    if not templates:
        log("❌ No email template. Add in Email Templates.", "ERROR")
        automation_running = False
        return

    tpl = dict(templates[0])
    store_session = requests.Session()
    store_session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 SHOPIFY HUNTER AUTOMATION STARTED", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📡 PHASE 1 — LEAD GENERATION", "INFO")

    for kw_row in keywords:
        if not automation_running or total_leads >= min_leads:
            break

        keyword = kw_row['keyword']
        country = kw_row['country']
        kw_leads = 0

        log(f"🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")
        store_urls = search_shopify_stores(keyword, country)

        if not store_urls:
            log(f"⚠️  0 stores found — try different keyword/country.", "WARN")
            conn = get_db()
            conn.execute("UPDATE keywords SET used=1, leads_found=0 WHERE id=?", (kw_row['id'],))
            conn.commit()
            conn.close()
            continue

        log(f"🏪 {len(store_urls)} stores found — checking payment gateways...", "INFO")

        for url in store_urls:
            if not automation_running or total_leads >= min_leads:
                break

            conn = get_db()
            exists = conn.execute("SELECT 1 FROM leads WHERE url=?", (url,)).fetchone()
            conn.close()
            if exists:
                continue

            log(f"🔎 Checking: {url}", "INFO")
            info = get_store_info(url, store_session)

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
                    email_status = info['email'] or '⚠ no email'
                    log(f"✅ Lead #{total_leads} — {info['store_name']} | {email_status}", "SUCCESS")
                    save_lead_to_sheet(info, apps_url)
                except:
                    pass
                finally:
                    conn.close()
            else:
                log(f"⏭️  Skipped (has payment or invalid): {url}", "INFO")

            time.sleep(random.uniform(1, 3))

        conn = get_db()
        conn.execute("UPDATE keywords SET used=1, leads_found=? WHERE id=?", (kw_leads, kw_row['id']))
        conn.commit()
        conn.close()
        log(f"🏷️  '{keyword}' done — {kw_leads} leads collected", "SUCCESS")

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"🎯 LEAD GENERATION DONE — {total_leads} total leads", "SUCCESS")

    if not apps_url:
        log("⚠️  No Apps Script URL — email phase skipped", "WARN")
        automation_running = False
        return

    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")

    conn = get_db()
    outreach_leads = conn.execute(
        "SELECT * FROM leads WHERE email IS NOT NULL AND email!='' AND email_sent=0"
    ).fetchall()
    conn.close()

    log(f"📨 {len(outreach_leads)} leads queued for outreach", "INFO")

    for i, lead in enumerate(outreach_leads):
        if not automation_running:
            break
        ld = dict(lead)
        log(f"✉️  {i+1}/{len(outreach_leads)} → {ld['email']}", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], ld, groq_key)
        ok = send_email_via_script(ld['email'], subject, body, apps_url)
        if ok:
            conn = get_db()
            conn.execute("UPDATE leads SET email_sent=1 WHERE id=?", (ld['id'],))
            conn.commit()
            conn.close()
            log(f"✅ Sent → {ld['email']}", "SUCCESS")
        else:
            log(f"❌ Failed → {ld['email']}", "ERROR")
        delay = random.randint(60, 120)
        log(f"⏳ Next in {delay}s...", "INFO")
        time.sleep(delay)

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE — AUTOMATION COMPLETE!", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    automation_running = False

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
        'total_leads': total_leads,
        'emails_sent': emails_sent,
        'kw_total': kw_total,
        'kw_used': kw_used,
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
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/keywords/<int:kid>', methods=['DELETE'])
def api_del_keyword(kid):
    conn = get_db()
    conn.execute("DELETE FROM keywords WHERE id=?", (kid,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/keywords/reset', methods=['POST'])
def api_reset_keywords():
    conn = get_db()
    conn.execute("UPDATE keywords SET used=0")
    conn.commit()
    conn.close()
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
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})

@app.route('/api/templates/<int:tid>', methods=['DELETE'])
def api_del_template(tid):
    conn = get_db()
    conn.execute("DELETE FROM templates WHERE id=?", (tid,))
    conn.commit()
    conn.close()
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
    conn.commit()
    conn.close()
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
    log("⛔ Automation stopped by user", "WARN")
    return jsonify({'status': 'stopped'})

@app.route('/api/schedule', methods=['POST'])
def api_schedule():
    d = request.json
    t = d.get('time', '')
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", ('scheduled_time', t))
    conn.commit()
    conn.close()
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
