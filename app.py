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
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except requests.exceptions.Timeout:
            log(f"Sheet API timeout (attempt {attempt+1}/3)", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error (attempt {attempt+1}/3): {e}", "WARN")
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

# ── Store discovery — maximum URLs ───────────────────────────────────────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def find_shopify_stores(keyword, country, serpapi_key):
    """
    4 source থেকে store URL collect করে — maximum coverage.
    URLScan (fresh) + SerpAPI 4 time ranges + CommonCrawl index
    """
    all_urls = set()

    # ── SOURCE 1: URLScan.io (freshest stores, no API key needed) ────────────
    log(f"   [1/4] URLScan.io scanning...", "INFO")
    urlscan_queries = [
        f"domain:myshopify.com AND page.title:{keyword.lower().replace(' ','')}",
        f"domain:myshopify.com AND page.body:{keyword.lower().replace(' ','')}",
    ]
    for uq in urlscan_queries:
        try:
            r = requests.get(
                f"https://urlscan.io/api/v1/search/?q={requests.utils.quote(uq)}&size=100&sort=time",
                timeout=12, headers={'User-Agent': 'Mozilla/5.0'}
            )
            if r.status_code == 200:
                for result in r.json().get('results', []):
                    page_url = result.get('page', {}).get('url', '')
                    m = MYSHOPIFY_RE.search(page_url)
                    if m:
                        all_urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   URLScan error: {e}", "WARN")
        time.sleep(0.5)

    log(f"   URLScan: {len(all_urls)} stores found", "INFO")

    # ── SOURCE 2: SerpAPI — 4 different time ranges for maximum coverage ─────
    log(f"   [2/4] SerpAPI multi-range search...", "INFO")

    # Time ranges: past week, past month, past 3 months, past year
    # qdr:w = week, qdr:m = month, qdr:m3 = 3 months, qdr:y = year
    time_ranges = [
        ('qdr:w',  'past 7 days'),
        ('qdr:m',  'past 30 days'),
        ('qdr:m3', 'past 3 months'),
        ('qdr:y',  'past year'),
    ]

    # Multiple query templates
    query_templates = [
        f'site:myshopify.com "{keyword}" {country}',
        f'site:myshopify.com "{keyword}" "coming soon"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com {keyword} {country} shop',
        f'site:myshopify.com {keyword} store {country}',
        f'site:myshopify.com inurl:{keyword.replace(" ","-")}',
    ]

    serp_count_before = len(all_urls)
    for tbs, label in time_ranges:
        if len(all_urls) >= 300:
            break
        for q_template in query_templates[:3]:  # top 3 templates per time range
            if len(all_urls) >= 300:
                break
            try:
                params = {
                    'api_key': serpapi_key,
                    'engine': 'google',
                    'q': q_template,
                    'num': 100,
                    'tbs': tbs,
                    'gl': 'us',
                    'hl': 'en',
                }
                res = requests.get('https://serpapi.com/search', params=params, timeout=20)
                if res.status_code == 200:
                    data = res.json()
                    if data.get('error'):
                        log(f"   SerpAPI error: {data['error']}", "WARN")
                        break
                    results = data.get('organic_results', [])
                    before = len(all_urls)
                    for item in results:
                        m = MYSHOPIFY_RE.match(item.get('link', ''))
                        if m:
                            all_urls.add(f"https://{m.group(1)}.myshopify.com")
                    new = len(all_urls) - before
                    if new > 0:
                        log(f"   [{label}] +{new} stores", "INFO")
            except Exception as e:
                log(f"   SerpAPI error: {e}", "WARN")
            time.sleep(1)

    serp_new = len(all_urls) - serp_count_before
    log(f"   SerpAPI: +{serp_new} new stores", "INFO")

    # ── SOURCE 3: CommonCrawl index (free, huge dataset) ─────────────────────
    log(f"   [3/4] CommonCrawl index...", "INFO")
    try:
        cc_url = f"https://index.commoncrawl.org/CC-MAIN-2024-51-index"
        params = {
            'url': f'*.myshopify.com/*{keyword.replace(" ", "*")}*',
            'output': 'json',
            'limit': 200,
            'fl': 'url',
        }
        r = requests.get(cc_url, params=params, timeout=15)
        if r.status_code == 200:
            cc_before = len(all_urls)
            for line in r.text.strip().split('\n'):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    m = MYSHOPIFY_RE.search(data.get('url', ''))
                    if m:
                        all_urls.add(f"https://{m.group(1)}.myshopify.com")
                except:
                    continue
            cc_new = len(all_urls) - cc_before
            log(f"   CommonCrawl: +{cc_new} stores", "INFO")
    except Exception as e:
        log(f"   CommonCrawl error: {e}", "WARN")

    # ── SOURCE 4: Bing via SerpAPI ────────────────────────────────────────────
    log(f"   [4/4] Bing search...", "INFO")
    bing_before = len(all_urls)
    bing_queries = [
        f'site:myshopify.com {keyword} {country}',
        f'site:myshopify.com {keyword}',
    ]
    for bq in bing_queries:
        if len(all_urls) >= 400:
            break
        try:
            params = {
                'api_key': serpapi_key,
                'engine': 'bing',
                'q': bq,
                'count': 50,
            }
            res = requests.get('https://serpapi.com/search', params=params, timeout=20)
            if res.status_code == 200:
                data = res.json()
                if not data.get('error'):
                    for item in data.get('organic_results', []):
                        m = MYSHOPIFY_RE.match(item.get('link', ''))
                        if m:
                            all_urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   Bing error: {e}", "WARN")
        time.sleep(1)

    bing_new = len(all_urls) - bing_before
    log(f"   Bing: +{bing_new} stores", "INFO")

    total = list(all_urls)
    log(f"📦 Total: {len(total)} unique stores to check", "INFO")
    return total

# ── Checkout-based payment detection (PROVEN TO WORK) ────────────────────────
def check_store_target(base_url, session):
    """
    Actual checkout test — adds product to cart then checks checkout page.
    This is the PROVEN method that correctly identifies no-payment stores.
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

        # Skip password protected stores
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password protected"}

        # Get a product to add to cart
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1",
                                   headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}

            prod_data = prod_req.json()
            if not prod_data.get('products'):
                # Store with 0 products — still check checkout directly
                chk_direct = session.get(f"{base_url}/checkout",
                                         headers=headers, timeout=12)
                chk_html = chk_direct.text.lower()
                if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html:
                    return {"is_shopify": True, "is_lead": True,
                            "reason": "0 products + checkout disabled"}
                return {"is_shopify": True, "is_lead": False, "reason": "No products to test"}

            variant_id = prod_data['products'][0]['variants'][0]['id']

            # Add to cart
            session.post(
                f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'},
                timeout=10
            )

            # Go to checkout
            chk_req = session.get(f"{base_url}/checkout", headers=headers,
                                   timeout=15, allow_redirects=True)
            chk_html = chk_req.text.lower()

            # Payment keyword check
            payment_keywords = [
                'visa', 'mastercard', 'amex', 'paypal', 'credit card',
                'debit card', 'card number', 'stripe', 'klarna', 'afterpay',
                'shop pay', 'apple pay', 'google pay', 'card-number',
                'payment-method', 'pay now', 'complete order'
            ]
            for pk in payment_keywords:
                if pk in chk_html:
                    return {"is_shopify": True, "is_lead": False,
                            "reason": f"Has payment ('{pk}' found)"}

            # Explicit no-payment messages
            if ("isn't accepting payments" in chk_html or
                    "not accepting payments" in chk_html or
                    "no payment methods" in chk_html):
                return {"is_shopify": True, "is_lead": True,
                        "reason": "Checkout explicitly disabled"}

            # Checkout exists but no payment keywords — LEAD!
            if ('checkout' in chk_html or 'contact information' in chk_html):
                return {"is_shopify": True, "is_lead": True,
                        "reason": "Checkout reached, no payment options found"}

            return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive checkout"}

        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"Checkout error: {e}"}

    except Exception as e:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
                      '.svg', 'noreply', 'domain.com', 'no-reply']
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
            for path in ['/pages/contact', '/contact', '/pages/about-us']:
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
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
URL: {lead.get('url', '')}
Country: {lead.get('country', '')}
Problem: NO payment gateway configured — cannot accept payments.
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, helpful tone, 1 soft CTA, HTML <p> tags
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
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
        log("👉 Set APPS_SCRIPT_URL in Render → Environment", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    serpapi_key = cfg.get('serpapi_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — CFG screen → save", "ERROR"); return
    if not serpapi_key:
        log("❌ SerpAPI Key missing — CFG screen → save", "ERROR"); return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"📧 Template: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — HUNTING NO-PAYMENT STORES", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Sources: URLScan + Google(4 ranges) + CommonCrawl + Bing", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = 0
        rejected_payment = 0
        rejected_other = 0

        log(f"\n🎯 [{keyword}] in [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country, serpapi_key)
        except Exception as e:
            log(f"Search failed: {e}", "WARN")
            store_urls = []

        if not store_urls:
            log("⚠️  No URLs found", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores via checkout test...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                result = check_store_target(url, session)

                if not result.get("is_shopify"):
                    continue

                if not result.get("is_lead"):
                    reason = result.get('reason', '')
                    if 'payment' in reason.lower() or 'Has payment' in reason:
                        rejected_payment += 1
                    else:
                        rejected_other += 1
                    # Progress log every 10 stores
                    if (idx + 1) % 10 == 0:
                        log(f"   Progress: {idx+1}/{len(store_urls)} checked | "
                            f"leads: {kw_leads} | rejected(payment): {rejected_payment} | "
                            f"other: {rejected_other}", "INFO")
                    time.sleep(0.5)
                    continue

                # ✅ QUALIFIED LEAD
                log(f"   🎯 MATCH ({result.get('reason')}) → collecting info...", "SUCCESS")
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
                log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                log(f"   Error: {e}", "WARN"); continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"\n✅ '{keyword}' — {kw_leads} leads | paid: {rejected_payment} | other: {rejected_other}", "SUCCESS")

    # ── Phase 2: Email ────────────────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total: {total_leads} leads", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email', ''))
                  and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped", "WARN"); break

        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")

        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)

        send_resp = call_sheet({
            'action': 'send_email',
            'to': email_to,
            'subject': subject,
            'body': body,
            'lead_id': lead.get('id', '')
        })

        if send_resp.get('status') == 'ok':
            log(f"   ✅ Sent → {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")

        delay = random.randint(90, 150)
        log(f"   ⏳ Next in {delay}s...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check Google Sheet for leads.", "SUCCESS")
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
            leads = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            kws = kr.get('keywords', [])
            kw_total = len(kws)
            kw_used  = sum(1 for k in kws if k.get('status') == 'used')
        except: pass
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
            trigger='date', run_date=run_time,
            id='scheduled_run', replace_existing=True
        )
        log(f"📅 Scheduled for {data['time']}", "INFO")
        return jsonify({'status': 'scheduled', 'time': data['time']})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
