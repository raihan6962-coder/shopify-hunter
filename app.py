from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
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
# THE "REAL & NEW" UNLIMITED SCRAPER (No Fake Names)
# ─────────────────────────────────────────────────────────────────────────────
def get_massive_store_list(keyword, country, serpapi_key):
    urls = set()
    kw_clean = keyword.lower().replace(' ', '').replace('-', '')
    
    log(f"🚀 REAL & NEW MODE: Scraping authentic databases for '{keyword}'...", "INFO")

    # ── METHOD 1: crt.sh (SSL Logs - Guarantees NEW stores) ──
    log(f"   -> Checking crt.sh (Newly created SSL certificates)...", "INFO")
    try:
        # Searching for the keyword in the SSL certificate name
        r = requests.get(f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json", timeout=20)
        if r.status_code == 200:
            for cert in r.json():
                name = cert.get('common_name', '') or cert.get('name_value', '')
                for n in name.split('\n'):
                    m = MYSHOPIFY_RE.search(n)
                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        log(f"   crt.sh timeout/error: {e}", "WARN")

    # ── METHOD 2: URLScan.io (Recently scanned REAL stores) ──
    log(f"   -> Checking URLScan (Recently active stores)...", "INFO")
    try:
        r = requests.get(f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=2000&sort=time", timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                page_url = res.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception:
        pass

    # ── METHOD 3: CommonCrawl (Massive Global Web Index) ──
    log(f"   -> Checking CommonCrawl (Global Web Index)...", "INFO")
    try:
        # Using a recent index to find stores
        cc_url = f"https://index.commoncrawl.org/CC-MAIN-2024-10-index?url=*.myshopify.com/*{kw_clean}*&output=json&limit=2000"
        r = requests.get(cc_url, timeout=20)
        if r.status_code == 200 and r.text.strip():
            for line in r.text.strip().split('\n'):
                try:
                    data = json.loads(line)
                    m = MYSHOPIFY_RE.search(data.get('url', ''))
                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                except:
                    continue
    except Exception:
        pass

    # ── METHOD 4: SerpAPI (Google - Broad Search) ──
    if serpapi_key:
        log(f"   -> Checking Google via SerpAPI...", "INFO")
        queries = [
            f'site:myshopify.com "{keyword}"',
            f'site:myshopify.com intitle:"{keyword}"',
            f'site:myshopify.com inurl:{kw_clean}'
        ]
        for q in queries:
            # No hard limit here, let it scrape as much as possible (up to 5 pages per query)
            for start in [0, 100, 200, 300, 400]:
                try:
                    params = {'api_key': serpapi_key, 'engine': 'google', 'q': q, 'num': 100, 'start': start}
                    res = requests.get('https://serpapi.com/search', params=params, timeout=15)
                    if res.status_code == 200:
                        results = res.json().get('organic_results', [])
                        if not results: break # Stop if no more results on this page
                        for item in results:
                            m = MYSHOPIFY_RE.search(item.get('link', ''))
                            if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                except Exception: pass
                time.sleep(1)

    urls_list = list(urls)
    random.shuffle(urls_list)
    log(f"📦 Successfully collected {len(urls_list)} REAL stores to test!", "SUCCESS")
    return urls_list

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        log(f"💥 FATAL: {e}", "ERROR")
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
    serpapi_key = cfg.get('serpapi_key', '').strip()
    
    log(f"✅ Config loaded | Target: UNLIMITED (Saving all REAL & ALIVE URLs)", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    session = requests.Session()
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — SCRAPE & SAVE 'ALIVE' STORES DIRECTLY TO SHEET", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # 1. Scrape REAL stores from databases
        store_urls = get_massive_store_list(keyword, country, serpapi_key)

        if not store_urls:
            log("⚠️  No stores found. Moving to next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"💾 Checking if stores are ALIVE and saving directly to Google Sheet...", "INFO")

        for idx, url in enumerate(store_urls):
            if not automation_running: break

            try:
                # 🔥 ALIVE CHECK: শুধু দেখবে ওয়েবসাইটটা বেঁচে আছে কিনা (Timeout 5s)
                # মরা (Dead) ওয়েবসাইট সেভ করবে না!
                r = session.get(url, timeout=5, allow_redirects=True)
                if r.status_code != 200 or 'shopify' not in r.text.lower():
                    # log(f"   [{idx+1}/{len(store_urls)}] 🚫 DEAD STORE — {url}", "WARN")
                    continue 

                # সুন্দর করে স্টোরের নাম বানাবে URL থেকে
                store_name = url.replace('https://', '').replace('.myshopify.com', '').replace('-', ' ').title()
                
                # সরাসরি শিটে সেভ করবে
                save_resp = call_sheet({
                    'action': 'save_lead', 
                    'store_name': store_name,
                    'url': url, 
                    'email': 'N/A (Scrape Only)', 
                    'phone': 'N/A', 
                    'country': country, 
                    'keyword': keyword
                })
                
                if save_resp.get('error'):
                    continue
                if save_resp.get('status') == 'duplicate':
                    log(f"   [{idx+1}/{len(store_urls)}] ⏭️ Duplicate — {url}", "INFO")
                    continue

                total_leads += 1; kw_leads += 1
                log(f"   [{idx+1}/{len(store_urls)}] ✅ SAVED #{total_leads} → {url}", "SUCCESS")
                
                # Fast saving (0.2s delay to prevent Google Sheet API timeout)
                time.sleep(0.2) 

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} urls saved", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"🎉 ALL DONE! {total_leads} REAL & ALIVE URLs saved to Google Sheet.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr = call_sheet({'action': 'get_leads'})
            if not lr.get('error'):
                leads = lr.get('leads', [])
                total_leads = len(leads)
                emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            if not kr.get('error'):
                kws = kr.get('keywords', [])
                kw_total = len(kws)
                kw_used  = sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads,
                    'emails_sent': emails_sent, 'kw_total': kw_total,
                    'kw_used': kw_used, 'script_connected': bool(script_url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL', ''): return jsonify({'error': 'APPS_SCRIPT_URL not set'})
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
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
