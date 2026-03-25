from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import requests
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
import urllib.parse

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
# THE BULLETPROOF MASSIVE SCRAPER (No crt.sh, No Crashes)
# ─────────────────────────────────────────────────────────────────────────────
def get_massive_store_list(keyword):
    """
    ৫টি ভিন্ন সোর্স থেকে হাজার হাজার Niche স্টোর বের করবে।
    """
    urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    
    log(f"🔍 Searching 5 Global Databases for '{keyword}' stores...", "INFO")

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    # SOURCE 1: URLScan.io (Recent Stores)
    log(f"   -> Checking URLScan (Recently scanned stores)...", "INFO")
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=1000&sort=time"
        r = requests.get(urlscan_url, timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                page_url = res.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        pass

    # SOURCE 2: AlienVault OTX (Passive DNS - Huge Database)
    log(f"   -> Checking AlienVault OTX (Global DNS Records)...", "INFO")
    try:
        r = requests.get("https://otx.alienvault.com/api/v1/indicators/domain/myshopify.com/passive_dns", timeout=15)
        if r.status_code == 200:
            for entry in r.json().get('passive_dns', []):
                h = entry.get('hostname', '').lower()
                if h.endswith('.myshopify.com') and kw_clean in h:
                    urls.add(f"https://{h}")
    except Exception as e:
        pass

    # SOURCE 3: HackerTarget (Subdomain Scanner)
    log(f"   -> Checking HackerTarget...", "INFO")
    try:
        r = requests.get("https://api.hackertarget.com/hostsearch/?q=myshopify.com", timeout=15)
        if r.status_code == 200:
            for line in r.text.split('\n'):
                h = line.split(',')[0].lower()
                if h.endswith('.myshopify.com') and kw_clean in h:
                    urls.add(f"https://{h}")
    except Exception as e:
        pass

    # SOURCE 4 & 5: Yahoo & AOL Search (Unblockable Search Engines)
    log(f"   -> Checking Yahoo & AOL for 'Opening Soon' stores...", "INFO")
    queries = [
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}" "password"',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com intitle:"{keyword}"'
    ]
    
    for q in queries:
        if len(urls) > 2000: break
        encoded_q = urllib.parse.quote_plus(q)
        # Yahoo
        try:
            r = requests.get(f"https://search.yahoo.com/search?p={encoded_q}&n=100", headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                m = MYSHOPIFY_RE.search(a['href'])
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
        except: pass
        # AOL
        try:
            r = requests.get(f"https://search.aol.com/aol/search?q={encoded_q}", headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                m = MYSHOPIFY_RE.search(a['href'])
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
        except: pass
        time.sleep(1)

    urls_list = list(urls)
    log(f"📦 Found {len(urls_list)} RAW stores for '{keyword}'!", "SUCCESS")
    return urls_list


# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
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
    min_leads = int(cfg.get('min_leads', 50) or 50)

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — MASSIVE NICHE STORE SCRAPER", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # 1. Scrape thousands of stores
        store_urls = get_massive_store_list(keyword)

        if not store_urls:
            log("⚠️  No stores found for this keyword.", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"💾 Checking if stores are alive and saving directly to Google Sheet...", "INFO")

        # 2. Check if alive and Save directly to sheet
        for url in store_urls:
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                # Quick check to see if the store is actually alive (Timeout 5s)
                r = requests.get(url, timeout=5)
                if r.status_code != 200 or 'shopify' not in r.text.lower():
                    continue # Dead store, skip it

                # Extract a clean store name from URL
                store_name = url.replace('https://', '').replace('.myshopify.com', '').replace('-', ' ').title()

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
                    log(f"   ⏭️  Duplicate — {url}", "INFO"); continue

                total_leads += 1
                kw_leads += 1
                log(f"   ✅ SAVED #{total_leads} → {url}", "SUCCESS")
                time.sleep(0.5)

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} urls saved", "SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"🎉 ALL DONE! {total_leads} Fresh URLs saved to Google Sheet.", "SUCCESS")
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
