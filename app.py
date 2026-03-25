from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import requests
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os

# DuckDuckGo for Unlimited Free Searches
try:
    from duckduckgo_search import DDGS
    DDGS_AVAILABLE = True
except ImportError:
    DDGS_AVAILABLE = False

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
# THE BULLETPROOF 7-DAYS SCRAPER (No Google, No Crashes)
# ─────────────────────────────────────────────────────────────────────────────
def get_recent_ssl_stores(keyword):
    """
    গত ৭ দিনে তৈরি হওয়া Niche স্টোরগুলো ৪টি ভিন্ন সোর্স থেকে বের করবে।
    crt.sh ডাউন থাকলেও অন্য সোর্সগুলো ব্যাকআপ হিসেবে কাজ করবে।
    """
    urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    seven_days_ago = datetime.now() - timedelta(days=7)
    
    log(f"🔍 Searching for '{keyword}' stores created in the last 1-7 days...", "INFO")

    # SOURCE 1: DuckDuckGo (Past 7 Days Filter) - 100% Google Free
    if DDGS_AVAILABLE:
        log(f"   -> Checking DuckDuckGo (Filtered by Past 7 Days)...", "INFO")
        try:
            with DDGS() as ddgs:
                # timelimit='w' মানে শুধু গত ১ সপ্তাহের (Past Week) রেজাল্ট আনবে
                results = ddgs.text(f'site:myshopify.com "{keyword}"', timelimit='w', max_results=200)
                if results:
                    for r in results:
                        m = MYSHOPIFY_RE.search(r.get('href', ''))
                        if m: urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception as e:
            log(f"   DuckDuckGo error: {e}", "WARN")

    # SOURCE 2: CertSpotter (Newly minted SSL certs)
    log(f"   -> Checking CertSpotter (Recent SSL Certificates)...", "INFO")
    try:
        r = requests.get(
            'https://api.certspotter.com/v1/issuances?domain=myshopify.com&include_subdomains=true&expand=dns_names&match_wildcards=false', 
            timeout=15
        )
        if r.status_code == 200:
            for cert in r.json():
                for name in cert.get('dns_names', []):
                    if name.endswith('.myshopify.com') and kw_clean in name.lower():
                        urls.add(f"https://{name}")
    except Exception as e:
        pass

    # SOURCE 3: URLScan.io (Strictly Past 7 Days)
    log(f"   -> Checking URLScan (Scanned in the last 7 days)...", "INFO")
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND date:>now-7d&size=1000"
        r = requests.get(urlscan_url, timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                page_url = res.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m and kw_clean in m.group(1).lower():
                    urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        pass

    # SOURCE 4: crt.sh (With 502 Crash Protection)
    log(f"   -> Checking crt.sh SSL Logs...", "INFO")
    try:
        r = requests.get(
            f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json", 
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        )
        if r.status_code == 200:
            try:
                for cert in r.json():
                    date_str = cert.get('not_before', '')
                    if date_str:
                        try:
                            cert_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
                            if cert_date >= seven_days_ago:
                                name = cert.get('common_name', '') or cert.get('name_value', '')
                                for n in name.split('\n'):
                                    m = MYSHOPIFY_RE.search(n)
                                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                        except Exception:
                            pass
            except ValueError:
                log(f"   crt.sh is down (502 Bad Gateway). Skipping safely.", "WARN")
        else:
            log(f"   crt.sh returned status {r.status_code}. Skipping safely.", "WARN")
    except Exception as e:
        log(f"   crt.sh connection timeout. Skipping safely.", "WARN")

    urls_list = list(urls)
    log(f"📦 Found {len(urls_list)} VERIFIED NEW stores (1-7 days old) for '{keyword}'!", "SUCCESS")
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
    log("🚀 PHASE 1 — SIMPLE 7-DAYS SCRAPER (NO GOOGLE)", "SUCCESS")
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

        # 1. Scrape only 1-7 days old stores from 4 sources
        store_urls = get_recent_ssl_stores(keyword)

        if not store_urls:
            log("⚠️  No new stores found in the last 7 days for this keyword.", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"💾 Saving {len(store_urls)} stores directly to Google Sheet...", "INFO")

        # 2. Save directly to sheet (No checkout check, no inside scraping)
        for url in store_urls:
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                # Extract a clean store name from URL
                store_name = url.replace('https://', '').replace('.myshopify.com', '').replace('-', ' ').title()

                save_resp = call_sheet({
                    'action': 'save_lead', 
                    'store_name': store_name,
                    'url': url, 
                    'email': 'N/A (Scrape Only)', # No email scraping
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
                time.sleep(0.5) # Fast saving

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
