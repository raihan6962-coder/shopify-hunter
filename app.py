import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urlparse
import cloudscraper

# --- Page Config ---
st.set_page_config(page_title="Shopify Store Finder", page_icon="🔥", layout="wide")
st.title("🔥 Ultra-Targeted Shopify Leads (Unfinished Store Hack)")
st.markdown("Targeting **Under-Construction & New** stores to guarantee NO Payment Gateways.")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Scrape & Analyze:", 50, 300, 150)

# --- Functions ---
def get_brave_search_results(keyword, location, num_results, status_text):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    urls = set()
    
    # THE UNFINISHED STORE HACK:
    # We removed the strict quotes around keyword/location to massively increase results.
    # We added footprints that ONLY new/unfinished stores have (No payments!).
    modifiers = [
        '"not currently accepting payments"',
        '"welcome to our store"', # Default Shopify text
        '"test store"',
        '"123 fake street"', # Default Shopify address
        '"example.com"', # Default email domain
        '"powered by shopify" "2024"',
        '"powered by shopify" "2025"',
        'contact us',
        'shipping',
        'returns'
    ]
    
    for mod in modifiers:
        if len(urls) >= num_results:
            break
            
        # Notice: No strict quotes around keyword and location anymore!
        query = f'site:myshopify.com {keyword} {location} {mod}'
        status_text.text(f"Scraping: {query} (Found: {len(urls)} so far...)")
        
        for page in range(4): # Search 4 pages per modifier
            if len(urls) >= num_results:
                break
                
            try:
                brave_url = f"https://search.brave.com/search?q={query}&offset={page}"
                response = scraper.get(brave_url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    found_in_page = 0
                    
                    for a in soup.find_all('a', href=True):
                        link = a['href']
                        if 'myshopify.com' in link and 'brave.com' not in link and 'google.com' not in link:
                            try:
                                parsed_uri = urlparse(link)
                                base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                                if base_url not in urls:
                                    urls.add(base_url)
                                    found_in_page += 1
                            except:
                                continue
                                
                    if found_in_page == 0:
                        break 
                        
                time.sleep(1.5) 
            except:
                break

    return list(urls)[:num_results]

def analyze_store(url):
    scraper = cloudscraper.create_scraper()
    store_data = {
        "Store URL": url,
        "Status": "Active",
        "Payment Gateway Setup": "Yes",
        "Email": "Not Found"
    }
    
    try:
        # STEP 1: Scrape Homepage
        response = scraper.get(url, timeout=10)
        html = response.text.lower()
        
        if "this shop is currently unavailable" in html or "shop is unavailable" in html:
            store_data["Status"] = "Dead Store"
            return store_data

        if "password" in html and ("opening soon" in html or "enter store using password" in html):
            store_data["Status"] = "Password Protected (Skipped)"
            return store_data
            
        # Extract Email
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
        if emails:
            bad_words = ['shopify', 'png', 'jpg', 'jpeg', 'w3.org', 'domain', 'sentry']
            valid_emails = [e for e in emails if not any(bw in e.lower() for bw in bad_words)]
            if valid_emails:
                store_data["Email"] = max(set(valid_emails), key=valid_emails.count)

        # STEP 2: DEEP CART SCAN
        strict_no_payment_phrases = [
            "this shop is not currently accepting payments",
            "this store can’t accept payments right now",
            "payment processing is currently unavailable",
            "checkout is disabled"
        ]
        
        if any(phrase in html for phrase in strict_no_payment_phrases):
            store_data["Payment Gateway Setup"] = "No"
        else:
            try:
                cart_url = url + "/cart"
                cart_response = scraper.get(cart_url, timeout=5)
                cart_html = cart_response.text.lower()
                
                if any(phrase in cart_html for phrase in strict_no_payment_phrases):
                    store_data["Payment Gateway Setup"] = "No"
            except:
                pass
                
    except:
        store_data["Status"] = "Failed to load"
        
    return store_data

# --- Main Logic ---
if st.button("🚀 Start Automation"):
    if not keyword or not location:
        st.warning("Please enter both Keyword and Location!")
    else:
        st.info("🔍 Initializing Unfinished Store Hack... Please wait.")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs
        store_urls = get_brave_search_results(keyword, location, target_count, status_text)
        
        if not store_urls:
            st.error("Brave Search blocked the request. Try waiting 1 minute.")
        else:
            st.success(f"🔥 BOOM! Bypassed the limit! Found {len(store_urls)} unique stores. Now deep-scanning...")
            
            results = []
            # Step 2: Analyze each store
            for i, url in enumerate(store_urls):
                status_text.text(f"Deep Scanning {i+1}/{len(store_urls)}: {url}")
                data = analyze_store(url)
                results.append(data)
                
                progress_bar.progress((i + 1) / len(store_urls))
                time.sleep(1)
                
            # Step 3: Display STRICTLY FILTERED Results
            status_text.text("✅ Automation Complete! Applying your strict filters...")
            df = pd.DataFrame(results)
            
            # --- STRICT FILTERING LOGIC ---
            perfect_leads = df[
                (df["Payment Gateway Setup"] == "No") & 
                (df["Email"] != "Not Found") &
                (df["Email"] != "example@example.com") & # Filter out default fake emails
                (df["Status"] != "Password Protected (Skipped)") &
                (df["Status"] != "Dead Store") &
                (df["Status"] != "Failed to load")
            ]
            
            st.markdown("---")
            st.markdown("### 🎯 YOUR PERFECT LEADS (Live Store + No Payment + Has Email)")
            
            if perfect_leads.empty:
                st.error(f"⚠️ We deep-scanned {len(store_urls)} stores. Try changing the keyword to something broader (e.g., just 'Fashion' instead of 'Clothing').")
            else:
                st.success(f"🎯 SUCCESS! Found {len(perfect_leads)} PERFECT leads matching your exact requirements!")
                st.dataframe(perfect_leads, use_container_width=True)
                
                csv_perfect = perfect_leads.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Perfect Leads (CSV)",
                    data=csv_perfect,
                    file_name=f'perfect_leads_{keyword}_{location}.csv',
                    mime='text/csv',
                )
