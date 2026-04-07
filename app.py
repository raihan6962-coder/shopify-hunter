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
st.title("🔥 Ultra-Targeted Shopify Leads (Strict Mode)")
st.markdown("Finding **NEW** stores with **NO Payment Gateway** AND **Valid Emails** based on your exact Keyword & Location.")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Scrape & Analyze:", 10, 150, 100)

# --- Functions ---
def get_search_results(keyword, location, num_results, status_text):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    urls = set() # Using set to automatically avoid duplicate URLs
    
    # Multi-Query System to get MORE results and NEW stores
    queries = [
        f'site:myshopify.com "{keyword}" "{location}" "2024" OR "2025"', # Focus on new/recent stores
        f'site:myshopify.com "{keyword}" "{location}" "Powered by Shopify"', # Focus on default new stores
        f'site:myshopify.com "{keyword}" "{location}"' # Broad search
    ]
    
    for query in queries:
        if len(urls) >= num_results:
            break
            
        status_text.text(f"Searching web for: {query} ...")
        page = 0
        
        # Searching up to 10 pages per query to force more results
        while page < 10 and len(urls) < num_results:
            try:
                # Primary Search: DuckDuckGo Lite (Better for scraping multiple pages)
                ddg_url = "https://html.duckduckgo.com/html/"
                response = scraper.post(ddg_url, data={'q': query, 's': page * 30}, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    found_in_page = 0
                    for a in soup.find_all('a', href=True):
                        link = a['href']
                        if 'myshopify.com' in link and link.startswith('http'):
                            try:
                                parsed_uri = urlparse(link)
                                base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                                if 'duckduckgo' not in base_url and 'google' not in base_url:
                                    urls.add(base_url)
                                    found_in_page += 1
                            except:
                                continue
                    
                    # If no new URLs found on this page, stop checking this query
                    if found_in_page == 0:
                        break
                page += 1
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
        response = scraper.get(url, timeout=10)
        html = response.text.lower()
        
        # 1. DEAD STORE CHECK (Skip abandoned stores)
        if "this shop is currently unavailable" in html or "shop is unavailable" in html or "only one step left!" in html:
            store_data["Status"] = "Dead/Abandoned Store"
            return store_data # We will filter this out later

        # 2. STRICT PAYMENT GATEWAY CHECK
        # These are the EXACT phrases Shopify uses when a store has NO payment provider connected
        strict_no_payment_phrases = [
            "this shop is not currently accepting payments",
            "this store can’t accept payments right now",
            "payment processing is currently unavailable"
        ]
        
        if any(phrase in html for phrase in strict_no_payment_phrases):
            store_data["Payment Gateway Setup"] = "No"
            
        # 3. ADVANCED EMAIL EXTRACTION
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
        if emails:
            bad_words = ['shopify', 'png', 'jpg', 'jpeg', 'w3.org', 'example', 'domain', 'sentry', 'test']
            valid_emails = [e for e in emails if not any(bw in e.lower() for bw in bad_words)]
            
            if valid_emails:
                store_data["Email"] = max(set(valid_emails), key=valid_emails.count)
                
    except:
        store_data["Status"] = "Failed to load"
        
    return store_data

# --- Main Logic ---
if st.button("🚀 Start Automation"):
    if not keyword or not location:
        st.warning("Please enter both Keyword and Location!")
    else:
        st.info("🔍 Initializing Multi-Query Search... Please wait.")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs using Multi-Query
        store_urls = get_search_results(keyword, location, target_count, status_text)
        
        if not store_urls:
            st.error("Could not find any stores. Try changing the keyword or location.")
        else:
            st.success(f"Found {len(store_urls)} stores matching your keyword & location! Now analyzing...")
            
            results = []
            # Step 2: Analyze each store
            for i, url in enumerate(store_urls):
                status_text.text(f"Analyzing store {i+1}/{len(store_urls)}: {url}")
                data = analyze_store(url)
                results.append(data)
                
                # Update progress bar
                progress_bar.progress((i + 1) / len(store_urls))
                time.sleep(1)
                
            # Step 3: Display STRICTLY FILTERED Results
            status_text.text("✅ Automation Complete! Applying your strict filters...")
            df = pd.DataFrame(results)
            
            # --- STRICT FILTERING LOGIC ---
            # 1. Payment Gateway MUST be "No"
            # 2. Email MUST NOT be "Not Found"
            # 3. Status MUST NOT be "Dead/Abandoned Store"
            perfect_leads = df[
                (df["Payment Gateway Setup"] == "No") & 
                (df["Email"] != "Not Found") &
                (df["Status"] != "Dead/Abandoned Store") &
                (df["Status"] != "Failed to load")
            ]
            
            st.markdown("---")
            st.markdown("### 🎯 YOUR PERFECT LEADS (New + No Payment + Has Email)")
            
            if perfect_leads.empty:
                st.error(f"⚠️ We analyzed {len(store_urls)} stores, but none matched ALL your strict requirements. (Most stores either had payments setup, were dead, or had no email). Try a different keyword.")
            else:
                st.success(f"🎯 Successfully extracted {len(perfect_leads)} PERFECT leads!")
                st.dataframe(perfect_leads, use_container_width=True)
                
                # CSV Download ONLY for Perfect Leads
                csv_perfect = perfect_leads.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Perfect Leads (CSV)",
                    data=csv_perfect,
                    file_name=f'perfect_leads_{keyword}_{location}.csv',
                    mime='text/csv',
                )
