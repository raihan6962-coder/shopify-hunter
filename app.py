import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urlparse
import cloudscraper

# --- Page Config ---
st.set_page_config(page_title="Shopify Store Finder", page_icon="🎯", layout="wide")
st.title("🎯 Ultra-Targeted Shopify Leads")
st.markdown("Strictly finding stores with **NO Payment Gateway** AND **Valid Emails**.")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Scrape & Analyze:", 10, 100, 50)

# --- Functions ---
def get_search_results(query, num_results, status_text):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    urls = []
    page = 0
    
    # Increased page limit to 15 to find MORE raw stores
    status_text.text("Executing Advanced Search Query to find stores without payment gateways...")
    while len(urls) < num_results and page < 15:
        try:
            brave_url = f"https://search.brave.com/search?q={query}&offset={page}"
            response = scraper.get(brave_url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    link = a['href']
                    if 'myshopify.com' in link and 'brave.com' not in link:
                        try:
                            parsed_uri = urlparse(link)
                            base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                            if base_url not in urls:
                                urls.append(base_url)
                        except:
                            continue
            page += 1
            time.sleep(1) 
        except:
            break

    # Fallback to DDG if Brave blocks
    if len(urls) < 5:
        status_text.text("Expanding search using alternative secure engine...")
        try:
            ddg_url = "https://html.duckduckgo.com/html/"
            response = scraper.post(ddg_url, data={'q': query}, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                link = a['href']
                if 'myshopify.com' in link and link.startswith('http'):
                    try:
                        parsed_uri = urlparse(link)
                        base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                        if base_url not in urls and 'duckduckgo' not in base_url:
                            urls.append(base_url)
                    except:
                        continue
        except Exception as e:
            pass

    return urls[:num_results]

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
        
        # 1. Aggressive Payment Gateway Check
        no_payment_keywords = [
            "this shop is not currently accepting payments",
            "payment gateway not setup",
            "store owner hasn't setup payments",
            "checkout is disabled",
            "payment processing is currently unavailable",
            "opening soon",
            "password"
        ]
        
        if any(kw in html for kw in no_payment_keywords):
            store_data["Payment Gateway Setup"] = "No"
            if "opening soon" in html or "password" in html:
                store_data["Status"] = "Password Protected (Very New)"
        
        # 2. Advanced Email Extraction (Better Regex)
        # Finds emails even if they are hidden in code
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
        if emails:
            # Filter out fake/system emails
            bad_words = ['shopify', 'png', 'jpg', 'jpeg', 'w3.org', 'example', 'domain', 'sentry']
            valid_emails = [e for e in emails if not any(bw in e.lower() for bw in bad_words)]
            
            if valid_emails:
                # Get the most common email found on the page
                store_data["Email"] = max(set(valid_emails), key=valid_emails.count)
                
    except:
        store_data["Status"] = "Failed to load"
        
    return store_data

# --- Main Logic ---
if st.button("🚀 Start Automation"):
    if not keyword or not location:
        st.warning("Please enter both Keyword and Location!")
    else:
        st.info("🔍 Initializing Strict Lead Finder... Please wait.")
        
        # THE SECRET SAUCE: Forcing search engine to find broken/new stores directly
        search_query = f'site:myshopify.com "{keyword}" "{location}" ("not currently accepting payments" OR "opening soon")'
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs
        store_urls = get_search_results(search_query, target_count, status_text)
        
        if not store_urls:
            st.error("Could not find stores. Try a broader keyword (e.g., just 'Clothing' instead of 'Men Clothing').")
        else:
            st.success(f"Found {len(store_urls)} highly potential stores! Extracting Emails & Verifying Payments...")
            
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
            
            # --- STRICT FILTERING LOGIC (YOUR REQUIREMENT) ---
            perfect_leads = df[
                (df["Payment Gateway Setup"] == "No") & 
                (df["Email"] != "Not Found")
            ]
            
            st.markdown("---")
            st.markdown("### 🎯 YOUR TARGETED LEADS")
            
            if perfect_leads.empty:
                st.error("⚠️ We scanned the stores, but none had BOTH 'No Payment Gateway' AND a 'Public Email'. Try increasing the slider to 100 or use a different keyword/location.")
            else:
                st.success(f"🎯 BOOM! Successfully extracted {len(perfect_leads)} leads matching your EXACT requirements!")
                st.dataframe(perfect_leads, use_container_width=True)
                
                # CSV Download ONLY for Perfect Leads
                csv_perfect = perfect_leads.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Perfect Leads (CSV)",
                    data=csv_perfect,
                    file_name=f'targeted_leads_{keyword}_{location}.csv',
                    mime='text/csv',
                )
