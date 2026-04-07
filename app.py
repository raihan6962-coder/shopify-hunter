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
st.title("🎯 Advanced Shopify Store Finder")
st.markdown("Finding **Highly Targeted** Shopify stores using **Brave Search** (Anti-Bot Bypass Method).")

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
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    urls = []
    page = 0
    
    # --- TRY 1: BRAVE SEARCH ---
    status_text.text("Attempting to bypass Brave Search bot protection...")
    while len(urls) < num_results and page < 5:
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
            time.sleep(2) 
        except:
            break

    # --- TRY 2: FALLBACK ---
    if len(urls) == 0:
        status_text.text("Brave blocked the Cloud Server. Using alternative secure engine...")
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
        
        # 1. Check if it's a new/password protected store
        if "password" in html and "opening soon" in html:
            store_data["Status"] = "Password Protected (Very New)"
            store_data["Payment Gateway Setup"] = "No (Not Launched)"
            return store_data

        # 2. Check for Payment Gateway absence
        no_payment_keywords = [
            "this shop is not currently accepting payments",
            "payment gateway not setup",
            "store owner hasn't setup payments",
            "checkout is disabled"
        ]
        
        if any(kw in html for kw in no_payment_keywords):
            store_data["Payment Gateway Setup"] = "No"
        
        if ".myshopify.com" in url:
            store_data["Status"] = "Uses Default Domain (Likely New)"
            
        # 3. Extract Email
        emails = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", html)
        if emails:
            valid_emails = [e for e in emails if "shopify" not in e and "png" not in e and "jpg" not in e and "w3.org" not in e]
            if valid_emails:
                store_data["Email"] = valid_emails[0]
                
    except:
        store_data["Status"] = "Failed to load"
        
    return store_data

# --- Main Logic ---
if st.button("🚀 Start Automation"):
    if not keyword or not location:
        st.warning("Please enter both Keyword and Location!")
    else:
        st.info("🔍 Initializing Anti-Bot Scraper... Please wait.")
        
        search_query = f'site:myshopify.com "{keyword}" "{location}"'
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs
        store_urls = get_search_results(search_query, target_count, status_text)
        
        if not store_urls:
            st.error("Could not find stores. The keyword might be too specific or all search engines blocked the server.")
        else:
            st.success(f"Found {len(store_urls)} stores to analyze! Please wait...")
            
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
            status_text.text("✅ Automation Complete! Filtering your specific leads...")
            df = pd.DataFrame(results)
            
            # --- STRICT FILTERING LOGIC ---
            # Only keep stores where Payment Gateway is "No" AND Email is NOT "Not Found"
            perfect_leads = df[
                (df["Payment Gateway Setup"].str.contains("No", na=False)) & 
                (df["Email"] != "Not Found")
            ]
            
            st.markdown("---")
            st.markdown("### 🎯 YOUR TARGETED LEADS")
            
            if perfect_leads.empty:
                st.error("⚠️ We scraped the stores, but none of them matched your strict requirements (No Payment Gateway + Has Email). Try increasing the slider to 100 or changing the keyword.")
            else:
                st.success(f"🎯 Successfully extracted {len(perfect_leads)} leads matching your exact requirements!")
                st.dataframe(perfect_leads, use_container_width=True)
                
                # CSV Download ONLY for Perfect Leads
                csv_perfect = perfect_leads.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Leads (CSV)",
                    data=csv_perfect,
                    file_name=f'targeted_leads_{keyword}_{location}.csv',
                    mime='text/csv',
                )
