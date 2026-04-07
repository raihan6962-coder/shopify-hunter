import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urlparse

# --- Page Config ---
st.set_page_config(page_title="Shopify Store Finder", page_icon="🕵️‍♂️", layout="wide")
st.title("🕵️‍♂️ Advanced Shopify Store Finder")
st.markdown("Finding new Shopify stores using **Brave Search** (Via Free Public API).")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Find:", 10, 50, 30)

# --- Functions ---
def get_brave_results_free_api(query, num_results):
    """
    Using SearXNG Public APIs to get Brave Search results for FREE.
    No API key required.
    """
    # List of free public API servers (if one is busy, it tries the next)
    public_apis = [
        "https://searx.be",
        "https://searx.tiekoetter.com",
        "https://paulgo.io",
        "https://search.mdosch.de"
    ]
    
    urls = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    for api_server in public_apis:
        try:
            # engines=brave forces the API to fetch results ONLY from Brave Search
            api_url = f"{api_server}/search?q={query}&engines=brave&format=json"
            
            response = requests.get(api_url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract results from JSON
                for result in data.get('results', []):
                    link = result.get('url', '')
                    
                    if 'myshopify.com' in link:
                        try:
                            # Clean the URL to get only the store domain
                            parsed_uri = urlparse(link)
                            base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
                            
                            if base_url not in urls:
                                urls.append(base_url)
                        except:
                            continue
                            
                # If we found URLs, stop checking other servers
                if len(urls) > 0:
                    break 
                    
        except Exception as e:
            continue # If this server fails, try the next one in the list
            
    return urls[:num_results]

def analyze_store(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    store_data = {
        "Store URL": url,
        "Status": "Active",
        "Payment Gateway Setup": "Yes",
        "Email": "Not Found"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
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
            valid_emails = [e for e in emails if "shopify" not in e and "png" not in e and "jpg" not in e]
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
        st.info("🔍 Fetching results from Brave Search (via Free API)... Please wait.")
        
        # Query format
        search_query = f'site:myshopify.com "{keyword}" "{location}"'
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs via Free API
        status_text.text("Connecting to Brave Search...")
        store_urls = get_brave_results_free_api(search_query, target_count)
        
        if not store_urls:
            st.error("Could not find stores. The keyword might be too specific or servers are busy. Try a different keyword.")
        else:
            st.success(f"Found {len(store_urls)} potential stores from Brave! Now analyzing them...")
            
            results = []
            # Step 2: Analyze each store
            for i, url in enumerate(store_urls):
                status_text.text(f"Analyzing store {i+1}/{len(store_urls)}: {url}")
                data = analyze_store(url)
                results.append(data)
                
                # Update progress bar
                progress_bar.progress((i + 1) / len(store_urls))
                time.sleep(1) # Be polite to servers
                
            # Step 3: Display Results
            status_text.text("✅ Automation Complete!")
            df = pd.DataFrame(results)
            
            # Filter to show stores without payment gateways first
            df = df.sort_values(by="Payment Gateway Setup", ascending=True)
            
            st.dataframe(df, use_container_width=True)
            
            # Step 4: CSV Download Button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Data as CSV",
                data=csv,
                file_name=f'shopify_stores_{keyword}_{location}.csv',
                mime='text/csv',
            )
