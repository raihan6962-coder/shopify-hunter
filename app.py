import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

# --- Page Config ---
st.set_page_config(page_title="Shopify Store Finder", page_icon="🕵️‍♂️", layout="wide")
st.title("🕵️‍♂️ Advanced Shopify Store Finder (No API)")
st.markdown("Find new Shopify stores without payment gateways using Brave Search.")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Find:", 10, 50, 30)

# --- Functions ---
def get_brave_search_results(query, num_results):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    urls = []
    page = 0
    
    while len(urls) < num_results and page < 5: # Search up to 5 pages
        # Brave search URL format
        search_url = f"https://search.brave.com/search?q={query}&offset={page}"
        try:
            response = requests.get(search_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links in Brave search results
            for a in soup.find_all('a', href=True):
                link = a['href']
                if 'myshopify.com' in link and 'google.com' not in link and 'brave.com' not in link:
                    # Clean URL to get the base store link
                    base_url = link.split('/')[0] + "//" + link.split('/')[2]
                    if base_url not in urls:
                        urls.append(base_url)
            page += 1
            time.sleep(2) # Sleep to avoid getting blocked by Brave
        except Exception as e:
            st.error(f"Search Error: {e}")
            break
            
    return urls[:num_results]

def analyze_store(url):
    headers = {"User-Agent": "Mozilla/5.0"}
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
        # Shopify often shows these texts if payments aren't set up
        no_payment_keywords = [
            "this shop is not currently accepting payments",
            "payment gateway not setup",
            "store owner hasn't setup payments",
            "checkout is disabled"
        ]
        
        if any(kw in html for kw in no_payment_keywords):
            store_data["Payment Gateway Setup"] = "No"
        
        # If it's a basic .myshopify.com domain, it's usually new and might not have payments
        if ".myshopify.com" in url:
            store_data["Status"] = "Uses Default Domain (Likely New)"
            
        # 3. Extract Email
        emails = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", html)
        if emails:
            # Filter out common image/shopify emails
            valid_emails = [e for e in emails if "shopify" not in e and "png" not in e]
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
        st.info("🔍 Searching Brave Search... Please wait (This takes time because we are not using APIs).")
        
        # Query: site:myshopify.com "keyword" "location"
        search_query = f'site:myshopify.com "{keyword}" "{location}"'
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Scrape URLs
        status_text.text("Scraping search results from Brave...")
        store_urls = get_brave_search_results(search_query, target_count)
        
        if not store_urls:
            st.error("Could not find any stores. Brave might have blocked the request. Try again later.")
        else:
            st.success(f"Found {len(store_urls)} potential stores! Now analyzing them...")
            
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
