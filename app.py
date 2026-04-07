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
st.title("🔥 Ultra-Targeted Shopify Leads (Deep Scrape)")
st.markdown("Bypassing Brave limits & Deep scanning `/cart` pages for **NO Payment Gateway** + **Valid Emails**.")

# --- User Inputs ---
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Enter Keyword (e.g., Clothing, Pet):")
with col2:
    location = st.text_input("Enter Location (e.g., USA, London):")

target_count = st.slider("Minimum Stores to Scrape & Analyze:", 10, 200, 150)

# --- Functions ---
def get_brave_search_results(keyword, location, num_results, status_text):
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    
    urls = set()
    
    # ALPHABET HACK: Forcing Brave to give MORE than 36 results by changing the query slightly
    base_queries = [
        f'site:myshopify.com "{keyword}" "{location}" "not currently accepting payments"',
        f'site:myshopify.com "{keyword}" "{location}" "@gmail.com" OR "@yahoo.com"',
        f'site:myshopify.com "{keyword}" "{location}"'
    ]
    
    # Adding vowel variations to bypass pagination limits
    for letter in ['a', 'e', 'i', 'o', 'u']:
        base_queries.append(f'site:myshopify.com "{keyword}" "{location}" {letter}')
        
    for query in base_queries:
        if len(urls) >= num_results:
            break
            
        status_text.text(f"Bypassing limits... Searching Brave for: {query}")
        
        for page in range(5): # Search 5 pages per variation
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
                        break # Move to next query if this page is empty
                        
                time.sleep(1.5) # Anti-block delay
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
        
        # Filter out Dead Stores
        if "this shop is currently unavailable" in html or "shop is unavailable" in html:
            store_data["Status"] = "Dead Store"
            return store_data

        # Filter out Password Protected Stores (AS YOU REQUESTED)
        if "password" in html and ("opening soon" in html or "enter store using password" in html):
            store_data["Status"] = "Password Protected (Skipped)"
            return store_data
            
        # Extract Email from Homepage
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
        if emails:
            bad_words = ['shopify', 'png', 'jpg', 'jpeg', 'w3.org', 'example', 'domain', 'sentry', 'test']
            valid_emails = [e for e in emails if not any(bw in e.lower() for bw in bad_words)]
            if valid_emails:
                store_data["Email"] = max(set(valid_emails), key=valid_emails.count)

        # STEP 2: DEEP CART SCAN (The Secret Sauce)
        # If it's a live store, the payment error is usually on the cart/checkout page!
        strict_no_payment_phrases = [
            "this shop is not currently accepting payments",
            "this store can’t accept payments right now",
            "payment processing is currently unavailable"
        ]
        
        # Check homepage first just in case
        if any(phrase in html for phrase in strict_no_payment_phrases):
            store_data["Payment Gateway Setup"] = "No"
        else:
            # If not on homepage, silently check the /cart page!
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
        st.info("🔍 Initializing Deep Scraper... Please wait.")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Get URLs
        store_urls = get_brave_search_results(keyword, location, target_count, status_text)
        
        if not store_urls:
            st.error("Brave Search blocked the request. Try waiting 1 minute.")
        else:
            st.success(f"🔥 Successfully bypassed limits! Found {len(store_urls)} stores. Now deep-scanning /cart pages...")
            
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
            # 1. No Payment Gateway
            # 2. Has Email
            # 3. NOT Password Protected
            # 4. NOT Dead
            perfect_leads = df[
                (df["Payment Gateway Setup"] == "No") & 
                (df["Email"] != "Not Found") &
                (df["Status"] != "Password Protected (Skipped)") &
                (df["Status"] != "Dead Store") &
                (df["Status"] != "Failed to load")
            ]
            
            st.markdown("---")
            st.markdown("### 🎯 YOUR PERFECT LEADS (Live Store + No Payment + Has Email)")
            
            if perfect_leads.empty:
                st.error(f"⚠️ We deep-scanned {len(store_urls)} stores. Many had emails, but ALL of them had active payment gateways. Try a different keyword (e.g., 'Jewelry' or 'Toys').")
            else:
                st.success(f"🎯 BOOM! Found {len(perfect_leads)} PERFECT leads matching your exact requirements!")
                st.dataframe(perfect_leads, use_container_width=True)
                
                csv_perfect = perfect_leads.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Perfect Leads (CSV)",
                    data=csv_perfect,
                    file_name=f'perfect_leads_{keyword}_{location}.csv',
                    mime='text/csv',
                )
