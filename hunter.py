import requests
import re
import time
import random
from urllib.parse import urljoin, urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

BRAVE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://search.brave.com/",
    "DNT": "1",
}

PAYMENT_SIGNATURES = [
    "stripe.js", "js.stripe.com",
    "braintreegateway.com", "braintree",
    "paypal.com/sdk", "paypalobjects.com",
    "shop.app/pay", "shopify-payment",
    "square.com/v2/payments", "squareup.com",
    "checkout.razorpay.com",
    "secure.2checkout.com",
    "cdn.klarna.com",
    "js.afterpay.com",
    "cdn.affirm.com",
]

def brave_search_urls(keyword, location, max_results=60):
    """Brave Search theke Shopify store URL collect koro — no API, direct HTTP"""
    found_urls = set()
    queries = [
        f'site:myshopify.com "{keyword}" "{location}"',
        f'"{keyword}" "{location}" shopify store',
        f'"{keyword}" shop "{location}" shopify',
        f'"{keyword}" "{location}" online store shopify',
        f'site:myshopify.com {keyword} {location}',
        f'{keyword} {location} "powered by shopify"',
    ]

    for query in queries:
        if len(found_urls) >= max_results:
            break
        try:
            encoded = quote_plus(query)
            url = f"https://search.brave.com/search?q={encoded}&source=web"
            resp = requests.get(url, headers=BRAVE_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            html = resp.text
            # Extract all hrefs from search results
            raw_links = re.findall(r'href=["\']([^"\']+)["\']', html)
            for link in raw_links:
                if is_shopify_candidate(link):
                    domain = extract_domain(link)
                    if domain:
                        found_urls.add(domain)
            # Also extract from text snippets that mention .myshopify.com
            myshopify = re.findall(r'([\w\-]+\.myshopify\.com)', html)
            for s in myshopify:
                found_urls.add(f"https://{s}")
            time.sleep(random.uniform(1.5, 3.0))
        except Exception:
            continue

    return list(found_urls)[:max_results]


def is_shopify_candidate(url):
    """URL ta Shopify store howar chance ache?"""
    if not url.startswith("http"):
        return False
    skip = ["brave.com", "google.com", "youtube.com", "wikipedia",
            "facebook.com", "twitter.com", "instagram.com", "tiktok.com",
            "reddit.com", "amazon.com", "ebay.com", "etsy.com"]
    for s in skip:
        if s in url:
            return False
    if "myshopify.com" in url:
        return True
    if any(x in url for x in ["/products/", "/collections/", "/cart", "shopify"]):
        return True
    return False


def extract_domain(url):
    """URL theke clean domain ber koro"""
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        pass
    return None


def check_shopify(url, timeout=10):
    """Ei URL ta ki sত্যিই Shopify store? Homepage check."""
    try:
        resp = requests.get(url, headers=BRAVE_HEADERS, timeout=timeout, allow_redirects=True)
        html = resp.text.lower()
        final_url = resp.url

        shopify_signals = [
            "shopify.com/s/files",
            "cdn.shopify.com",
            "myshopify.com",
            "shopify-section",
            "shopify_analytics",
            '"shopify"',
            "powered by shopify",
        ]
        score = sum(1 for sig in shopify_signals if sig in html)
        is_shopify = score >= 1 or "myshopify.com" in final_url

        # Store name
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', resp.text, re.IGNORECASE)
        store_name = title_match.group(1).strip() if title_match else urlparse(url).netloc

        return is_shopify, store_name, final_url
    except Exception:
        return False, "", url


def check_payment_gateway(url, timeout=12):
    """
    /checkout page check kore payment gateway ache ki na.
    Returns: (has_gateway: bool, detected: list)
    """
    detected = []
    pages_to_check = [url, urljoin(url, "/cart"), urljoin(url, "/checkout")]

    for page in pages_to_check:
        try:
            resp = requests.get(page, headers=BRAVE_HEADERS, timeout=timeout, allow_redirects=True)
            html = resp.text.lower()
            for sig in PAYMENT_SIGNATURES:
                if sig.lower() in html and sig not in detected:
                    detected.append(sig)
        except Exception:
            continue

    has_gateway = len(detected) > 0
    return has_gateway, detected


def get_contact_info(url, timeout=10):
    """Contact page theke email/phone ber koro"""
    emails = set()
    phones = set()

    contact_pages = [
        urljoin(url, "/pages/contact"),
        urljoin(url, "/pages/contact-us"),
        urljoin(url, "/contact"),
        url,
    ]

    for page in contact_pages[:2]:
        try:
            resp = requests.get(page, headers=BRAVE_HEADERS, timeout=timeout)
            html = resp.text
            found_emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', html)
            for e in found_emails:
                if not any(skip in e for skip in ["example", "email@", "test@", "shopify.com", "cdn."]):
                    emails.add(e.lower())
            found_phones = re.findall(r'[\+]?[\d\s\-\(\)]{10,15}', html)
            for p in found_phones:
                clean = re.sub(r'\s+', '', p)
                if len(clean) >= 10:
                    phones.add(clean[:15])
        except Exception:
            continue

    return list(emails)[:3], list(phones)[:2]


def analyze_store(url):
    """Single store full analysis"""
    result = {
        "url": url,
        "store_name": "",
        "is_shopify": False,
        "has_payment": False,
        "payment_detected": [],
        "emails": [],
        "phones": [],
        "status": "checking",
        "final_url": url,
    }

    try:
        is_shopify, store_name, final_url = check_shopify(url)
        result["is_shopify"] = is_shopify
        result["store_name"] = store_name
        result["final_url"] = final_url

        if not is_shopify:
            result["status"] = "not_shopify"
            return result

        has_gateway, detected = check_payment_gateway(final_url)
        result["has_payment"] = has_gateway
        result["payment_detected"] = detected

        emails, phones = get_contact_info(final_url)
        result["emails"] = emails
        result["phones"] = phones

        result["status"] = "done"
    except Exception as e:
        result["status"] = f"error: {str(e)[:50]}"

    return result


def run_hunt(keyword, location, progress_callback=None, max_stores=60):
    """
    Full hunt:
    1. Brave search theke URL collect
    2. Parallel store analysis
    3. Filter: Shopify + no payment gateway
    """
    if progress_callback:
        progress_callback({"phase": "searching", "msg": f"Brave Search চলছে: {keyword} + {location}", "pct": 5})

    urls = brave_search_urls(keyword, location, max_results=max_stores)

    if progress_callback:
        progress_callback({"phase": "analyzing", "msg": f"{len(urls)} টা URL পাওয়া গেছে, analyze করছি...", "pct": 20})

    results = []
    total = len(urls)

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_url = {executor.submit(analyze_store, url): url for url in urls}
        done = 0
        for future in as_completed(future_to_url):
            done += 1
            pct = 20 + int((done / max(total, 1)) * 70)
            try:
                res = future.result(timeout=30)
                results.append(res)
                if progress_callback:
                    progress_callback({
                        "phase": "analyzing",
                        "msg": f"[{done}/{total}] {res.get('store_name', res['url'][:40])}",
                        "pct": pct,
                        "partial": res,
                    })
            except Exception:
                pass

    # Filter: only Shopify + no payment gateway
    qualified = [r for r in results if r["is_shopify"] and not r["has_payment"] and r["status"] == "done"]

    if progress_callback:
        progress_callback({"phase": "done", "msg": f"সম্পন্ন! {len(qualified)} টা যোগ্য store পাওয়া গেছে।", "pct": 100})

    return qualified, results
