import os
import requests
import pandas as pd
import time
import datetime as dt
from dotenv import load_dotenv
import re
from collections import Counter

# ====================================================
# Load environment variables
# ====================================================
load_dotenv()

# ====================================================
# Constants
# ====================================================
START_DATE = dt.date(2023, 4, 28)
END_DATE   = dt.date(2025, 10, 28)
OUTPUT_FILE = "historical_news_hdfc_bank_filtered.csv"
REQUEST_DELAY = 15  # seconds

# ====================================================
# GDELT Queries
# ====================================================
COMPANY_QUERY = (
    '("HDFC Bank" OR "HDFC Bank Ltd" OR "HDFC Bank Limited" '
    'OR "HDFC Bank results" OR "HDFC Bank profit" OR "HDFC Bank shares" '
    'OR "HDFC Bank stock" OR "HDFC Bank RBI" OR "HDFC Bank loans" '
    'OR "HDFC Bank financial services")'
)

SECTOR_QUERY = (
    '("Indian banking sector" OR "Indian banks" OR "Reserve Bank of India" '
    'OR "RBI Bank of India" OR "State Bank of India" OR "SBI Bank" '
    'OR "ICICI Bank" OR "Axis Bank" OR "Kotak Mahindra Bank" '
    'OR "Yes Bank" OR "Bank of Baroda" OR "Punjab National Bank")'
)

# ====================================================
# Financial Keywords
# ====================================================
FINANCE_KEYWORDS = [
    "hdfc", "hdfc bank", "hdfc securities", "hdfc credit card",
    "bank", "banking", "deposits", "lending", "nbfc", "branch", "retail banking",
    "wholesale banking", "credit", "loan", "interest rate", "fixed deposit",
    "stock", "share", "equity", "nse", "bse", "ipo", "dividend", "valuation",
    "market cap", "investor", "investment", "mutual fund", "portfolio",
    "profit", "revenue", "earnings", "net income", "quarterly results",
    "q1 results", "q2 results", "q3 results", "q4 results", "financial year",
    "rbi", "reserve bank of india", "monetary policy", "repo rate",
    "liquidity", "npas", "non performing asset", "asset quality",
    "crr", "slr", "basel norms", "regulatory", "merger", "acquisition",
]

KEYWORD_PATTERN = re.compile("|".join(map(re.escape, FINANCE_KEYWORDS)), re.IGNORECASE)

# ====================================================
# Utility: Relevance scoring based on keyword matches
# ====================================================
def compute_relevance(text):
    if not isinstance(text, str):
        return 0
    words = re.findall(r'\b\w+\b', text.lower())
    counts = Counter(words)
    score = sum(counts.get(k.lower(), 0) for k in FINANCE_KEYWORDS)
    return score

# ====================================================
# Fetch function
# ====================================================
def fetch_news_from_gdelt(query, start_date, end_date):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": 250,
        "format": "json",
        "sort": "DateDesc",
        "STARTDATETIME": f"{start_date.strftime('%Y%m%d')}000000",
        "ENDDATETIME": f"{end_date.strftime('%Y%m%d')}235959",
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        if response.status_code != 200:
            print(f"⚠️ HTTP {response.status_code} for {start_date}: {response.text[:100]}")
            return []

        data = response.json()
        if "articles" not in data:
            return []

        articles = []
        for a in data["articles"]:
            title = a.get("title", "")
            if not title or not KEYWORD_PATTERN.search(title):
                continue  # skip non-financial titles

            score = compute_relevance(title)
            if score == 0:
                continue

            articles.append({
                "date": a.get("seendate"),
                "title": title,
                "url": a.get("url"),
                "domain": a.get("domain"),
                "language": a.get("language"),
                "source_country": a.get("sourcecountry"),
                "relevance_score": score,
            })
        return articles

    except Exception as e:
        print(f"⚠️ Error fetching for {start_date}: {e}")
        return []

# ====================================================
# Main function
# ====================================================
def main():
    all_articles = []
    total_company = total_sector = 0
    current_date = START_DATE

    while current_date <= END_DATE:
        next_date = current_date + dt.timedelta(days=1)
        print(f"\n📰 Fetching {current_date}")

        company_articles = fetch_news_from_gdelt(COMPANY_QUERY, current_date, next_date)
        for c in company_articles:
            c["category"] = "company"
        company_articles = sorted(company_articles, key=lambda x: x["relevance_score"], reverse=True)[:15]
        print(f"  🏦 Company (top 15): {len(company_articles)}")

        sector_articles = fetch_news_from_gdelt(SECTOR_QUERY, current_date, next_date)
        for s in sector_articles:
            s["category"] = "sector"
        sector_articles = sorted(sector_articles, key=lambda x: x["relevance_score"], reverse=True)[:30]
        print(f"  💹 Sector (top 30): {len(sector_articles)}")

        total_company += len(company_articles)
        total_sector += len(sector_articles)
        all_articles.extend(company_articles + sector_articles)

        time.sleep(REQUEST_DELAY)
        current_date = next_date

    if not all_articles:
        print("❌ No articles fetched.")
        return

    df = pd.DataFrame(all_articles)
    df.drop_duplicates(subset=["title", "url"], inplace=True)
    df.sort_values(by="date", inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n✅ Fetch complete!")
    print(f"Total saved: {len(df)} | 🏦 {total_company} company | 💹 {total_sector} sector")
    print(f"📁 File: {OUTPUT_FILE}")

# ====================================================
if __name__ == "__main__":
    main()
