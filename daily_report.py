"""
Daily Hyros Edge Report Generator
Pulls yesterday's NEW Edge subscription data from Hyros, analyzes it with Claude,
and appends a report to a Google Doc.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import anthropic
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------------------------------------------------------------------------
# Configuration (pulled from environment variables / GitHub Secrets)
# ---------------------------------------------------------------------------
HYROS_API_KEY = os.environ.get("HYROS_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_DOC_ID = os.environ.get("GOOGLE_DOC_ID", "").strip()

HYROS_BASE_URL = "https://api.hyros.com/v1/api/v1.0"

# US Eastern timezone (UTC-4 during EDT, UTC-5 during EST)
US_EASTERN = timezone(timedelta(hours=-4))  # EDT (March-November)


# ---------------------------------------------------------------------------
# Step 1: Pull NEW (non-recurring) sales data from Hyros
# ---------------------------------------------------------------------------
def get_yesterday_range():
    """Return yesterday's start and end timestamps in US Eastern time."""
    now_eastern = datetime.now(US_EASTERN)
    today_eastern = now_eastern.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_eastern - timedelta(days=1)
    yesterday_end = today_eastern - timedelta(seconds=1)
    return yesterday_start.isoformat(), yesterday_end.isoformat()


def fetch_new_edge_sales(from_date, to_date):
    """Fetch only NEW (non-recurring) sales, then filter for Edge products."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    all_sales = []
    page_id = None

    while True:
        params = {
            "fromDate": from_date,
            "toDate": to_date,
            "isRecurringSale": "NON_RECURRING",
            "pageSize": 250,
        }
        if page_id:
            params["pageId"] = page_id

        resp = requests.get(f"{HYROS_BASE_URL}/sales", headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  Hyros API error {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        data = resp.json()

        sales = data.get("result", [])
        all_sales.extend(sales)

        page_id = data.get("nextPageId")
        if not page_id or not sales:
            break

    # Filter for Edge products
    edge_sales = []
    for sale in all_sales:
        product = sale.get("product", {})
        name = (product.get("name") or "").lower()
        tag = (product.get("tag") or "").lower()
        if "edge" in name or "edge" in tag:
            edge_sales.append(sale)

    print(f"  Total non-recurring sales: {len(all_sales)}, Edge: {len(edge_sales)}")
    return edge_sales


def fetch_landing_pages(emails):
    """Fetch landing page URLs from lead clicks for each customer."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    landing_pages = {}

    for email in emails:
        try:
            resp = requests.get(
                f"{HYROS_BASE_URL}/leads/clicks",
                headers=headers,
                params={"email": email, "pageSize": 50},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            clicks = resp.json().get("result", [])

            # Collect unique landing pages (the 'page' field, without query params)
            pages = []
            for click in clicks:
                page = click.get("page", "")
                if page and "edge" in page.lower():
                    pages.append(page)

            if pages:
                landing_pages[email] = list(dict.fromkeys(pages))  # dedupe, keep order
        except Exception:
            continue

    return landing_pages


# ---------------------------------------------------------------------------
# Step 2: Summarize raw data into a structured digest
# ---------------------------------------------------------------------------
def build_data_summary(sales, landing_pages, report_date):
    """Build a structured summary from raw Hyros data for Claude to analyze."""
    total_revenue = 0
    total_refunded = 0
    unique_customers = set()
    sources_first = {}
    sources_last = {}
    platforms = {}
    campaigns = {}
    products = {}
    landing_page_counts = {}
    sale_details = []

    for sale in sales:
        price_info = sale.get("price", {})
        revenue = price_info.get("price", 0) or 0
        refunded = price_info.get("refunded", 0) or 0

        total_revenue += revenue
        total_refunded += refunded

        # Customer tracking
        lead = sale.get("lead", {})
        email = lead.get("email", "unknown")
        unique_customers.add(email)

        # First-touch attribution
        first_source = sale.get("firstSource", {})
        if first_source:
            src_name = first_source.get("name", "unknown")
            sources_first[src_name] = sources_first.get(src_name, 0) + 1

            ts = first_source.get("trafficSource", {})
            if ts:
                platform = ts.get("name", "unknown")
                platforms[platform] = platforms.get(platform, 0) + revenue

            cat = first_source.get("category", {})
            if cat:
                camp_name = cat.get("name", "unknown")
                campaigns[camp_name] = campaigns.get(camp_name, 0) + revenue

            # Ad creative info
            ad_info = first_source.get("sourceLinkAd", {})

        # Last-touch attribution
        last_source = sale.get("lastSource", {})
        if last_source:
            src_name = last_source.get("name", "unknown")
            sources_last[src_name] = sources_last.get(src_name, 0) + 1

        # Product breakdown
        product = sale.get("product", {})
        prod_name = product.get("name", "unknown")
        products[prod_name] = products.get(prod_name, 0) + revenue

        # Landing pages for this customer
        customer_pages = landing_pages.get(email, [])
        for page in customer_pages:
            landing_page_counts[page] = landing_page_counts.get(page, 0) + 1

        # Individual sale detail
        sale_details.append({
            "revenue": revenue,
            "refunded": refunded,
            "customer_email": email,
            "first_source": first_source.get("name", "N/A") if first_source else "N/A",
            "first_source_platform": first_source.get("trafficSource", {}).get("name", "N/A") if first_source else "N/A",
            "first_source_organic": first_source.get("organic", None) if first_source else None,
            "campaign": first_source.get("category", {}).get("name", "N/A") if first_source else "N/A",
            "ad_name": first_source.get("sourceLinkAd", {}).get("name", "N/A") if first_source and first_source.get("sourceLinkAd") else "N/A",
            "last_source": last_source.get("name", "N/A") if last_source else "N/A",
            "last_source_platform": last_source.get("trafficSource", {}).get("name", "N/A") if last_source else "N/A",
            "product": prod_name,
            "landing_pages": customer_pages[:3],  # Top 3 landing pages
        })

    summary = {
        "report_date": report_date,
        "total_new_subscriptions": len(sales),
        "unique_new_customers": len(unique_customers),
        "total_revenue": round(total_revenue, 2),
        "total_refunded": round(total_refunded, 2),
        "net_revenue": round(total_revenue - total_refunded, 2),
        "revenue_by_platform": dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_campaign": dict(sorted(campaigns.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_product": dict(sorted(products.items(), key=lambda x: x[1], reverse=True)),
        "first_touch_sources": dict(sorted(sources_first.items(), key=lambda x: x[1], reverse=True)),
        "last_touch_sources": dict(sorted(sources_last.items(), key=lambda x: x[1], reverse=True)),
        "landing_pages": dict(sorted(landing_page_counts.items(), key=lambda x: x[1], reverse=True)),
        "sale_details": sale_details,
    }

    return summary


# ---------------------------------------------------------------------------
# Step 3: Send to Claude for analysis
# ---------------------------------------------------------------------------
CLAUDE_SYSTEM_PROMPT = """You are a senior marketing analyst creating a daily performance report for NEW Edge subscriptions at Benzinga.

IMPORTANT: This report covers only NEW subscriptions (not recurring renewals). Each line item is a new subscription purchase.

Your report should be clear, actionable, and written for a business owner — not a data scientist. Use plain language.

Structure your report exactly like this:

# Edge Daily Report — {date}

## Quick Summary
A 2-3 sentence overview: total new subscriptions, unique customers, revenue, and the headline takeaway.

## Key Metrics
- New Subscriptions: X (line items)
- Unique New Customers: X
- Total Revenue: $X
- Net Revenue (after refunds): $X
- Refunds: $X

## Product Mix
Break down new subscriptions by product variant (monthly, annual, 3-year, upgrades, etc.) with revenue for each.

## Attribution Breakdown
### First Touch (How They Found Us)
Break down by platform (Facebook, Google, organic, etc.) and top campaigns/ad sets. Show revenue per source.

### Last Touch (Final Click Before Purchase)
Same breakdown — what was the final touchpoint before purchase.

## Landing Pages
Show which landing page URLs drove the most new subscriptions. Include the full URL path.

## Notable Patterns
Anything interesting: which campaigns are performing best, which landing pages convert, mix of product types, any red flags.

## Actionable Insights
2-3 specific, actionable recommendations based on this data. Be concrete.

Keep the tone professional but conversational. Use dollar amounts and percentages."""


def analyze_with_claude(summary):
    """Send the data summary to Claude for analysis and get a formatted report."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        system=CLAUDE_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Here is yesterday's Hyros data for NEW Edge subscriptions. Generate the daily report.\n\n{json.dumps(summary, indent=2)}",
            }
        ],
    )

    return message.content[0].text


# ---------------------------------------------------------------------------
# Step 4: Write to Google Doc
# ---------------------------------------------------------------------------
def append_to_google_doc(report_text, report_date):
    """Append the report to a Google Doc, adding a page break before each new report."""
    creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=["https://www.googleapis.com/auth/documents"]
    )
    service = build("docs", "v1", credentials=creds)

    # Get the current document length (we append at the end)
    doc = service.documents().get(documentId=GOOGLE_DOC_ID).execute()
    end_index = doc["body"]["content"][-1]["endIndex"] - 1

    # Build the requests: page break + report content
    requests_body = []

    # Add a page break before the report (skip if doc is empty)
    if end_index > 1:
        requests_body.append(
            {"insertText": {"location": {"index": end_index}, "text": "\n"}}
        )
        end_index += 1
        requests_body.append(
            {
                "insertPageBreak": {
                    "location": {"index": end_index}
                }
            }
        )
        end_index += 1
        requests_body.append(
            {"insertText": {"location": {"index": end_index}, "text": "\n"}}
        )
        end_index += 1

    # Insert the report text
    requests_body.append(
        {"insertText": {"location": {"index": end_index}, "text": report_text}}
    )

    service.documents().batchUpdate(
        documentId=GOOGLE_DOC_ID, body={"requests": requests_body}
    ).execute()

    print(f"Report appended to Google Doc: https://docs.google.com/document/d/{GOOGLE_DOC_ID}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Validate required config
    missing = []
    for var in ["HYROS_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_DOC_ID"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Get yesterday's date range (US Eastern)
    from_date, to_date = get_yesterday_range()
    now_eastern = datetime.now(US_EASTERN)
    yesterday_eastern = now_eastern - timedelta(days=1)
    yesterday_str = yesterday_eastern.strftime("%B %d, %Y")
    print(f"Generating Edge report for {yesterday_str}...")
    print(f"  Date range: {from_date} to {to_date}")

    # Pull NEW (non-recurring) Edge sales from Hyros
    print("Fetching new Edge subscriptions from Hyros...")
    sales = fetch_new_edge_sales(from_date, to_date)
    print(f"  New Edge subscriptions: {len(sales)}")

    # Fetch landing page data for each customer
    emails = list(set(s.get("lead", {}).get("email", "") for s in sales if s.get("lead", {}).get("email")))
    print(f"Fetching landing pages for {len(emails)} customers...")
    landing_pages = fetch_landing_pages(emails)
    print(f"  Found landing page data for {len(landing_pages)} customers")

    # Build summary
    summary = build_data_summary(sales, landing_pages, yesterday_str)
    print(f"  Total revenue: ${summary['total_revenue']:,.2f}")
    print(f"  Unique customers: {summary['unique_new_customers']}")

    # Analyze with Claude
    print("Analyzing data with Claude...")
    report = analyze_with_claude(summary)
    print("  Report generated")

    # Write to Google Doc
    print("Appending report to Google Doc...")
    append_to_google_doc(report, yesterday_str)

    print("Done!")


if __name__ == "__main__":
    main()
