"""
Daily Hyros Edge Report Generator
Pulls yesterday's Edge product data from Hyros, analyzes it with Claude,
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
GOOGLE_DOC_ID = os.environ.get("GOOGLE_DOC_ID")
# Supports multiple tags separated by commas, e.g. "#benzinga-edge-month-19,#benzinga-edge-year-199"
_raw_tags = os.environ.get("HYROS_PRODUCT_TAG", "#edge").strip()
HYROS_PRODUCT_TAGS = [tag.strip() for tag in _raw_tags.split(",") if tag.strip()]

HYROS_BASE_URL = "https://api.hyros.com/v1/api/v1.0"


# ---------------------------------------------------------------------------
# Step 1: Pull sales data from Hyros
# ---------------------------------------------------------------------------
def get_yesterday_range():
    """Return yesterday's start and end timestamps in ISO 8601."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today - timedelta(days=1)
    yesterday_end = today - timedelta(seconds=1)
    return yesterday_start.isoformat(), yesterday_end.isoformat()


def fetch_hyros_sales(from_date, to_date):
    """Fetch all Edge sales from Hyros for the given date range (handles pagination and multiple product tags)."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    all_sales = []

    # Hyros accepts up to 20 product tags per request, so batch them
    # Format: productTags="tag1","tag2","tag3"
    batch_size = 20
    for i in range(0, len(HYROS_PRODUCT_TAGS), batch_size):
        batch = HYROS_PRODUCT_TAGS[i:i + batch_size]
        tags_param = ",".join(f'"{tag}"' for tag in batch)

        page_id = None
        while True:
            params = {
                "productTags": tags_param,
                "fromDate": from_date,
                "toDate": to_date,
                "pageSize": 250,
            }
            if page_id:
                params["pageId"] = page_id

            print(f"  Requesting batch {i // batch_size + 1} ({len(batch)} tags)...")
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

    return all_sales


def fetch_hyros_calls(from_date, to_date):
    """Fetch calls from Hyros for the given date range."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    params = {"fromDate": from_date, "toDate": to_date, "pageSize": 250}

    try:
        resp = requests.get(f"{HYROS_BASE_URL}/calls", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("result", [])
    except Exception:
        # Calls endpoint may not be available on all plans
        return []


# ---------------------------------------------------------------------------
# Step 2: Summarize raw data into a structured digest
# ---------------------------------------------------------------------------
def build_data_summary(sales, calls, report_date):
    """Build a structured summary from raw Hyros data for Claude to analyze."""
    total_revenue = 0
    total_refunded = 0
    total_quantity = 0
    unique_customers = set()
    recurring_count = 0
    non_recurring_count = 0
    sources_first = {}
    sources_last = {}
    platforms = {}
    campaigns = {}
    products = {}
    sale_details = []

    for sale in sales:
        price_info = sale.get("price", {})
        revenue = price_info.get("price", 0) or 0
        refunded = price_info.get("refunded", 0) or 0
        quantity = sale.get("quantity", 1) or 1

        total_revenue += revenue
        total_refunded += refunded
        total_quantity += quantity

        # Customer tracking
        lead = sale.get("lead", {})
        email = lead.get("email", "unknown")
        unique_customers.add(email)

        # Recurring vs one-time
        if sale.get("recurring"):
            recurring_count += 1
        else:
            non_recurring_count += 1

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

        # Last-touch attribution
        last_source = sale.get("lastSource", {})
        if last_source:
            src_name = last_source.get("name", "unknown")
            sources_last[src_name] = sources_last.get(src_name, 0) + 1

        # Product breakdown
        product = sale.get("product", {})
        prod_name = product.get("name", "unknown")
        products[prod_name] = products.get(prod_name, 0) + revenue

        # Individual sale detail
        sale_details.append({
            "revenue": revenue,
            "refunded": refunded,
            "recurring": sale.get("recurring", False),
            "customer_email": email,
            "first_source": first_source.get("name", "N/A") if first_source else "N/A",
            "last_source": last_source.get("name", "N/A") if last_source else "N/A",
            "platform": first_source.get("trafficSource", {}).get("name", "N/A") if first_source else "N/A",
            "campaign": first_source.get("category", {}).get("name", "N/A") if first_source else "N/A",
            "product": prod_name,
        })

    summary = {
        "report_date": report_date,
        "total_sales_count": len(sales),
        "total_quantity": total_quantity,
        "total_revenue": round(total_revenue, 2),
        "total_refunded": round(total_refunded, 2),
        "net_revenue": round(total_revenue - total_refunded, 2),
        "unique_customers": len(unique_customers),
        "recurring_sales": recurring_count,
        "new_sales": non_recurring_count,
        "revenue_by_platform": dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_campaign": dict(sorted(campaigns.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_product": dict(sorted(products.items(), key=lambda x: x[1], reverse=True)),
        "first_touch_sources": dict(sorted(sources_first.items(), key=lambda x: x[1], reverse=True)),
        "last_touch_sources": dict(sorted(sources_last.items(), key=lambda x: x[1], reverse=True)),
        "total_calls": len(calls),
        "sale_details": sale_details,
    }

    return summary


# ---------------------------------------------------------------------------
# Step 3: Send to Claude for analysis
# ---------------------------------------------------------------------------
CLAUDE_SYSTEM_PROMPT = """You are a senior marketing analyst creating a daily performance report for the "Edge" product.

Your report should be clear, actionable, and written for a business owner — not a data scientist. Use plain language.

Structure your report exactly like this:

# Edge Daily Report — {date}

## Quick Summary
A 2-3 sentence overview of the day: total sales, revenue, and the headline takeaway.

## Key Metrics
- Total Sales: X
- Total Revenue: $X
- Net Revenue (after refunds): $X
- New Customers: X
- Recurring Sales: X
- Refunds: $X

## Attribution Breakdown
### Where Sales Came From (First Touch)
Break down by platform (Facebook, Google, etc.) and top campaigns. Show revenue per source.

### Last Touch Attribution
Same breakdown but for last-touch — what was the final touchpoint before purchase.

## Notable Patterns
Anything interesting: which campaigns are performing best, any red flags (high refunds, drop in a channel), mix of recurring vs new.

## Actionable Insights
2-3 specific, actionable recommendations based on this data. Be concrete — "increase budget on X campaign" not "consider optimizing".

Keep the tone professional but conversational. Use dollar amounts and percentages. If the data is sparse (few or no sales), note that clearly and suggest possible reasons."""


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
                "content": f"Here is yesterday's Hyros data for the Edge product. Generate the daily report.\n\n{json.dumps(summary, indent=2)}",
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

    print(f"Using {len(HYROS_PRODUCT_TAGS)} product tags")

    # Get yesterday's date range
    from_date, to_date = get_yesterday_range()
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%B %d, %Y")
    print(f"Generating Edge report for {yesterday_str}...")

    # Pull data from Hyros
    print("Fetching sales from Hyros...")
    sales = fetch_hyros_sales(from_date, to_date)
    print(f"  Found {len(sales)} sales")

    print("Fetching calls from Hyros...")
    calls = fetch_hyros_calls(from_date, to_date)
    print(f"  Found {len(calls)} calls")

    # Build summary
    summary = build_data_summary(sales, calls, yesterday_str)
    print(f"  Total revenue: ${summary['total_revenue']:,.2f}")

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
