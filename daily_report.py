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

# Primary checkout pages (source/entry pages) — these are where traffic lands.
# Upgrade pages, thank-you pages, and upsell pages are excluded.
PRIMARY_CHECKOUT_PATHS = [
    "/premium/ideas/benzinga-edge-3",
    "/premium/ideas/benzinga-edge-4",
    "/premium/ideas/benzinga-edge-5",
    "/premium/ideas/benzinga-edge-2/",
    "/premium/ideas/benzinga-edge-report-2/",
    "/premium/ideas/benzinga-edge-memorial-day-special",
    "/premium/ideas/benzinga-edge-ranking",
    "/premium/ideas/benzinga-edge-ranking-checkout/",
    "/premium/ideas/benzinga-edge-trial-30-days/",
    "/premium/ideas/benzinga-edge-trial-report/",
    "/premium/ideas/benzinga-edge-trial-30-days-report/",
    "/premium/ideas/benzinga-edge-trial",
    "/premium/ideas/benzinga-edge-cheatsheet/",
    "/premium/ideas/benzinga-edge-cheatsheet-2/",
    "/premium/ideas/get-3-years-of-edge/",
    "/premium/ideas/edge-upgrade-to-1-year-offer-checkout/",
    "/edge/",
]


def is_source_checkout_page(url):
    """Check if a URL is a primary checkout/source page (not an upgrade or thank-you)."""
    from urllib.parse import urlparse
    path = urlparse(url).path.rstrip("/") + "/"
    # Normalize: strip trailing slash for comparison
    path_clean = path.rstrip("/")
    for p in PRIMARY_CHECKOUT_PATHS:
        p_clean = p.rstrip("/")
        if path_clean == p_clean or path_clean.startswith(p_clean):
            return True
    return False


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

    print(f"  Total non-recurring sales: {len(all_sales)}, Edge line items: {len(edge_sales)}")
    return edge_sales


def fetch_source_checkout_page(email):
    """Fetch click data for a customer and find their source checkout page (entry point)."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    try:
        resp = requests.get(
            f"{HYROS_BASE_URL}/leads/clicks",
            headers=headers,
            params={"email": email, "pageSize": 50},
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        clicks = resp.json().get("result", [])

        # Find the earliest click on a primary checkout page
        source_page = None
        for click in reversed(clicks):  # reversed = earliest first
            page = click.get("page", "")
            if page and is_source_checkout_page(page):
                source_page = page
                break  # Take the earliest/first source checkout page

        return source_page
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 2: Group sales by customer and build summary
# ---------------------------------------------------------------------------
def build_data_summary(sales, report_date):
    """Group sales by customer (1 customer = 1 purchase) and build summary."""

    # Group line items by customer email
    customers = {}
    for sale in sales:
        email = sale.get("lead", {}).get("email", "unknown")
        if email not in customers:
            customers[email] = {
                "email": email,
                "line_items": [],
                "total_order_value": 0,
                "total_refunded": 0,
                "first_source": sale.get("firstSource"),
                "last_source": sale.get("lastSource"),
            }
        price_info = sale.get("price", {})
        revenue = price_info.get("price", 0) or 0
        refunded = price_info.get("refunded", 0) or 0

        customers[email]["line_items"].append({
            "product": sale.get("product", {}).get("name", "unknown"),
            "revenue": revenue,
            "refunded": refunded,
        })
        customers[email]["total_order_value"] += revenue
        customers[email]["total_refunded"] += refunded

    # Fetch source checkout pages for each customer
    print(f"Fetching source checkout pages for {len(customers)} customers...")
    for email, cust in customers.items():
        source_page = fetch_source_checkout_page(email)
        cust["source_checkout_page"] = source_page
        if source_page:
            print(f"  {email}: {source_page}")
        else:
            print(f"  {email}: no source page found")

    # Build aggregated metrics
    total_revenue = 0
    total_refunded = 0
    platforms = {}
    campaigns = {}
    products = {}
    source_pages = {}
    ad_creatives = {}  # ad name -> {purchases, revenue}
    purchase_details = []

    for email, cust in customers.items():
        order_value = cust["total_order_value"]
        refunded = cust["total_refunded"]
        total_revenue += order_value
        total_refunded += refunded

        # Attribution from first sale's sources
        first_source = cust["first_source"]
        if first_source:
            ts = first_source.get("trafficSource", {})
            if ts:
                platform = ts.get("name", "unknown")
                is_organic = first_source.get("organic", False)
                label = f"{platform} ({'organic' if is_organic else 'paid'})"
                platforms[label] = platforms.get(label, 0) + order_value

            cat = first_source.get("category", {})
            if cat:
                camp_name = cat.get("name", "unknown")
                campaigns[camp_name] = campaigns.get(camp_name, 0) + order_value

            # Ad creative tracking
            ad_info = first_source.get("sourceLinkAd", {})
            if ad_info:
                ad_name = ad_info.get("name", "unknown")
                if ad_name and ad_name != "unknown":
                    if ad_name not in ad_creatives:
                        ad_creatives[ad_name] = {"purchases": 0, "revenue": 0, "campaign": cat.get("name", "N/A") if cat else "N/A", "platform": ts.get("name", "N/A") if ts else "N/A"}
                    ad_creatives[ad_name]["purchases"] += 1
                    ad_creatives[ad_name]["revenue"] += order_value

        # Product breakdown (per line item)
        for item in cust["line_items"]:
            prod = item["product"]
            products[prod] = products.get(prod, 0) + item["revenue"]

        # Source checkout page
        page = cust.get("source_checkout_page")
        if page:
            # Clean to just the path for readability
            from urllib.parse import urlparse
            path = urlparse(page).path
            source_pages[path] = source_pages.get(path, 0) + 1

        # Build purchase detail
        last_source = cust["last_source"]
        purchase_details.append({
            "customer_email": email,
            "order_value": round(order_value, 2),
            "refunded": round(refunded, 2),
            "items": [item["product"] for item in cust["line_items"]],
            "first_touch_source": first_source.get("name", "N/A") if first_source else "N/A",
            "first_touch_platform": first_source.get("trafficSource", {}).get("name", "N/A") if first_source else "N/A",
            "first_touch_organic": first_source.get("organic", None) if first_source else None,
            "campaign": first_source.get("category", {}).get("name", "N/A") if first_source else "N/A",
            "ad_name": first_source.get("sourceLinkAd", {}).get("name", "N/A") if first_source and first_source.get("sourceLinkAd") else "N/A",
            "last_touch_source": last_source.get("name", "N/A") if last_source else "N/A",
            "last_touch_platform": last_source.get("trafficSource", {}).get("name", "N/A") if last_source else "N/A",
            "source_checkout_page": cust.get("source_checkout_page", "N/A"),
        })

    num_purchases = len(customers)
    aov = round(total_revenue / num_purchases, 2) if num_purchases > 0 else 0

    summary = {
        "report_date": report_date,
        "total_purchases": num_purchases,
        "total_revenue": round(total_revenue, 2),
        "total_refunded": round(total_refunded, 2),
        "net_revenue": round(total_revenue - total_refunded, 2),
        "average_order_value": aov,
        "revenue_by_platform": dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_campaign": dict(sorted(campaigns.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_product": dict(sorted(products.items(), key=lambda x: x[1], reverse=True)),
        "source_checkout_pages": dict(sorted(source_pages.items(), key=lambda x: x[1], reverse=True)),
        "ad_creatives": dict(sorted(ad_creatives.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "purchase_details": purchase_details,
    }

    return summary


# ---------------------------------------------------------------------------
# Step 3: Send to Claude for analysis
# ---------------------------------------------------------------------------
CLAUDE_SYSTEM_PROMPT = """You are a senior marketing analyst creating a daily performance report for NEW Edge subscriptions at Benzinga.

IMPORTANT CONTEXT:
- Each "purchase" = one unique customer. A customer may buy an annual plan + a multi-year upgrade in one session — that counts as ONE purchase with a combined order value.
- "Source checkout page" = the landing page where the customer entered the funnel (e.g., /benzinga-edge-5). Upgrade and thank-you pages are excluded.
- This report covers only NEW subscriptions (not recurring renewals).

Structure your report exactly like this:

# Edge Daily Report — {date}

## Quick Summary
2-3 sentences: total new purchases, revenue, AOV, and the headline takeaway.

## Key Metrics
- New Purchases: X
- Total Revenue: $X
- Net Revenue (after refunds): $X
- Average Order Value: $X
- Refunds: $X

## Product Mix
Break down by product variant with revenue. Note when customers bundle (e.g., Annual + 3-Year Upgrade).

## Source Checkout Pages
Which landing pages drove the new purchases. Show the page path and number of purchases from each.

## Attribution Breakdown
### First Touch (How They Found Us)
Break down by platform and campaign. Show revenue per source.

### Last Touch
Final touchpoint before purchase.

## Ad Creative Performance
Show which specific ad creatives drove purchases. Include the ad name, campaign it belongs to, platform, number of purchases, and revenue. This is critical for knowing which ads to scale and which to cut. If a purchase came from organic/unattributed traffic (no ad creative), note that separately.

## Notable Patterns & Actionable Insights
Combine patterns and 2-3 concrete recommendations into one section. Include ad-level recommendations when the data supports it (e.g., "Scale ad X — it drove Y purchases at $Z AOV").

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

    # Build summary (groups by customer, fetches source pages)
    summary = build_data_summary(sales, yesterday_str)
    print(f"  Total purchases: {summary['total_purchases']}")
    print(f"  Total revenue: ${summary['total_revenue']:,.2f}")
    print(f"  AOV: ${summary['average_order_value']:,.2f}")

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
