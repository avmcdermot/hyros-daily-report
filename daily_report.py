"""
Daily Hyros Edge Report Generator
Pulls yesterday's NEW Edge subscription data from Hyros, analyzes it with Claude,
and outputs:
  1. Styled HTML email to your inbox
  2. HTML report saved to Google Drive (archive)
  3. Key metrics row appended to Google Sheet (trend tracking)
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import anthropic
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HYROS_API_KEY = os.environ.get("HYROS_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "").strip()
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "").strip()

HYROS_BASE_URL = "https://api.hyros.com/v1/api/v1.0"
US_EASTERN = timezone(timedelta(hours=-4))  # EDT

# Primary checkout pages (entry points into the Edge funnel)
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
    """Check if a URL is a primary checkout/source page."""
    path = urlparse(url).path.rstrip("/")
    for p in PRIMARY_CHECKOUT_PATHS:
        if path == p.rstrip("/"):
            return True
    return False


# ---------------------------------------------------------------------------
# Step 1: Pull data from Hyros
# ---------------------------------------------------------------------------
def get_yesterday_range():
    now_eastern = datetime.now(US_EASTERN)
    today_eastern = now_eastern.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_eastern - timedelta(days=1)
    yesterday_end = today_eastern - timedelta(seconds=1)
    return yesterday_start.isoformat(), yesterday_end.isoformat()


def fetch_new_edge_sales(from_date, to_date):
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

        for click in reversed(clicks):
            page = click.get("page", "")
            if page and is_source_checkout_page(page):
                return page
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 2: Build summary (grouped by customer)
# ---------------------------------------------------------------------------
def build_data_summary(sales, report_date):
    # Group by customer
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

    # Fetch source pages
    print(f"Fetching source checkout pages for {len(customers)} customers...")
    for email, cust in customers.items():
        source_page = fetch_source_checkout_page(email)
        cust["source_checkout_page"] = source_page

    # Aggregate
    total_revenue = 0
    total_refunded = 0
    platforms = {}
    campaigns = {}
    products = {}
    source_pages = {}
    ad_creatives = {}
    purchase_details = []

    for email, cust in customers.items():
        order_value = cust["total_order_value"]
        refunded = cust["total_refunded"]
        total_revenue += order_value
        total_refunded += refunded

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

            ad_info = first_source.get("sourceLinkAd", {})
            if ad_info:
                ad_name = ad_info.get("name", "")
                if ad_name:
                    if ad_name not in ad_creatives:
                        ad_creatives[ad_name] = {
                            "purchases": 0, "revenue": 0,
                            "campaign": cat.get("name", "N/A") if cat else "N/A",
                            "platform": ts.get("name", "N/A") if ts else "N/A",
                        }
                    ad_creatives[ad_name]["purchases"] += 1
                    ad_creatives[ad_name]["revenue"] += order_value

        for item in cust["line_items"]:
            prod = item["product"]
            products[prod] = products.get(prod, 0) + item["revenue"]

        page = cust.get("source_checkout_page")
        if page:
            path = urlparse(page).path
            source_pages[path] = source_pages.get(path, 0) + 1

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
            "source_checkout_page": urlparse(page).path if page else "N/A",
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
# Step 3: Claude generates styled HTML report
# ---------------------------------------------------------------------------
CLAUDE_SYSTEM_PROMPT = """You are a senior marketing analyst creating a daily performance report for NEW Edge subscriptions at Benzinga.

IMPORTANT CONTEXT:
- Each "purchase" = one unique customer. A customer may buy an annual plan + a multi-year upgrade in one session — that counts as ONE purchase with a combined order value.
- "Source checkout page" = the landing page where the customer entered the funnel (e.g., /benzinga-edge-5). Upgrade and thank-you pages are excluded.
- This report covers only NEW subscriptions (not recurring renewals).

OUTPUT FORMAT: You must return a complete, self-contained HTML document with inline CSS styling. The report should look professional and be easy to scan.

Use the Benzinga brand design system with these EXACT colors and fonts:
- Navy (background): #000725
- Surface (cards/sections): #071A47
- Amber (accent/highlights): #F07520
- White (text): #F8F9FB
- Blue (secondary): #1B3D82
- Grey (muted text): #5A6B7A
- Silver (borders/dividers): #D7DADE
- Font: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif
- Body background: #020B1A
- Text color: #B0B8C4 for body, #FFFFFF for headings and key numbers
- KPI cards: #071A47 background with amber (#F07520) large bold numbers, arranged in a horizontal row
- Tables: #071A47 background, alternating rows with #000725, white text, amber for important numbers
- Section headers: use uppercase letter-spacing labels in grey, with amber left-border accent (4px solid #F07520)
- Badges/pills for platforms: Facebook=#1B3D82, Google=#198754, Organic=#5A6B7A with white text
- Positive metrics in #198754 (green), negative/refunds in #dc3545 (red)
- Links and highlights in amber #F07520

Structure:
1. Header with report date and Edge logo placeholder
2. KPI row: Purchases | Revenue | AOV | Net Revenue
3. Product Mix table
4. Source Checkout Pages table (which landing pages drove purchases)
5. Attribution: First Touch breakdown (platform + campaign with revenue)
6. Ad Creative Performance table (ad name, campaign, platform, purchases, revenue)
7. Last Touch breakdown
8. Individual Purchase Details table (email, order value, items, source page, first touch, ad)
9. Notable Patterns & Actionable Insights section

Make it scannable — a busy executive should get the key story in 5 seconds from the KPIs, then drill into tables as needed. Keep written analysis concise and punchy."""


def analyze_with_claude(summary):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=CLAUDE_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Generate the styled HTML daily report for this data:\n\n{json.dumps(summary, indent=2)}",
            }
        ],
    )

    return message.content[0].text


# ---------------------------------------------------------------------------
# Step 4a: Send HTML email via Resend
# ---------------------------------------------------------------------------
def send_email(html_content, report_date):
    if not all([RESEND_API_KEY, EMAIL_RECIPIENT]):
        print("  Email not configured (missing RESEND_API_KEY or EMAIL_RECIPIENT). Skipping.")
        return False

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": "Edge Daily Report <onboarding@resend.dev>",
                "to": [addr.strip() for addr in EMAIL_RECIPIENT.split(",")],
                "subject": f"Edge Daily Report — {report_date}",
                "html": html_content,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            print(f"  Email sent to {EMAIL_RECIPIENT}")
            return True
        else:
            print(f"  Email failed ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        print(f"  Email failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Step 4b: Save HTML to Google Drive
# ---------------------------------------------------------------------------
def save_to_drive(html_content, report_date):
    if not GOOGLE_DRIVE_FOLDER_ID:
        print("  Google Drive not configured. Skipping.")
        return

    creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=creds)

    filename = f"Edge Report - {report_date}.html"
    file_metadata = {
        "name": filename,
        "parents": [GOOGLE_DRIVE_FOLDER_ID],
        "mimeType": "text/html",
    }
    media = MediaInMemoryUpload(html_content.encode("utf-8"), mimetype="text/html")

    file = service.files().create(
        body=file_metadata, media_body=media, fields="id,webViewLink"
    ).execute()

    print(f"  Saved to Drive: {file.get('webViewLink', file.get('id'))}")


# ---------------------------------------------------------------------------
# Step 4c: Append metrics row to Google Sheet
# ---------------------------------------------------------------------------
def append_to_sheet(summary):
    if not GOOGLE_SHEET_ID:
        print("  Google Sheet not configured. Skipping.")
        return

    creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Check if header row exists
    result = service.spreadsheets().values().get(
        spreadsheetId=GOOGLE_SHEET_ID, range="A1:A1"
    ).execute()
    existing = result.get("values", [])

    if not existing:
        # Write header row first
        headers = [[
            "Date", "Purchases", "Revenue", "Net Revenue", "AOV",
            "Refunded", "Top Source Page", "Top Platform", "Top Campaign", "Top Ad Creative"
        ]]
        service.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="A1",
            valueInputOption="RAW",
            body={"values": headers},
        ).execute()

    # Build data row
    top_source = list(summary["source_checkout_pages"].keys())[0] if summary["source_checkout_pages"] else "N/A"
    top_platform = list(summary["revenue_by_platform"].keys())[0] if summary["revenue_by_platform"] else "N/A"
    top_campaign = list(summary["revenue_by_campaign"].keys())[0] if summary["revenue_by_campaign"] else "N/A"
    top_ad = list(summary["ad_creatives"].keys())[0] if summary["ad_creatives"] else "N/A"

    row = [[
        summary["report_date"],
        summary["total_purchases"],
        summary["total_revenue"],
        summary["net_revenue"],
        summary["average_order_value"],
        summary["total_refunded"],
        top_source,
        top_platform,
        top_campaign,
        top_ad,
    ]]

    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="A:J",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": row},
    ).execute()

    print(f"  Metrics appended to Sheet")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    missing = []
    for var in ["HYROS_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_SERVICE_ACCOUNT_JSON"]:
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

    # Pull data
    print("Fetching new Edge subscriptions from Hyros...")
    sales = fetch_new_edge_sales(from_date, to_date)

    # Build summary (groups by customer, fetches source pages)
    summary = build_data_summary(sales, yesterday_str)
    print(f"  Purchases: {summary['total_purchases']}")
    print(f"  Revenue: ${summary['total_revenue']:,.2f}")
    print(f"  AOV: ${summary['average_order_value']:,.2f}")

    # Generate HTML report
    print("Generating HTML report with Claude...")
    html_report = analyze_with_claude(summary)
    # Strip markdown code fences if Claude wraps the HTML
    if html_report.startswith("```"):
        html_report = html_report.split("\n", 1)[1]
    if html_report.endswith("```"):
        html_report = html_report.rsplit("```", 1)[0]
    html_report = html_report.strip()
    print("  Report generated")

    # Output 1: Email
    print("Sending email...")
    send_email(html_report, yesterday_str)

    # Output 2: Google Drive archive (optional, may fail for service accounts)
    if GOOGLE_DRIVE_FOLDER_ID:
        print("Saving to Google Drive...")
        try:
            save_to_drive(html_report, yesterday_str)
        except Exception as e:
            print(f"  Drive save skipped (non-fatal): {e}")

    # Output 3: Google Sheet metrics
    print("Appending to Google Sheet...")
    append_to_sheet(summary)

    print("Done!")


if __name__ == "__main__":
    main()
