"""
Weekly Hyros Edge Report Generator
Pulls the previous week's (Mon-Sun) NEW Edge subscription data from Hyros,
analyzes it with Claude, and outputs:
  1. Styled HTML email
  2. HTML report saved to GitHub Pages archive
  3. Weekly metrics row appended to 'Weekly Report' tab in Google Sheet
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

# ---------------------------------------------------------------------------
# Import shared helpers from daily_report
# ---------------------------------------------------------------------------
from daily_report import (
    HYROS_API_KEY,
    ANTHROPIC_API_KEY,
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_SHEET_ID,
    RESEND_API_KEY,
    EMAIL_RECIPIENT,
    TEST_EMAIL,
    TEST_MODE,
    HYROS_BASE_URL,
    US_EASTERN,
    fetch_new_edge_sales,
    fetch_customer_click_data,
    get_product_display_name,
    is_source_checkout_page,
    save_report_file,
)


GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "avmcdermot/hyros-daily-report")


# ---------------------------------------------------------------------------
# Helper: extract date string from a Hyros sale object
# ---------------------------------------------------------------------------
def extract_sale_date(sale):
    """Try multiple date fields and formats (ISO string or Unix ms timestamp)."""
    for field in ("createdDate", "date", "saleDate", "created_at"):
        val = sale.get(field)
        if val is None:
            continue
        # Unix timestamp in milliseconds (number or numeric string)
        if isinstance(val, (int, float)):
            dt = datetime.fromtimestamp(val / 1000, tz=US_EASTERN)
            return dt.strftime("%Y-%m-%d")
        val_str = str(val).strip()
        if not val_str:
            continue
        # Numeric string (Unix ms)
        if val_str.isdigit():
            dt = datetime.fromtimestamp(int(val_str) / 1000, tz=US_EASTERN)
            return dt.strftime("%Y-%m-%d")
        # ISO 8601 string like "2026-03-25T14:30:00..."
        if len(val_str) >= 10 and val_str[4:5] == "-":
            return val_str[:10]
    return "unknown"


# ---------------------------------------------------------------------------
# Date range: previous Monday through Sunday
# ---------------------------------------------------------------------------
def get_last_week_range():
    """Return (start, end) for the previous Mon-Sun week in US Eastern."""
    now_eastern = datetime.now(US_EASTERN)
    today = now_eastern.replace(hour=0, minute=0, second=0, microsecond=0)
    # today.weekday(): Monday=0, Sunday=6
    # We want last Monday: go back to this Monday, then subtract 7
    days_since_monday = today.weekday()  # 0 if today is Monday
    this_monday = today - timedelta(days=days_since_monday)
    last_monday = this_monday - timedelta(days=7)
    last_sunday_end = this_monday - timedelta(seconds=1)
    return last_monday.isoformat(), last_sunday_end.isoformat()


# ---------------------------------------------------------------------------
# Build weekly summary (same logic as daily but with daily breakdown)
# ---------------------------------------------------------------------------
def build_weekly_summary(sales, week_label, start_date, end_date):
    """Build aggregated weekly summary + per-day breakdown."""
    from urllib.parse import urlparse

    customers = {}
    daily_revenue = {}  # {date_str: revenue}
    daily_purchases = {}  # {date_str: count}

    # Debug: print first sale's keys so we can see what date fields exist
    if sales:
        sample = sales[0]
        print(f"  [DEBUG] Sample sale keys: {list(sample.keys())}")
        for field in ("createdDate", "date", "saleDate", "created_at"):
            if field in sample:
                print(f"  [DEBUG] {field} = {sample[field]} (type: {type(sample[field]).__name__})")

    for sale in sales:
        email = sale.get("lead", {}).get("email", "unknown")

        # Track daily breakdown by sale date
        day_str = extract_sale_date(sale)

        if email not in customers:
            customers[email] = {
                "email": email,
                "line_items": [],
                "total_order_value": 0,
                "total_refunded": 0,
                "first_source": sale.get("firstSource"),
                "last_source": sale.get("lastSource"),
                "purchase_day": day_str,
            }

        price_info = sale.get("price", {})
        revenue = price_info.get("price", 0) or 0
        refunded = price_info.get("refunded", 0) or 0

        customers[email]["line_items"].append({
            "product": get_product_display_name(sale),
            "revenue": revenue,
            "refunded": refunded,
        })
        customers[email]["total_order_value"] += revenue
        customers[email]["total_refunded"] += refunded

    # Build daily breakdown from customers
    for email, cust in customers.items():
        day = cust["purchase_day"]
        daily_purchases[day] = daily_purchases.get(day, 0) + 1
        daily_revenue[day] = daily_revenue.get(day, 0) + cust["total_order_value"]

    # Fetch click data
    print(f"Fetching click data for {len(customers)} customers...")
    for email, cust in customers.items():
        click_data = fetch_customer_click_data(email)
        cust["source_checkout_page"] = click_data["source_checkout_page"]
        cust["utms"] = click_data["utms"]
        cust["device"] = click_data["device"]

    # Aggregate (same as daily)
    total_revenue = 0
    total_refunded = 0
    platforms = {}
    campaigns = {}
    products = {}
    source_pages = {}
    ad_creatives = {}
    utm_campaigns = {}
    utm_sources = {}
    utm_ads = {}
    utm_ad_types = {}
    devices = {}

    for email, cust in customers.items():
        order_value = cust["total_order_value"]
        refunded = cust["total_refunded"]
        total_revenue += order_value
        total_refunded += refunded

        device = cust.get("device", "Unknown")
        devices[device] = devices.get(device, 0) + 1

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
            if prod not in products:
                products[prod] = {"revenue": 0, "count": 0, "refunded": 0}
            products[prod]["revenue"] += item["revenue"]
            products[prod]["refunded"] += item["refunded"]
            products[prod]["count"] += 1

        page = cust.get("source_checkout_page")
        if page:
            path = urlparse(page).path
            source_pages[path] = source_pages.get(path, 0) + 1

        utms = cust.get("utms", {})
        utm_camp = utms.get("utm_campaign", "")
        if utm_camp:
            if utm_camp not in utm_campaigns:
                utm_campaigns[utm_camp] = {"purchases": 0, "revenue": 0}
            utm_campaigns[utm_camp]["purchases"] += 1
            utm_campaigns[utm_camp]["revenue"] += order_value

        utm_src = utms.get("utm_source", "")
        if utm_src:
            if utm_src not in utm_sources:
                utm_sources[utm_src] = {"purchases": 0, "revenue": 0}
            utm_sources[utm_src]["purchases"] += 1
            utm_sources[utm_src]["revenue"] += order_value

        utm_ad = utms.get("utm_ad", "")
        if utm_ad:
            if utm_ad not in utm_ads:
                utm_ads[utm_ad] = {"purchases": 0, "revenue": 0}
            utm_ads[utm_ad]["purchases"] += 1
            utm_ads[utm_ad]["revenue"] += order_value

        utm_ad_type = utms.get("utm_adType", "") or utms.get("utm_ad_type", "")
        if utm_ad_type:
            if utm_ad_type not in utm_ad_types:
                utm_ad_types[utm_ad_type] = {"purchases": 0, "revenue": 0}
            utm_ad_types[utm_ad_type]["purchases"] += 1
            utm_ad_types[utm_ad_type]["revenue"] += order_value

    num_purchases = len(customers)
    aov = round(total_revenue / num_purchases, 2) if num_purchases > 0 else 0

    # Sort daily breakdown by date
    sorted_daily = sorted(daily_revenue.keys())
    daily_breakdown = []
    for day in sorted_daily:
        daily_breakdown.append({
            "date": day,
            "purchases": daily_purchases.get(day, 0),
            "revenue": round(daily_revenue.get(day, 0), 2),
        })

    summary = {
        "report_type": "weekly",
        "week_label": week_label,
        "start_date": start_date,
        "end_date": end_date,
        "total_purchases": num_purchases,
        "total_revenue": round(total_revenue, 2),
        "total_refunded": round(total_refunded, 2),
        "net_revenue": round(total_revenue - total_refunded, 2),
        "average_order_value": aov,
        "daily_breakdown": daily_breakdown,
        "revenue_by_platform": dict(sorted(platforms.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_campaign": dict(sorted(campaigns.items(), key=lambda x: x[1], reverse=True)),
        "revenue_by_product": dict(sorted(products.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "source_checkout_pages": dict(sorted(source_pages.items(), key=lambda x: x[1], reverse=True)),
        "ad_creatives": dict(sorted(ad_creatives.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_campaigns": dict(sorted(utm_campaigns.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_sources": dict(sorted(utm_sources.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_ads": dict(sorted(utm_ads.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_ad_types": dict(sorted(utm_ad_types.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "devices": dict(sorted(devices.items(), key=lambda x: x[1], reverse=True)),
    }

    return summary


# ---------------------------------------------------------------------------
# Claude prompt for weekly report
# ---------------------------------------------------------------------------
WEEKLY_CLAUDE_PROMPT = """You are a senior marketing analyst creating a WEEKLY performance report for NEW Edge subscriptions at Benzinga.

IMPORTANT CONTEXT:
- Each "purchase" = one unique customer. A customer may buy an annual plan + a multi-year upgrade in one session — that counts as ONE purchase with a combined order value.
- "Source checkout page" = the landing page where the customer entered the funnel (e.g., /benzinga-edge-5). Upgrade and thank-you pages are excluded.
- This report covers only NEW subscriptions (not recurring renewals).
- This is a WEEKLY report covering Monday through Sunday. Focus on trends, patterns, and week-level insights.

OUTPUT FORMAT: You must return EMAIL-SAFE HTML. This is critical:
- Use ONLY HTML tables for ALL layout (no flexbox, no grid, no CSS float — these break in Gmail)
- ALL styles must be INLINE on each element (no <style> blocks — Gmail strips them)
- Wrap everything in a centered table with max-width: 640px
- Use cellpadding, cellspacing, and border attributes on tables
- Use bgcolor attributes as backup for background colors
- Use align="center" on wrapper tables

Use the Benzinga dark theme with CLEAR visual hierarchy:
- Page/email background: #000725 (deep navy)
- Content containers: #0A1E4A (slightly lighter navy) with 12px border-radius, 1px solid #1B3D82 border, 20px padding
- Table header rows: #1B3D82 (slate blue)
- Table body rows: alternate between #0D2255 and #0A1E4A
- KPI cards: #071A47 with 1px solid #1B3D82 border
- Amber #F07520: KPI numbers, dollar amounts, count badges, accent borders
- White #F8F9FB: headings, table header text, KPI labels
- Light grey #B0B8C4: body text
- Steel grey #5A6B7A: footnotes, captions
- Font: 'DM Sans', Arial, sans-serif

KPI number colors: Purchases=#F8F9FB (white), Gross Revenue=#F07520 (amber), AOV=#F07520 (amber), Net Revenue=#198754 (green). Net Revenue must ALWAYS be green (#198754).

Count badges: amber (#F07520) circle with #000725 dark number inside.

Structure:
1. Header banner: "BENZINGA EDGE · WEEKLY REPORT" with week date range and "New Subscriptions Only" subtitle. Include an "ALL REPORTS" button linking to https://docs.google.com/spreadsheets/d/1tfnFznCmxaRTx5PFk9-vm4Jzv-EJ2uVRFSZVm7FNyJQ/edit — styled as: display:inline-block, padding:8px 18px, background:#F07520, color:#000725, font-weight:bold, font-size:12px, letter-spacing:1px, text-decoration:none, border-radius:4px
2. KPI row (4 cells): Total Purchases | Gross Revenue | AOV | Net Revenue
3. Device split (small text below KPIs)
4. **Daily Breakdown table** — THIS IS KEY FOR WEEKLY: show each day (Mon-Sun) with purchases and revenue per day. Highlight the best day in amber. This lets the reader see which days performed best.
5. Product Mix table (with count badges, net revenue)
6. Source Checkout Pages table
7. UTM Breakdown (campaigns, sources, ads, ad types — show ALL rows)
8. Attribution: First Touch breakdown
9. Attribution: Last Touch breakdown
10. Weekly Patterns & Strategic Insights (3-5 bullet points — more depth than daily since this is a weekly review. Compare best vs worst days, identify trends, suggest actions for next week.)

NOTE: Do NOT include an Individual Purchase Details table.

CRITICAL: You must include ALL sections listed above, especially the Daily Breakdown table.

Contextual footnotes below every data table (same as daily report style).

Refund handling: same as daily — red REFUNDED badges, footnotes with specifics.

Make it scannable — KPIs tell the weekly story in 5 seconds, daily breakdown shows the rhythm, and insights give strategic direction."""


def analyze_weekly_with_claude(summary):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=32768,
        timeout=600,
        system=WEEKLY_CLAUDE_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Generate the styled HTML weekly report for this data:\n\n{json.dumps(summary, indent=2)}",
            }
        ],
    )

    return message.content[0].text


# ---------------------------------------------------------------------------
# Send weekly email
# ---------------------------------------------------------------------------
def send_weekly_email(html_content, week_label):
    if not all([RESEND_API_KEY, EMAIL_RECIPIENT]):
        print("  Email not configured. Skipping.")
        return False

    if TEST_MODE and TEST_EMAIL:
        recipients = [TEST_EMAIL]
        subject = f"[TEST] Edge Weekly Report — {week_label}"
        print(f"  TEST MODE: sending only to {TEST_EMAIL}")
    else:
        recipients = [addr.strip() for addr in EMAIL_RECIPIENT.split(",")]
        subject = f"Edge Weekly Report — {week_label}"

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": "Edge Weekly Report <edge-report@avmstrategygroup.com>",
                "to": recipients,
                "subject": subject,
                "html": html_content,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            print(f"  Email sent to {', '.join(recipients)}")
            return True
        else:
            print(f"  Email failed ({resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        print(f"  Email failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Append to 'Weekly Report' tab in Google Sheet
# ---------------------------------------------------------------------------
def append_weekly_to_sheet(summary, report_link=""):
    if not GOOGLE_SHEET_ID:
        print("  Google Sheet not configured. Skipping.")
        return

    creds_json = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build("sheets", "v4", credentials=creds)

    # Ensure "Weekly Report" sheet exists
    try:
        sheet_meta = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
        sheet_names = [s["properties"]["title"] for s in sheet_meta.get("sheets", [])]
        if "Weekly Report" not in sheet_names:
            service.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": "Weekly Report"}}}]},
            ).execute()
            headers = [[
                "Week", "Start Date", "End Date", "Purchases", "Revenue",
                "Net Revenue", "AOV", "Refunded", "Avg Daily Purchases",
                "Best Day", "Top Source Page", "Top Platform", "Top Campaign",
                "Full Report"
            ]]
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="'Weekly Report'!A1",
                valueInputOption="RAW",
                body={"values": headers},
            ).execute()
    except Exception as e:
        print(f"  Error creating Weekly Report tab: {e}")
        return

    # Find best day
    best_day = "N/A"
    best_rev = 0
    for day in summary.get("daily_breakdown", []):
        if day["revenue"] > best_rev:
            best_rev = day["revenue"]
            best_day = day["date"]

    avg_daily = round(summary["total_purchases"] / 7, 1) if summary["total_purchases"] > 0 else 0
    top_source = list(summary["source_checkout_pages"].keys())[0] if summary["source_checkout_pages"] else "N/A"
    top_platform = list(summary["revenue_by_platform"].keys())[0] if summary["revenue_by_platform"] else "N/A"
    top_campaign = list(summary["revenue_by_campaign"].keys())[0] if summary["revenue_by_campaign"] else "N/A"

    row = [[
        summary["week_label"],
        summary["start_date"][:10],
        summary["end_date"][:10],
        summary["total_purchases"],
        summary["total_revenue"],
        summary["net_revenue"],
        summary["average_order_value"],
        summary["total_refunded"],
        avg_daily,
        best_day,
        top_source,
        top_platform,
        top_campaign,
        report_link,
    ]]

    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="'Weekly Report'!A:N",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": row},
    ).execute()

    print(f"  Weekly metrics appended to Sheet")


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

    # Get last week's date range (Mon-Sun)
    from_date, to_date = get_last_week_range()
    start_dt = datetime.fromisoformat(from_date)
    end_dt = datetime.fromisoformat(to_date)
    week_label = f"{start_dt.strftime('%B %d')} — {end_dt.strftime('%B %d, %Y')}"
    print(f"Generating WEEKLY Edge report for {week_label}...")
    print(f"  Date range: {from_date} to {to_date}")

    # Pull data
    print("Fetching new Edge subscriptions from Hyros...")
    sales = fetch_new_edge_sales(from_date, to_date)

    # Debug: print all customer emails from Hyros so we can cross-check
    hyros_emails = set()
    for sale in sales:
        email = sale.get("lead", {}).get("email", "unknown")
        hyros_emails.add(email.lower())
    print(f"  [DEBUG] Unique customer emails from Hyros ({len(hyros_emails)}):")
    for e in sorted(hyros_emails):
        print(f"    {e}")

    # Build weekly summary
    summary = build_weekly_summary(sales, week_label, from_date, to_date)
    print(f"  Purchases: {summary['total_purchases']}")
    print(f"  Revenue: ${summary['total_revenue']:,.2f}")
    print(f"  AOV: ${summary['average_order_value']:,.2f}")
    if summary.get("daily_breakdown"):
        print(f"  Daily breakdown:")
        for day in summary["daily_breakdown"]:
            print(f"    {day['date']}: {day['purchases']} purchases, ${day['revenue']:,.2f}")

    # Generate HTML report
    print("Generating weekly HTML report with Claude...")
    html_report = analyze_weekly_with_claude(summary)
    if html_report.startswith("```"):
        html_report = html_report.split("\n", 1)[1]
    if html_report.endswith("```"):
        html_report = html_report.rsplit("```", 1)[0]
    html_report = html_report.strip()
    print("  Report generated")

    # Output 1: Email
    print("Sending weekly email...")
    send_weekly_email(html_report, week_label)

    # Output 2: Save HTML file
    print("Saving report file...")
    import re
    date_slug = f"week-{start_dt.strftime('%Y-%m-%d')}"
    filepath = f"docs/reports/{date_slug}.html"
    os.makedirs("docs/reports", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html_report)
    print(f"  Saved report to {filepath}")
    report_link = f"https://{GITHUB_REPO.split('/')[0]}.github.io/{GITHUB_REPO.split('/')[1]}/reports/{date_slug}.html"

    # Output 3: Google Sheet (Weekly Report tab)
    print("Appending to Weekly Report sheet...")
    append_weekly_to_sheet(summary, report_link)

    print("Done!")


if __name__ == "__main__":
    main()
