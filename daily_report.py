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


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HYROS_API_KEY = os.environ.get("HYROS_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
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

    # Step 1: Find Edge customers and their sales
    edge_sales = []
    edge_customers = set()
    for sale in all_sales:
        product = sale.get("product", {})
        name = (product.get("name") or "").lower()
        tag = (product.get("tag") or "").lower()
        if "edge" in name or "edge" in tag:
            edge_sales.append(sale)
            email = sale.get("lead", {}).get("email", "")
            if email:
                edge_customers.add(email)

    # Step 2: Find Trade Alerts upsells by those same Edge customers
    upsell_sales = []
    for sale in all_sales:
        email = sale.get("lead", {}).get("email", "")
        if email not in edge_customers:
            continue
        product = sale.get("product", {})
        name = (product.get("name") or "").lower()
        tag = (product.get("tag") or "").lower()
        if "trade" in name or "trade-alerts" in tag:
            # Avoid duplicates (in case it was already captured)
            if sale not in edge_sales:
                upsell_sales.append(sale)

    combined = edge_sales + upsell_sales
    print(f"  Total non-recurring sales: {len(all_sales)}, Edge line items: {len(edge_sales)}, Trade Alerts upsells: {len(upsell_sales)}")
    return combined


def fetch_customer_click_data(email):
    """Fetch source checkout page, UTM params, and device from click data."""
    headers = {"API-Key": HYROS_API_KEY, "Accept": "application/json"}
    result = {"source_checkout_page": None, "utms": {}, "device": "Unknown"}
    try:
        resp = requests.get(
            f"{HYROS_BASE_URL}/leads/clicks",
            headers=headers,
            params={"email": email, "pageSize": 100},
            timeout=15,
        )
        if resp.status_code != 200:
            return result
        clicks = resp.json().get("result", [])

        # Find source checkout page
        for click in reversed(clicks):
            page = click.get("page", "")
            if page and is_source_checkout_page(page):
                result["source_checkout_page"] = page
                break

        # Collect all UTM params across clicks
        for click in clicks:
            parsed = click.get("parsedParameters", {})
            for k, v in parsed.items():
                clean_key = k.replace("amp;", "")  # fix malformed &amp; params
                if "utm" in clean_key.lower() and clean_key not in result["utms"]:
                    result["utms"][clean_key] = v

        # Detect device from user agent
        for click in clicks:
            agent = click.get("agent", "")
            if agent:
                agent_lower = agent.lower()
                if "mobile" in agent_lower or "android" in agent_lower or "iphone" in agent_lower:
                    result["device"] = "Mobile"
                else:
                    result["device"] = "Desktop"
                break

        return result
    except Exception:
        return result


# ---------------------------------------------------------------------------
# Step 2: Build summary (grouped by customer)
# ---------------------------------------------------------------------------
def get_product_display_name(sale):
    """Map Hyros product names to readable display names using price and tag."""
    product = sale.get("product", {})
    name = product.get("name", "unknown")
    tag = (product.get("tag") or "").lower()
    price = sale.get("price", {}).get("price", 0) or 0

    name_lower = name.lower()

    if "trade" in name_lower or "trade-alerts" in tag:
        return "Trade Alerts Upsell"
    if "3-year" in name_lower or "3-year-upg" in tag:
        return "Benzinga Edge 3-Year Upgrade"
    if "2-year" in name_lower or "2-year" in tag:
        return "Benzinga Edge 2-Year Upgrade"
    if "79-year" in tag:
        return "Benzinga Edge Annual $79"
    if "year-129" in tag:
        return "Benzinga Edge Annual $129"
    if "year-199" in tag or ("annual" in tag and "199" in tag):
        return "Benzinga Edge Annual $199"
    if "month" in tag or price == 19:
        return "Benzinga Edge Monthly $19"
    if "7-day" in tag or "trial" in tag or price == 19:
        return "Benzinga Edge 7-Day Trial"

    # Fallback: use price to differentiate
    if price <= 19:
        return f"Benzinga Edge Trial/Monthly (${price})"
    elif price <= 99:
        return f"Benzinga Edge Annual (${price})"
    elif price <= 199:
        return f"Benzinga Edge Annual ${price}"
    else:
        return f"{name} (${price})"


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
            "product": get_product_display_name(sale),
            "revenue": revenue,
            "refunded": refunded,
        })
        customers[email]["total_order_value"] += revenue
        customers[email]["total_refunded"] += refunded

    # Fetch click data (source pages, UTMs, device)
    print(f"Fetching click data for {len(customers)} customers...")
    for email, cust in customers.items():
        click_data = fetch_customer_click_data(email)
        cust["source_checkout_page"] = click_data["source_checkout_page"]
        cust["utms"] = click_data["utms"]
        cust["device"] = click_data["device"]

    # Aggregate
    total_revenue = 0
    total_refunded = 0
    platforms = {}
    campaigns = {}
    products = {}  # {name: {"revenue": X, "count": Y}}
    source_pages = {}
    ad_creatives = {}
    utm_campaigns = {}  # {campaign: {"purchases": X, "revenue": Y}}
    utm_sources = {}    # {source: {"purchases": X, "revenue": Y}}
    utm_ads = {}        # {ad: {"purchases": X, "revenue": Y}}
    devices = {}        # {device: count}
    purchase_details = []

    for email, cust in customers.items():
        order_value = cust["total_order_value"]
        refunded = cust["total_refunded"]
        total_revenue += order_value
        total_refunded += refunded

        # Device tracking
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

        # Product mix with counts
        for item in cust["line_items"]:
            prod = item["product"]
            if prod not in products:
                products[prod] = {"revenue": 0, "count": 0}
            products[prod]["revenue"] += item["revenue"]
            products[prod]["count"] += 1

        # Source checkout pages
        page = cust.get("source_checkout_page")
        if page:
            path = urlparse(page).path
            source_pages[path] = source_pages.get(path, 0) + 1

        # UTM aggregation
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

        # Also track utm_adType as a separate breakdown
        utm_ad_type = utms.get("utm_adType", "") or utms.get("utm_ad_type", "")

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
            "utm_source": utms.get("utm_source", "N/A"),
            "utm_campaign": utms.get("utm_campaign", "N/A"),
            "utm_medium": utms.get("utm_medium", "N/A"),
            "utm_ad": utms.get("utm_ad", "N/A"),
            "utm_ad_type": utm_ad_type or "N/A",
            "device": device,
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
        "revenue_by_product": dict(sorted(products.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "source_checkout_pages": dict(sorted(source_pages.items(), key=lambda x: x[1], reverse=True)),
        "ad_creatives": dict(sorted(ad_creatives.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_campaigns": dict(sorted(utm_campaigns.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_sources": dict(sorted(utm_sources.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "utm_ads": dict(sorted(utm_ads.items(), key=lambda x: x[1]["revenue"], reverse=True)),
        "devices": dict(sorted(devices.items(), key=lambda x: x[1], reverse=True)),
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

OUTPUT FORMAT: You must return EMAIL-SAFE HTML. This is critical:
- Use ONLY HTML tables for ALL layout (no flexbox, no grid, no CSS float — these break in Gmail)
- ALL styles must be INLINE on each element (no <style> blocks — Gmail strips them)
- Wrap everything in a centered table with max-width: 640px
- Use cellpadding, cellspacing, and border attributes on tables
- Use bgcolor attributes as backup for background colors
- Use align="center" on wrapper tables

Use the Benzinga dark theme with CLEAR visual hierarchy:
- Page/email background: #000725 (deep navy) — the ENTIRE report is on this dark background
- Content containers: #0A1E4A (slightly lighter navy) — used for ALL section containers, giving clear separation from the page background. Each section (Product Mix, UTM, etc.) gets its own container with this background, 12px border-radius, 1px solid #1B3D82 border, and 20px padding.
- Table header rows: #1B3D82 (slate blue) — strong contrast from the container background
- Table body rows: alternate between #0D2255 and #0A1E4A — subtle striping that's visible but not jarring
- KPI cards: #071A47 with 1px solid #1B3D82 border
- Amber #F07520: KPI numbers, dollar amounts, count badges, accent borders, section header left borders
- White #F8F9FB: headings, table header text, KPI labels, key numbers
- Light grey #B0B8C4: body text in tables, regular content
- Steel grey #5A6B7A: footnotes, captions, muted text
- Silver #D7DADE: used sparingly for emphasis text
- Font: 'DM Sans', Arial, sans-serif

DESIGN PRINCIPLE: Full dark theme, but with CLEAR LAYERS. The page is #000725, containers are #0A1E4A with visible borders, table headers are #1B3D82. This creates 3 distinct depth levels so content doesn't blend together. Every section is wrapped in its own bordered container with padding and margin-bottom for breathing room.

Layout rules:
- The entire email body: background #000725
- Header banner: seamless with the page background, just text and an amber divider line
- KPI cards: single <table>, one <tr>, 4 <td> cells (25% width). #071A47 background, 1px solid #1B3D82 border, 8px border-radius. Amber numbers, white labels. 8px cellspacing.
- Section containers: each section (Product Mix, UTM, Source Pages, etc.) is wrapped in a <table> with bgcolor="#0A1E4A", 1px solid #1B3D82 border, 12px border-radius, 20px cellpadding. 24px margin-bottom between sections.
- Section headers: #F8F9FB white text, uppercase, letter-spacing:2px, with 4px left border in amber. INSIDE the container, not floating outside.
- Data tables INSIDE containers: header row #1B3D82, body rows alternate #0D2255 / #0A1E4A. Text: #B0B8C4 for regular, #F8F9FB for important, #F07520 for dollar amounts. Cell borders: 1px solid rgba(27,61,130,0.5).
- Badges/pills: display:inline-block, padding:3px 10px, border-radius:12px, font-size:11px. Facebook=#1B3D82, Google=#198754, Organic=#5A6B7A, Yahoo=#6f42c1, Robinhood=#198754, Unknown=#5A6B7A — white text.
- Count badges: #F07520 circle, #000725 dark number inside
- Positive: #198754 green. Negative/refunds: #dc3545 red.
- Footnotes: #5A6B7A italic, 12px, inside the container at bottom

Product Mix table rules:
- Columns: Product | Count | Revenue | % of Gross
- The "Count" column should show a small amber (#F07520) circular badge with the number inside (display:inline-block, width:28px, height:28px, line-height:28px, text-align:center, border-radius:50%, background:#F07520, color:#000725, font-weight:bold, font-size:13px)

UTM Breakdown table rules:
- Show three sub-tables: UTM Campaigns, UTM Sources, UTM Ads
- Each sub-table: columns are Name | Purchases | Revenue
- IMPORTANT: You MUST render all data rows from the utm_campaigns, utm_sources, and utm_ads objects. Do NOT skip data or show empty tables. Each key in these objects is a row.

Device split: show a small inline note near the KPIs or below them, e.g. "Mobile: 6 | Desktop: 5"

Structure:
1. Header banner: navy background with "Benzinga EDGE · DAILY REPORT" text, report date, "New Subscriptions Only" subtitle
2. KPI row (4 cells in one table row): Purchases | Gross Revenue | AOV | Net Revenue
3. Device split (small text below KPIs)
4. Product Mix table (with count badges)
5. Source Checkout Pages table (which landing pages drove purchases)
6. UTM Breakdown (utm_campaign, utm_source, utm_ad tables — MUST show all rows with data)
7. Attribution: First Touch breakdown (platform + campaign with revenue) — MUST include this section
8. Attribution: Last Touch breakdown — MUST include this section
9. Individual Purchase Details table (email, order value, items, source page, first touch, utm_campaign, utm_ad, utm_ad_type, device)
10. Notable Patterns & Actionable Insights section (2-3 bullet points, concise)

CRITICAL: You must include ALL sections listed above. Do not skip or omit any section even if the data seems sparse.

Contextual footnotes:
- Below EVERY data table, add a small italic grey (#5A6B7A) footnote with useful context. Examples:
  - Source Checkout Pages: "* X purchases had no trackable checkout page (direct/unknown entry)" if some are N/A
  - UTM tables: "* X purchases had no UTM data (direct traffic or untagged links)" if some customers lack UTMs
  - First Touch: "* Based on Hyros first-click attribution model"
  - Product Mix: if there are refunds, add a footnote like "* 1 refund of $129 on Annual $199 (angelamagno516@yahoo.com)" with the specific details
- These footnotes add crucial context. Always include them.

Refund handling:
- In the Product Mix table, if a product has a refund, show a small red "REFUNDED" badge or strikethrough on that row
- In the KPI section, the refund should be clearly called out under Gross Revenue (already doing this with "-$X refund")
- In the Individual Purchase Details table, clearly mark refunded orders with a red "REFUNDED" badge

Make it scannable — a busy executive should get the key story in 5 seconds from the KPIs. Keep written analysis to 2-3 punchy bullet points max."""


def analyze_with_claude(summary):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=32768,
        timeout=600,
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
# Step 4b: Save HTML report to repo (for GitHub Pages archive)
# ---------------------------------------------------------------------------
def save_report_file(html_content, report_date):
    """Save HTML report to docs/reports/ for GitHub Pages hosting."""
    import re
    date_slug = re.sub(r'[^a-zA-Z0-9]+', '-', report_date).strip('-').lower()
    filepath = f"docs/reports/{date_slug}.html"
    os.makedirs("docs/reports", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"  Saved report to {filepath}")
    return filepath, date_slug


# ---------------------------------------------------------------------------
# Step 4c: Append metrics row to Google Sheet
# ---------------------------------------------------------------------------
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "avmcdermot/hyros-daily-report")


def append_to_sheet(summary, report_link=""):
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
        headers = [[
            "Date", "Purchases", "Revenue", "Net Revenue", "AOV",
            "Refunded", "Top Source Page", "Top Platform", "Top Campaign",
            "Top Ad Creative", "Full Report"
        ]]
        service.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="A1",
            valueInputOption="RAW",
            body={"values": headers},
        ).execute()

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
        report_link,
    ]]

    service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="A:K",
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

    # Output 2: Save HTML file to repo (for GitHub Pages archive)
    print("Saving report file...")
    filepath, date_slug = save_report_file(html_report, yesterday_str)
    report_link = f"https://{GITHUB_REPO.split('/')[0]}.github.io/{GITHUB_REPO.split('/')[1]}/reports/{date_slug}.html"

    # Output 3: Google Sheet metrics (with link to full report)
    print("Appending to Google Sheet...")
    append_to_sheet(summary, report_link)

    print("Done!")


if __name__ == "__main__":
    main()
