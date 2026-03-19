#!/usr/bin/env python3
"""
scripts/reddit_alerts.py
Reddit Activity Alerts

Compares current subreddit activity to previous run and generates
START/PAUSE alerts. Run daily — tells you which subreddits to
activate or deactivate in Reddit Ads Manager.

Usage:
    # Check for alerts
    .venv/bin/python3 scripts/reddit_alerts.py

    # Push alerts to Google Sheets
    .venv/bin/python3 scripts/reddit_alerts.py --sheets --sheet-id YOUR_ID
"""
import json
import os
import sys
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/seed-subreddits.json")
PREV_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/previous-activity.json")
CURR_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/activity-report.json")
ALERTS_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/alerts.json")

REDDIT = "https://www.reddit.com"
ATOM = "http://www.w3.org/2005/Atom"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")

# A subreddit is "active" if it posted within this many days
ACTIVE_THRESHOLD_DAYS = 3
# A subreddit is "dead" if no posts in this many days
DEAD_THRESHOLD_DAYS = 14

SIGNAL_TIERS = {
    "context engineering": 5, "context layer": 5, "context graph": 5,
    "enterprise context": 5, "ai context": 5, "context vacuum": 5,
    "semantic layer": 3, "ontology": 3, "knowledge graph": 3,
    "active metadata": 3, "data governance": 3, "data catalog": 3,
    "metadata management": 3, "data lineage": 3,
    "model context protocol": 3, "mcp server": 3,
    "ai hallucination": 2, "ai governance": 2,
    "data quality": 2, "data mesh": 2, "context window": 2,
    "ai agent": 1, "llm": 1, "rag": 1,
    "atlan": 4, "collibra": 4, "alation": 4, "informatica": 4,
    "unity catalog": 4, "purview": 4, "datahub": 4,
}


def fetch_latest_post(subreddit: str) -> dict:
    """Fetch the latest post from a subreddit."""
    url = f"{REDDIT}/r/{subreddit}/new/.rss"
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10", "-A", UA, url],
            capture_output=True, timeout=15)
        if result.returncode != 0 or not result.stdout:
            return {"title": "", "date": "", "days_ago": None, "url": "", "kw_hits": 0}
    except Exception:
        return {"title": "", "date": "", "days_ago": None, "url": "", "kw_hits": 0}

    try:
        root = ET.fromstring(result.stdout)
    except ET.ParseError:
        return {"title": "", "date": "", "days_ago": None, "url": "", "kw_hits": 0}

    entries = root.findall(f"{{{ATOM}}}entry")
    if not entries:
        return {"title": "", "date": "", "days_ago": None, "url": "", "kw_hits": 0}

    # Count keyword hits across all recent posts
    kw_hits = 0
    for entry in entries[:25]:
        title_el = entry.find(f"{{{ATOM}}}title")
        t = (title_el.text or "").lower() if title_el is not None else ""
        for kw in SIGNAL_TIERS:
            if kw in t:
                kw_hits += 1

    entry = entries[0]
    title_el = entry.find(f"{{{ATOM}}}title")
    title = (title_el.text or "").strip() if title_el is not None else ""

    post_url = ""
    for link in entry.findall(f"{{{ATOM}}}link"):
        if link.get("rel", "alternate") == "alternate":
            post_url = link.get("href", "")
            break

    pub_el = entry.find(f"{{{ATOM}}}published")
    date_str = ""
    days_ago = None
    if pub_el is not None and pub_el.text:
        date_str = pub_el.text.strip()[:10]
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_ago = (datetime.now(timezone.utc) - dt).days
        except ValueError:
            pass

    return {
        "title": title,
        "date": date_str,
        "days_ago": days_ago,
        "url": post_url,
        "kw_hits": kw_hits,
    }


def run(push_sheets: bool = False, sheet_id: str = "") -> None:
    # Load seeds
    with open(SEED_FILE) as f:
        seeds = json.load(f)["subreddits"]

    # Load previous activity (if exists)
    prev = {}
    if os.path.exists(PREV_FILE):
        with open(PREV_FILE) as f:
            prev_data = json.load(f)
        for s in prev_data.get("subreddits", []):
            prev[s["subreddit"]] = s

    print(f"\n{'='*70}")
    print(f"  Reddit Activity Alerts — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"  Checking {len(seeds)} subreddits...")
    print(f"{'='*70}\n")

    # Poll each subreddit
    current = []
    alerts = []

    for i, name in enumerate(seeds, 1):
        print(f"  [{i}/{len(seeds)}] r/{name}...", end="", file=sys.stderr)
        post = fetch_latest_post(name)
        time.sleep(2)

        entry = {
            "subreddit": name,
            "last_post_date": post["date"],
            "days_since_last_post": post["days_ago"],
            "last_post_title": post["title"],
            "last_post_url": post["url"],
            "keyword_hits": post["kw_hits"],
        }
        current.append(entry)

        # Generate alerts by comparing to previous state
        prev_entry = prev.get(name, {})
        prev_days = prev_entry.get("days_since_last_post")
        curr_days = post["days_ago"]

        if curr_days is None:
            alert_type = "PAUSE"
            reason = "Could not fetch — subreddit may be private or banned"
        elif curr_days <= ACTIVE_THRESHOLD_DAYS and (prev_days is None or prev_days > ACTIVE_THRESHOLD_DAYS):
            alert_type = "START"
            reason = f"Became active — last post {curr_days}d ago, was {prev_days or '?'}d"
        elif curr_days > DEAD_THRESHOLD_DAYS and (prev_days is not None and prev_days <= DEAD_THRESHOLD_DAYS):
            alert_type = "PAUSE"
            reason = f"Went cold — no posts in {curr_days} days"
        elif curr_days <= ACTIVE_THRESHOLD_DAYS and post["kw_hits"] >= 3:
            alert_type = "BOOST"
            reason = f"Active + {post['kw_hits']} context keyword hits — consider increasing budget"
        else:
            alert_type = None
            reason = None

        icon = {"START": "🟢", "PAUSE": "🔴", "BOOST": "⚡"}.get(alert_type, "")

        if alert_type:
            alerts.append({
                "action": alert_type,
                "subreddit": name,
                "reason": reason,
                "last_post": post["title"][:60],
                "last_post_date": post["date"],
                "keyword_hits": post["kw_hits"],
            })
            print(f" {icon} {alert_type}: {reason}", file=sys.stderr)
        else:
            status = "active" if curr_days is not None and curr_days <= ACTIVE_THRESHOLD_DAYS else "quiet"
            print(f" — {status} ({curr_days or '?'}d ago)", file=sys.stderr)

    # Save current as "previous" for next run
    os.makedirs(os.path.dirname(PREV_FILE), exist_ok=True)
    with open(PREV_FILE, "w") as f:
        json.dump({
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "subreddits": current,
        }, f, indent=2)

    # Save current activity report
    with open(CURR_FILE, "w") as f:
        json.dump({
            "monitored_at": datetime.now(timezone.utc).isoformat(),
            "total": len(current),
            "subreddits": current,
        }, f, indent=2)

    # Save alerts
    with open(ALERTS_FILE, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "alerts": alerts,
        }, f, indent=2)

    # Print alerts summary
    starts = [a for a in alerts if a["action"] == "START"]
    pauses = [a for a in alerts if a["action"] == "PAUSE"]
    boosts = [a for a in alerts if a["action"] == "BOOST"]

    print(f"\n{'='*70}")
    if not alerts:
        print(f"  No changes — all subreddits same status as last check.")
    else:
        print(f"  {len(alerts)} ALERTS:")
        print(f"{'='*70}")

        if starts:
            print(f"\n  🟢 START ADS ({len(starts)}):")
            for a in starts:
                print(f"     r/{a['subreddit']:<25} {a['reason']}")
                print(f"       Latest: {a['last_post']} ({a['last_post_date']})")

        if boosts:
            print(f"\n  ⚡ BOOST BUDGET ({len(boosts)}):")
            for a in boosts:
                print(f"     r/{a['subreddit']:<25} {a['reason']}")

        if pauses:
            print(f"\n  🔴 PAUSE ADS ({len(pauses)}):")
            for a in pauses:
                print(f"     r/{a['subreddit']:<25} {a['reason']}")

    # Always show full status
    print(f"\n  {'─'*68}")
    print(f"  {'Subreddit':<28} {'Last Post':<12} {'Days':>5} {'KW':>4}  Status")
    print(f"  {'─'*68}")
    for c in sorted(current, key=lambda x: x["days_since_last_post"] or 9999):
        d = c["days_since_last_post"]
        if d is not None and d <= ACTIVE_THRESHOLD_DAYS:
            status = "🟢 active"
        elif d is not None and d <= DEAD_THRESHOLD_DAYS:
            status = "🟡 quiet"
        else:
            status = "🔴 dead"
        days = str(d) if d is not None else "?"
        print(f"  r/{c['subreddit']:<26} {c['last_post_date']:<12} {days:>5} "
              f"{c['keyword_hits']:>4}  {status}")

    print(f"\n{'='*70}\n")

    if push_sheets and sheet_id:
        _push_alerts_to_sheets(alerts, current, sheet_id)


def _push_alerts_to_sheets(alerts, current, sheet_id):
    """Add alerts tab to Google Sheet."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import google.auth.transport.requests
    except ImportError:
        print("  [ERROR] google-api-python-client not installed", file=sys.stderr)
        return

    adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
    with open(adc_path) as f:
        adc = json.load(f)
    creds = Credentials(
        token=None, refresh_token=adc["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=adc["client_id"], client_secret=adc["client_secret"],
        quota_project_id=adc.get("quota_project_id"),
    )
    creds.refresh(google.auth.transport.requests.Request())
    service = build("sheets", "v4", credentials=creds)

    now = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
    tab_name = f"Alerts {now}"

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name, "index": 0}}}]},
        ).execute()
    except Exception as e:
        print(f"  [WARN] {e}", file=sys.stderr)
        return

    # Build rows
    header = ["Action", "Subreddit", "Reason", "Last Post Date", "Days Ago",
              "KW Hits", "Latest Conversation"]
    rows = [header]

    # Alerts first
    for a in alerts:
        rows.append([
            a["action"], f"r/{a['subreddit']}", a["reason"],
            a["last_post_date"], "", a["keyword_hits"], a["last_post"],
        ])

    # Then full status
    rows.append([])
    rows.append(["STATUS", "Subreddit", "", "Last Post Date", "Days Ago", "KW Hits", ""])
    for c in sorted(current, key=lambda x: x["days_since_last_post"] or 9999):
        d = c["days_since_last_post"]
        status = "ACTIVE" if d is not None and d <= 3 else ("QUIET" if d is not None and d <= 14 else "DEAD")
        rows.append([
            status, f"r/{c['subreddit']}", "",
            c["last_post_date"], d if d is not None else "",
            c["keyword_hits"], c["last_post_title"][:60],
        ])

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{tab_name}'!A1",
        valueInputOption="RAW", body={"values": rows},
    ).execute()

    # Format
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tab_id = [s["properties"]["sheetId"] for s in meta["sheets"]
              if s["properties"]["title"] == tab_name][0]
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [
            {"repeatCell": {
                "range": {"sheetId": tab_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 1.0, "alpha": 1.0},
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }},
            {"updateSheetProperties": {
                "properties": {"sheetId": tab_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }},
            {"autoResizeDimensions": {
                "dimensions": {"sheetId": tab_id, "dimension": "COLUMNS",
                              "startIndex": 0, "endIndex": 7},
            }},
        ]},
    ).execute()

    print(f"  Pushed to https://docs.google.com/spreadsheets/d/{sheet_id}/edit", file=sys.stderr)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Reddit Activity Alerts")
    parser.add_argument("--sheets", action="store_true")
    parser.add_argument("--sheet-id", default="")
    args = parser.parse_args()
    run(push_sheets=args.sheets, sheet_id=args.sheet_id)
