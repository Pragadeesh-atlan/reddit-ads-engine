#!/usr/bin/env python3
"""
scripts/reddit_monitor.py
Skill: Reddit Activity Monitor (Agent 2)

Polls RSS feeds from seed subreddits, gets the latest conversation from each,
and classifies subreddits as active/stale. Outputs a report with the last post
date and title for each subreddit.

Input: intelligence/reddit-ads/seed-subreddits.json (from Agent 1)
Output: intelligence/reddit-ads/activity-report.json

Usage:
    # Monitor all seed subreddits
    .venv/bin/python3 scripts/reddit_monitor.py

    # Push results to Google Sheets (new tab on existing sheet)
    .venv/bin/python3 scripts/reddit_monitor.py --sheets

    # Use a specific sheet ID
    .venv/bin/python3 scripts/reddit_monitor.py --sheets --sheet-id 1_DVfc1...

No API key required. Uses Reddit's public RSS endpoints.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

REDDIT = "https://www.reddit.com"
ATOM = "http://www.w3.org/2005/Atom"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/seed-subreddits.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/activity-report.json")

# Signal keywords with tier weights
SIGNAL_TIERS = {
    # Tier 1 (5): Atlan's exact category language
    "context engineering": 5, "context layer": 5, "context graph": 5,
    "enterprise context": 5, "ai context": 5, "context vacuum": 5,
    # Tier 2 (3): Adjacent positioning
    "semantic layer": 3, "ontology": 3, "knowledge graph": 3,
    "active metadata": 3, "data governance": 3, "data catalog": 3,
    "metadata management": 3, "data lineage": 3,
    "model context protocol": 3, "mcp server": 3,
    # Tier 3 (2): Problems Atlan solves
    "ai hallucination": 2, "ai governance": 2,
    "data quality": 2, "data mesh": 2, "data fabric": 2,
    "context window": 2, "retrieval augmented": 2,
    # Tier 4 (1): Broad but relevant
    "ai agent": 1, "llm": 1, "rag": 1,
    # Tier 5 (4): Competitor mentions
    "atlan": 4, "collibra": 4, "alation": 4, "informatica": 4,
    "unity catalog": 4, "purview": 4, "datahub": 4,
}


# ── RSS fetcher ──────────────────────────────────────────────────────────────


def fetch_rss(subreddit: str) -> list[dict]:
    """Fetch latest posts from a subreddit's RSS feed."""
    url = f"{REDDIT}/r/{subreddit}/new/.rss"
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "12", "-A", UA, url],
            capture_output=True, timeout=15,
        )
        if result.returncode != 0 or not result.stdout:
            return []
    except (subprocess.TimeoutExpired, Exception):
        return []

    try:
        root = ET.fromstring(result.stdout)
    except ET.ParseError:
        return []

    posts = []
    for entry in root.findall(f"{{{ATOM}}}entry"):
        title_el = entry.find(f"{{{ATOM}}}title")
        title = (title_el.text or "").strip() if title_el is not None else ""

        post_url = ""
        for link in entry.findall(f"{{{ATOM}}}link"):
            if link.get("rel", "alternate") == "alternate":
                post_url = link.get("href", "")
                break

        pub_el = entry.find(f"{{{ATOM}}}published")
        pub_str = pub_el.text.strip() if pub_el is not None and pub_el.text else ""
        pub_dt = None
        if pub_str:
            try:
                pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except ValueError:
                pass

        content_el = entry.find(f"{{{ATOM}}}content")
        content = ""
        if content_el is not None and content_el.text:
            import html, re
            content = html.unescape(content_el.text)
            content = re.sub(r"<[^>]+>", " ", content)
            content = re.sub(r"\s+", " ", content).strip()[:500]

        posts.append({
            "title": title,
            "url": post_url,
            "content": content,
            "published": pub_str[:10] if pub_str else "",
            "pub_dt": pub_dt,
        })

    return posts


# ── Subreddit info fetcher ───────────────────────────────────────────────────


def fetch_subscriber_count(subreddit: str) -> int:
    """Fetch subscriber count from Reddit JSON API."""
    url = f"{REDDIT}/r/{subreddit}/about.json"
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10", "-A", UA, url],
            capture_output=True, timeout=15,
        )
        if result.returncode != 0 or not result.stdout:
            return 0
        data = json.loads(result.stdout.decode("utf-8"))
        return data.get("data", {}).get("subscribers") or 0
    except Exception:
        return 0


# ── Scoring ──────────────────────────────────────────────────────────────────


def score_posts(posts: list[dict]) -> dict:
    """Score posts for context signal relevance."""
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    week_posts = [p for p in posts if p.get("pub_dt") and p["pub_dt"] >= week_ago]

    keyword_hits = 0
    weighted_hits = 0
    keyword_posts = []

    for p in week_posts[:25]:
        text = (p.get("title", "") + " " + p.get("content", "")).lower()
        matches = []
        post_weight = 0
        for kw, weight in SIGNAL_TIERS.items():
            if kw in text:
                matches.append(kw)
                post_weight += weight
        if matches:
            keyword_hits += len(matches)
            weighted_hits += post_weight
            keyword_posts.append({
                "title": p["title"][:80],
                "keywords": matches[:5],
                "weight": post_weight,
                "url": p.get("url", ""),
                "date": p.get("published", ""),
            })

    keyword_posts.sort(key=lambda x: x["weight"], reverse=True)
    return {
        "posts_7d": len(week_posts),
        "keyword_hits": keyword_hits,
        "weighted_hits": weighted_hits,
        "keyword_posts": keyword_posts[:5],
    }


# ── Main ─────────────────────────────────────────────────────────────────────


def run(push_sheets: bool = False, sheet_id: str = "") -> None:
    # Load seeds
    if not os.path.exists(SEED_FILE):
        print(f"  [ERROR] Seed file not found: {SEED_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(SEED_FILE) as f:
        seed_data = json.load(f)
    names = seed_data["subreddits"]

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Reddit Activity Monitor — {len(names)} subreddits", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    results = []
    for i, name in enumerate(names, 1):
        print(f"  [{i}/{len(names)}] r/{name}...", end="", file=sys.stderr)

        # Fetch RSS
        posts = fetch_rss(name)
        time.sleep(1)

        # Fetch subscriber count
        subs = fetch_subscriber_count(name)
        time.sleep(1)

        # Latest post
        if posts:
            latest = posts[0]
            last_title = latest["title"]
            last_date = latest.get("published", "")
            last_url = latest.get("url", "")
            days_ago = None
            if latest.get("pub_dt"):
                days_ago = (datetime.now(timezone.utc) - latest["pub_dt"]).days
        else:
            last_title = "(no posts found)"
            last_date = ""
            last_url = ""
            days_ago = None

        # Score for context signal
        scores = score_posts(posts)

        # Classify
        if days_ago is not None and days_ago <= 1:
            status = "active"
        elif days_ago is not None and days_ago <= 7:
            status = "recent"
        elif days_ago is not None and days_ago <= 30:
            status = "slow"
        else:
            status = "dead"

        entry = {
            "subreddit": name,
            "subscribers": subs,
            "status": status,
            "last_post_date": last_date,
            "days_since_last_post": days_ago,
            "last_post_title": last_title,
            "last_post_url": last_url,
            "posts_7d": scores["posts_7d"],
            "keyword_hits": scores["keyword_hits"],
            "weighted_keyword_hits": scores["weighted_hits"],
            "top_keyword_posts": scores["keyword_posts"],
        }
        results.append(entry)

        status_icon = {"active": "🟢", "recent": "🟡", "slow": "🟠", "dead": "🔴"}.get(status, "⚪")
        print(f" {status_icon} {last_date or 'n/a'} ({days_ago or '?'}d ago) "
              f"kw={scores['keyword_hits']}  {last_title[:50]}", file=sys.stderr)

    # Sort: active first, then by days since last post
    results.sort(key=lambda r: (
        {"active": 0, "recent": 1, "slow": 2, "dead": 3}.get(r["status"], 4),
        r["days_since_last_post"] or 9999,
    ))

    # Summary
    active = [r for r in results if r["status"] == "active"]
    recent = [r for r in results if r["status"] == "recent"]
    slow = [r for r in results if r["status"] == "slow"]
    dead = [r for r in results if r["status"] == "dead"]

    report = {
        "monitored_at": datetime.now(timezone.utc).isoformat(),
        "total": len(results),
        "summary": {
            "active": len(active),
            "recent": len(recent),
            "slow": len(slow),
            "dead": len(dead),
        },
        "subreddits": results,
    }

    # Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  🟢 Active (today): {len(active)} | 🟡 Recent (this week): {len(recent)} "
          f"| 🟠 Slow (this month): {len(slow)} | 🔴 Dead: {len(dead)}", file=sys.stderr)
    print(f"  Saved to {OUTPUT_FILE}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # Print table
    print(f"\n{'#':<3} {'Subreddit':<28} {'Subs':>10} {'Status':<8} {'Last Post':<12} "
          f"{'Days':>5} {'KW':>4}  Latest Conversation")
    print("─" * 120)
    for i, r in enumerate(results, 1):
        subs_str = f"{r['subscribers']:,}" if r['subscribers'] else "?"
        days = str(r['days_since_last_post']) if r['days_since_last_post'] is not None else "?"
        icon = {"active": "🟢", "recent": "🟡", "slow": "🟠", "dead": "🔴"}.get(r["status"], "⚪")
        print(f"{i:<3} r/{r['subreddit']:<26} {subs_str:>10} {icon:<8} "
              f"{r['last_post_date']:<12} {days:>5} {r['keyword_hits']:>4}  "
              f"{r['last_post_title'][:50]}")

    # Push to sheets
    if push_sheets:
        _push_to_sheets(results, sheet_id)


def _push_to_sheets(results: list[dict], sheet_id: str) -> None:
    """Add a 'Latest Posts' tab to the Google Sheet."""
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

    if not sheet_id:
        # Create new spreadsheet
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        body = {
            "properties": {"title": f"Reddit Monitor — {now} UTC"},
            "sheets": [{"properties": {"title": "Latest Posts"}}],
        }
        result = service.spreadsheets().create(body=body).execute()
        sheet_id = result["spreadsheetId"]
        print(f"  Created new sheet: {result['spreadsheetUrl']}", file=sys.stderr)
    else:
        # Add tab to existing sheet
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": [{"addSheet": {"properties": {
                    "title": f"Monitor {datetime.now(timezone.utc).strftime('%m-%d %H:%M')}",
                    "index": 0,
                }}}]},
            ).execute()
        except Exception as e:
            print(f"  [WARN] Could not add tab: {e}", file=sys.stderr)

    # Get tab name
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tab_name = meta["sheets"][0]["properties"]["title"]
    tab_id = meta["sheets"][0]["properties"]["sheetId"]

    # Build rows
    header = ["#", "Subreddit", "Subscribers", "Status", "Last Post Date",
              "Days Ago", "Posts/7d", "KW Hits", "Latest Conversation", "Post URL"]
    rows = [header]
    for i, r in enumerate(results, 1):
        rows.append([
            i,
            f"r/{r['subreddit']}",
            r["subscribers"],
            r["status"].upper(),
            r["last_post_date"],
            r["days_since_last_post"] if r["days_since_last_post"] is not None else "",
            r["posts_7d"],
            r["keyword_hits"],
            r["last_post_title"],
            r["last_post_url"],
        ])

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    # Format
    requests = [
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
                          "startIndex": 0, "endIndex": 10},
        }},
    ]

    # Color code rows by status
    colors = {
        "ACTIVE": {"red": 0.85, "green": 0.95, "blue": 0.85},
        "RECENT": {"red": 1.0, "green": 0.95, "blue": 0.8},
        "SLOW": {"red": 1.0, "green": 0.9, "blue": 0.85},
        "DEAD": {"red": 0.95, "green": 0.85, "blue": 0.85},
    }
    for row_idx, row in enumerate(rows[1:], 1):
        status = row[3]
        if status in colors:
            requests.append({"repeatCell": {
                "range": {"sheetId": tab_id, "startRowIndex": row_idx,
                         "endRowIndex": row_idx + 1},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {**colors[status], "alpha": 1.0},
                }},
                "fields": "userEnteredFormat(backgroundColor)",
            }})

    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests},
    ).execute()

    print(f"  Pushed to https://docs.google.com/spreadsheets/d/{sheet_id}/edit", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Reddit Activity Monitor — poll seed subreddits.")
    parser.add_argument("--sheets", action="store_true", help="Push to Google Sheets")
    parser.add_argument("--sheet-id", default="", help="Existing spreadsheet ID to add tab to")
    args = parser.parse_args()
    run(push_sheets=args.sheets, sheet_id=args.sheet_id)


if __name__ == "__main__":
    main()
