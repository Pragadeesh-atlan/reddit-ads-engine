#!/usr/bin/env python3
"""
scripts/reddit_ads_to_sheets.py
Push Reddit Ads pipeline results to Google Sheets for review and approval.

Creates a spreadsheet with 3 tabs:
  1. Discovery   — All discovered subreddits with relevance scores
  2. Activity    — Activity monitoring results (hot/warm/cold)
  3. Activation  — Budget allocation + campaign targeting plan

Usage:
    # Push all three agents' results
    .venv/bin/python3 scripts/reddit_ads_to_sheets.py

    # Push only specific agent results
    .venv/bin/python3 scripts/reddit_ads_to_sheets.py --agent discovery
    .venv/bin/python3 scripts/reddit_ads_to_sheets.py --agent activity
    .venv/bin/python3 scripts/reddit_ads_to_sheets.py --agent activation

    # Custom input directory
    .venv/bin/python3 scripts/reddit_ads_to_sheets.py --dir intelligence/reddit-ads
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import google.auth
import google.auth.transport.requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ADC_PATH = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
INTEL_DIR = "intelligence/reddit-ads"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_creds():
    """Load Google OAuth credentials from gcloud application default credentials."""
    if not os.path.exists(ADC_PATH):
        print(f"  [ERROR] No credentials found at {ADC_PATH}", file=sys.stderr)
        print(f"  Run: gcloud auth application-default login --scopes="
              f"https://www.googleapis.com/auth/spreadsheets,"
              f"https://www.googleapis.com/auth/drive", file=sys.stderr)
        sys.exit(1)

    with open(ADC_PATH) as f:
        adc = json.load(f)

    creds = Credentials(
        token=None,
        refresh_token=adc["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=adc["client_id"],
        client_secret=adc["client_secret"],
        quota_project_id=adc.get("quota_project_id"),
    )
    # Refresh to get a valid access token
    creds.refresh(google.auth.transport.requests.Request())
    return creds


# ── Tab 1: Discovery ────────────────────────────────────────────────────────


def build_discovery_rows(data: dict) -> list[list]:
    """Build rows for the Discovery tab."""
    header = [
        "Rank", "Subreddit", "Subscribers", "Relevance Score",
        "Tier", "Description", "RSS URL", "Score Breakdown",
    ]
    rows = [header]

    for i, sub in enumerate(data.get("subreddits", []), 1):
        score = sub.get("relevance_score", 0)
        if score >= 50:
            tier = "Tier 1 — High"
        elif score >= 30:
            tier = "Tier 2 — Medium"
        else:
            tier = "Tier 3 — Low"

        rows.append([
            i,
            f"r/{sub['name']}",
            sub.get("subscribers", 0),
            score,
            tier,
            sub.get("description", "")[:200],
            sub.get("rss_url", ""),
            " | ".join(sub.get("score_breakdown", [])),
        ])
    return rows


# ── Tab 2: Activity ─────────────────────────────────────────────────────────


def build_activity_rows(data: dict) -> list[list]:
    """Build rows for the Activity tab."""
    header = [
        "Rank", "Subreddit", "Status", "Activity Score", "Relevance Score",
        "Posts (7d)", "Posts (24h)", "Keyword Hits", "Avg Comments",
        "Subscribers", "Top Keyword Posts",
    ]
    rows = [header]

    threshold = data.get("activation_threshold", 50)
    all_results = data.get("all_results", [])

    for i, r in enumerate(all_results, 1):
        score = r.get("activity_score", 0)
        if score >= threshold:
            status = "HOT"
        elif score >= threshold * 0.6:
            status = "WARM"
        else:
            status = "COLD"

        # Format top keyword posts
        kw_posts = r.get("keyword_posts", [])
        kw_summary = " | ".join(
            f"{p['title'][:40]} [{', '.join(p.get('keywords', [])[:2])}]"
            for p in kw_posts[:3]
        )

        rows.append([
            i,
            f"r/{r['subreddit']}",
            status,
            score,
            r.get("relevance_score", 0),
            r.get("posts_7d", 0),
            r.get("posts_24h", 0),
            r.get("keyword_hits", 0),
            r.get("avg_comments", 0),
            r.get("subscribers", 0),
            kw_summary,
        ])
    return rows


# ── Tab 3: Activation ───────────────────────────────────────────────────────


def build_activation_rows(data: dict) -> list[list]:
    """Build rows for the Activation tab."""
    header = [
        "Rank", "Subreddit", "Action", "Daily Budget (USD)",
        "Budget Share %", "Activity Score", "Relevance Score",
        "Composite Score", "Subscribers", "Posts (7d)", "Keyword Hits",
        "Top Keyword Posts",
    ]
    rows = [header]

    activations = data.get("activations", [])
    deactivations = data.get("deactivations", [])

    for i, a in enumerate(activations, 1):
        kw_posts = a.get("keyword_posts", [])
        kw_summary = " | ".join(
            f"{p['title'][:40]} [{', '.join(p.get('keywords', [])[:2])}]"
            for p in kw_posts[:3]
        )
        rows.append([
            i,
            f"r/{a['subreddit']}",
            a.get("action", "ACTIVATE"),
            a.get("daily_budget_usd", 0),
            a.get("budget_share", 0),
            a.get("activity_score", 0),
            a.get("relevance_score", 0),
            a.get("composite_score", 0),
            a.get("subscribers", 0),
            a.get("posts_7d", 0),
            a.get("keyword_hits", 0),
            kw_summary,
        ])

    for d in deactivations:
        rows.append([
            "",
            f"r/{d['subreddit']}",
            "DEACTIVATE",
            0,
            0,
            d.get("activity_score", 0),
            "",
            "",
            "",
            "",
            "",
            d.get("reason", ""),
        ])

    # Summary row
    total_budget = data.get("total_daily_budget", 0)
    active_count = len([a for a in activations if a.get("action") == "ACTIVATE"])
    rows.append([])
    rows.append(["", "TOTAL", "", total_budget, "100%", "", "", "", "", "", "", ""])
    rows.append(["", f"Subreddits to activate: {active_count}", "", "", "",
                 "", "", "", "", "", "", ""])
    changes = data.get("changes_from_previous", [])
    if changes:
        rows.append(["", f"Changes: {'; '.join(changes)}", "", "", "",
                     "", "", "", "", "", "", ""])

    return rows


# ── Spreadsheet creation ─────────────────────────────────────────────────────


def create_spreadsheet(
    creds,
    title: str,
    tabs: list[tuple[str, list[list]]],
) -> str:
    """
    Create a Google Spreadsheet with the given tabs.
    Returns the spreadsheet URL.
    """
    service = build("sheets", "v4", credentials=creds)

    # Create spreadsheet with tabs
    body = {
        "properties": {"title": title},
        "sheets": [
            {"properties": {"title": tab_name, "index": i}}
            for i, (tab_name, _) in enumerate(tabs)
        ],
    }
    print(f"  Creating spreadsheet: {title}", file=sys.stderr)
    result = service.spreadsheets().create(body=body).execute()
    spreadsheet_id = result["spreadsheetId"]
    url = result["spreadsheetUrl"]
    print(f"  Spreadsheet ID: {spreadsheet_id}", file=sys.stderr)

    # Write data to each tab
    for tab_name, rows in tabs:
        if not rows:
            continue
        print(f"  Writing {len(rows)-1} rows to '{tab_name}'", file=sys.stderr)
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

    # Format all tabs: bold headers, freeze row, auto-resize, conditional formatting
    sheet_ids = [s["properties"]["sheetId"] for s in result["sheets"]]
    requests = []

    for idx, (tab_name, rows) in enumerate(tabs):
        sid = sheet_ids[idx]
        num_cols = len(rows[0]) if rows else 10

        # Bold header with blue background
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 1.0, "alpha": 1.0},
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        })
        # Freeze header row
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        })
        # Auto-resize columns
        requests.append({
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sid,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_cols,
                },
            }
        })

    # Activity tab: color-code status column (column C, index 2)
    if len(tabs) >= 2 and tabs[1][1]:
        activity_sid = sheet_ids[1]
        activity_rows = tabs[1][1]
        # Green for HOT, Yellow for WARM, Grey for COLD
        for row_idx, row in enumerate(activity_rows[1:], 1):
            if len(row) > 2:
                status = row[2]
                if status == "HOT":
                    color = {"red": 0.85, "green": 0.95, "blue": 0.85}
                elif status == "WARM":
                    color = {"red": 1.0, "green": 0.95, "blue": 0.8}
                else:
                    color = {"red": 0.92, "green": 0.92, "blue": 0.92}
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": activity_sid,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": 2,
                            "endColumnIndex": 3,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {**color, "alpha": 1.0},
                                "textFormat": {"bold": True},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                })

    # Activation tab: color-code action column (column C, index 2)
    if len(tabs) >= 3 and tabs[2][1]:
        activation_sid = sheet_ids[2]
        activation_rows = tabs[2][1]
        for row_idx, row in enumerate(activation_rows[1:], 1):
            if len(row) > 2:
                action = row[2]
                if action == "ACTIVATE":
                    color = {"red": 0.85, "green": 0.95, "blue": 0.85}
                elif action == "DEACTIVATE":
                    color = {"red": 0.95, "green": 0.85, "blue": 0.85}
                elif action == "WAITLIST":
                    color = {"red": 1.0, "green": 0.95, "blue": 0.8}
                else:
                    continue
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": activation_sid,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": 2,
                            "endColumnIndex": 3,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {**color, "alpha": 1.0},
                                "textFormat": {"bold": True},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                })

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    return url


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push Reddit Ads pipeline results to Google Sheets."
    )
    parser.add_argument(
        "--dir",
        default=INTEL_DIR,
        help=f"Directory with pipeline output files (default: {INTEL_DIR})",
    )
    parser.add_argument(
        "--agent",
        choices=["discovery", "activity", "activation", "all"],
        default="all",
        help="Which agent results to push (default: all)",
    )
    args = parser.parse_args()

    # Load data files
    discovery_path = os.path.join(args.dir, "subreddits.json")
    activity_path = os.path.join(args.dir, "activity-report.json")
    activation_path = os.path.join(args.dir, "activation-plan.json")

    tabs = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    if args.agent in ("discovery", "all"):
        if os.path.exists(discovery_path):
            with open(discovery_path) as f:
                discovery_data = json.load(f)
            tabs.append(("Discovery", build_discovery_rows(discovery_data)))
            print(f"  Loaded Discovery: {discovery_data['total_subreddits']} subreddits", file=sys.stderr)
        else:
            print(f"  [SKIP] Discovery file not found: {discovery_path}", file=sys.stderr)

    if args.agent in ("activity", "all"):
        if os.path.exists(activity_path):
            with open(activity_path) as f:
                activity_data = json.load(f)
            tabs.append(("Activity", build_activity_rows(activity_data)))
            summary = activity_data.get("summary", {})
            print(f"  Loaded Activity: {summary.get('hot', 0)} hot, "
                  f"{summary.get('warm', 0)} warm, {summary.get('cold', 0)} cold", file=sys.stderr)
        else:
            print(f"  [SKIP] Activity file not found: {activity_path}", file=sys.stderr)

    if args.agent in ("activation", "all"):
        if os.path.exists(activation_path):
            with open(activation_path) as f:
                activation_data = json.load(f)
            tabs.append(("Activation", build_activation_rows(activation_data)))
            active = [a for a in activation_data.get("activations", [])
                      if a.get("action") == "ACTIVATE"]
            print(f"  Loaded Activation: {len(active)} subreddits, "
                  f"${activation_data.get('total_daily_budget', 0):.2f}/day", file=sys.stderr)
        else:
            print(f"  [SKIP] Activation file not found: {activation_path}", file=sys.stderr)

    if not tabs:
        print("  [ERROR] No data files found. Run the pipeline first.", file=sys.stderr)
        sys.exit(1)

    # Create spreadsheet
    creds = get_creds()
    title = f"Reddit Ads Pipeline — {now} UTC"
    url = create_spreadsheet(creds, title, tabs)

    print(f"\n  Spreadsheet URL: {url}", file=sys.stderr)
    print(url)  # Print URL to stdout for easy capture


if __name__ == "__main__":
    main()
