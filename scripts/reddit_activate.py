#!/usr/bin/env python3
"""
scripts/reddit_activate.py
Skill: Reddit Ad Activation (Agent 3)

Reads Agent 2's activity report and generates an activation plan:
- Which subreddits to target (active + have context-relevant discussions)
- Budget allocation per subreddit
- Which subreddits to pause (went cold since last run)

Input: intelligence/reddit-ads/activity-report.json (from Agent 2)
Output: intelligence/reddit-ads/activation-plan.json

Usage:
    # Generate activation plan with $50/day budget
    .venv/bin/python3 scripts/reddit_activate.py

    # Custom budget
    .venv/bin/python3 scripts/reddit_activate.py --budget 100

    # Push to Google Sheets
    .venv/bin/python3 scripts/reddit_activate.py --sheets

    # Add tab to existing sheet
    .venv/bin/python3 scripts/reddit_activate.py --sheets --sheet-id 1_DVfc1...
"""
import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ACTIVITY_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/activity-report.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/activation-plan.json")
HISTORY_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/activation-history.json")

MIN_SUBSCRIBERS = 100  # Don't target tiny subreddits


def run(budget: float = 50.0, push_sheets: bool = False, sheet_id: str = "") -> None:
    # Load activity report
    if not os.path.exists(ACTIVITY_FILE):
        print(f"  [ERROR] Activity report not found: {ACTIVITY_FILE}", file=sys.stderr)
        print(f"  Run Agent 2 (reddit_monitor.py) first.", file=sys.stderr)
        sys.exit(1)

    with open(ACTIVITY_FILE) as f:
        report = json.load(f)

    subreddits = report.get("subreddits", [])

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Reddit Ad Activation — ${budget:.0f}/day budget", file=sys.stderr)
    print(f"  Input: {len(subreddits)} subreddits from activity report", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # ── Filter: only active subreddits with enough subscribers ───────────
    eligible = []
    skipped = []
    for s in subreddits:
        status = s.get("status", "dead")
        subs = s.get("subscribers") or 0
        name = s["subreddit"]

        if status == "dead":
            skipped.append({"subreddit": name, "reason": "dead — no recent posts"})
        elif subs < MIN_SUBSCRIBERS:
            skipped.append({"subreddit": name, "reason": f"too small ({subs} subscribers)"})
        else:
            eligible.append(s)

    print(f"  Eligible: {len(eligible)} | Skipped: {len(skipped)}", file=sys.stderr)

    if not eligible:
        print("  [WARN] No eligible subreddits for activation.", file=sys.stderr)
        return

    # ── Allocate budget proportional to engagement signal ────────────────
    # Score = weighted_keyword_hits * log(subscribers)
    # Subreddits with more context discussions + larger audiences get more budget
    for s in eligible:
        wkh = s.get("weighted_keyword_hits", 0)
        subs = max(s.get("subscribers", 0), 1)
        # Keyword weight matters most, subscriber count is a multiplier
        s["_budget_score"] = max(wkh, 1) * math.log10(subs)

    total_score = sum(s["_budget_score"] for s in eligible)

    activations = []
    for s in eligible:
        share = s["_budget_score"] / total_score if total_score > 0 else 0
        daily = round(budget * share, 2)
        # Floor at $2/day minimum
        if daily < 2.0:
            daily = 2.0

        activations.append({
            "subreddit": s["subreddit"],
            "subscribers": s.get("subscribers", 0),
            "status": s["status"],
            "last_post_date": s.get("last_post_date", ""),
            "days_since_last_post": s.get("days_since_last_post"),
            "posts_7d": s.get("posts_7d", 0),
            "keyword_hits": s.get("keyword_hits", 0),
            "weighted_keyword_hits": s.get("weighted_keyword_hits", 0),
            "daily_budget_usd": daily,
            "budget_share_pct": round(share * 100, 1),
            "last_post_title": s.get("last_post_title", ""),
            "last_post_url": s.get("last_post_url", ""),
            "top_keyword_posts": s.get("top_keyword_posts", [])[:3],
        })

    # Sort by budget allocation (highest first)
    activations.sort(key=lambda a: a["daily_budget_usd"], reverse=True)
    total_allocated = sum(a["daily_budget_usd"] for a in activations)

    # ── Compare with previous plan ──────────────────────────────────────
    prev_subs = set()
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
            if history.get("plans"):
                prev_subs = set(history["plans"][-1].get("subreddits", []))
        except (json.JSONDecodeError, KeyError):
            pass

    curr_subs = {a["subreddit"] for a in activations}
    new_adds = curr_subs - prev_subs
    removed = prev_subs - curr_subs

    # ── Build plan ──────────────────────────────────────────────────────
    plan = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "daily_budget_usd": budget,
        "total_allocated_usd": round(total_allocated, 2),
        "subreddits_to_activate": len(activations),
        "subreddits_skipped": len(skipped),
        "activations": activations,
        "skipped": skipped,
        "changes": {
            "new": list(new_adds),
            "removed": list(removed),
        },
        "reddit_ads_targeting": [a["subreddit"] for a in activations],
    }

    # Save plan
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(plan, f, indent=2, default=str)

    # Save to history
    _save_history(plan)

    # ── Print results ───────────────────────────────────────────────────
    print(f"\n{'='*110}", file=sys.stderr)
    print(f"  Activation Plan — {plan['generated_at'][:19]} UTC — ${total_allocated:.2f}/day", file=sys.stderr)
    print(f"{'='*110}\n", file=sys.stderr)

    print(f"{'#':<3} {'Subreddit':<28} {'Subs':>10} {'Status':<8} {'Last Post':<12} "
          f"{'Posts/7d':>8} {'KW Hits':>8} {'Budget':>10}")
    print("─" * 110)
    for i, a in enumerate(activations, 1):
        subs = f"{a['subscribers']:,}" if a['subscribers'] else "?"
        icon = {"active": "🟢", "recent": "🟡", "slow": "🟠"}.get(a["status"], "⚪")
        print(f"{i:<3} r/{a['subreddit']:<26} {subs:>10} {icon:<8} "
              f"{a['last_post_date']:<12} {a['posts_7d']:>8} {a['keyword_hits']:>8} "
              f"${a['daily_budget_usd']:>8.2f}")

    print(f"\n    {'TOTAL':<86} ${total_allocated:>8.2f}")

    if new_adds:
        print(f"\n  📥 NEW: {', '.join(f'r/{s}' for s in new_adds)}")
    if removed:
        print(f"  📤 REMOVED: {', '.join(f'r/{s}' for s in removed)}")

    if skipped:
        print(f"\n  ⏭ Skipped ({len(skipped)}):")
        for s in skipped:
            print(f"    r/{s['subreddit']:<26} — {s['reason']}")

    print(f"\n  📋 Reddit Ads targeting list (copy-paste):")
    print(f"    {', '.join(a['subreddit'] for a in activations)}")
    print()

    if push_sheets:
        _push_to_sheets(plan, sheet_id)


def _save_history(plan: dict) -> None:
    """Append plan to history."""
    history = {"plans": []}
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
        except (json.JSONDecodeError, KeyError):
            pass

    history["plans"].append({
        "timestamp": plan["generated_at"],
        "subreddits": plan["reddit_ads_targeting"],
        "total_budget": plan["total_allocated_usd"],
        "count": plan["subreddits_to_activate"],
    })
    history["plans"] = history["plans"][-30:]  # Keep last 30

    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def _push_to_sheets(plan: dict, sheet_id: str) -> None:
    """Push activation plan to Google Sheets."""
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

    if not sheet_id:
        body = {
            "properties": {"title": f"Reddit Activation — {now} UTC"},
            "sheets": [{"properties": {"title": "Activation Plan"}}],
        }
        result = service.spreadsheets().create(body=body).execute()
        sheet_id = result["spreadsheetId"]
        tab_name = "Activation Plan"
        print(f"  Created: {result['spreadsheetUrl']}", file=sys.stderr)
    else:
        tab_name = f"Activate {now}"
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name, "index": 0}}}]},
            ).execute()
        except Exception as e:
            print(f"  [WARN] Could not add tab: {e}", file=sys.stderr)
            return

    # Build rows
    header = ["#", "Subreddit", "Subscribers", "Status", "Last Post Date",
              "Posts/7d", "KW Hits", "Daily Budget ($)", "Budget Share %",
              "Latest Conversation", "Post URL"]
    rows = [header]
    for i, a in enumerate(plan["activations"], 1):
        rows.append([
            i,
            f"r/{a['subreddit']}",
            a["subscribers"],
            a["status"].upper(),
            a["last_post_date"],
            a["posts_7d"],
            a["keyword_hits"],
            a["daily_budget_usd"],
            a["budget_share_pct"],
            a["last_post_title"],
            a["last_post_url"],
        ])
    # Total row
    rows.append([])
    rows.append(["", "TOTAL", "", "", "", "", "",
                 plan["total_allocated_usd"], "100%", "", ""])

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    # Format
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tab_id = [s["properties"]["sheetId"] for s in meta["sheets"]
              if s["properties"]["title"] == tab_name][0]

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
                          "startIndex": 0, "endIndex": 11},
        }},
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests},
    ).execute()

    print(f"  Pushed to https://docs.google.com/spreadsheets/d/{sheet_id}/edit", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Reddit Ad Activation — generate targeting plan.")
    parser.add_argument("--budget", type=float, default=50.0, help="Daily budget in USD (default: 50)")
    parser.add_argument("--sheets", action="store_true", help="Push to Google Sheets")
    parser.add_argument("--sheet-id", default="", help="Existing spreadsheet ID")
    args = parser.parse_args()
    run(budget=args.budget, push_sheets=args.sheets, sheet_id=args.sheet_id)


if __name__ == "__main__":
    main()
