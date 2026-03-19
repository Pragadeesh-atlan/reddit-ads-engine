#!/usr/bin/env python3
"""
scripts/reddit_daily.py
Reddit Ads Daily Brief

Runs the full daily workflow:
1. Research — poll all seed subreddits for latest activity
2. Compare — diff against yesterday's data
3. Recommend — which subreddits/keywords to ADD or REMOVE from campaigns
4. Output — Google Sheet with 2 tabs (Agent 1 Discovery + Agent 2 Latest Posts)
   plus an action summary at the top

Usage:
    # Daily run — output to terminal
    .venv/bin/python3 scripts/reddit_daily.py

    # Daily run — push to Google Sheets
    .venv/bin/python3 scripts/reddit_daily.py --sheets

    # Add to existing sheet
    .venv/bin/python3 scripts/reddit_daily.py --sheets --sheet-id YOUR_ID
"""
import argparse
import json
import math
import os
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/seed-subreddits.json")
YESTERDAY_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/yesterday.json")
TODAY_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/today.json")
HISTORY_DIR = os.path.join(BASE_DIR, "intelligence/reddit-ads/history")
BLOCKLIST_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/blocklist.json")

REDDIT = "https://www.reddit.com"
ATOM = "http://www.w3.org/2005/Atom"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36")

# Subreddit is targetable if last post within this many days
ACTIVE_DAYS = 3
# Subreddit should be removed if no posts in this many days
STALE_DAYS = 7

# Context keywords with weights for scoring
SIGNAL_TIERS = {
    "context engineering": 5, "context layer": 5, "context graph": 5,
    "enterprise context": 5, "ai context": 5, "context vacuum": 5,
    "semantic layer": 3, "ontology": 3, "knowledge graph": 3,
    "active metadata": 3, "data governance": 3, "data catalog": 3,
    "metadata management": 3, "data lineage": 3,
    "model context protocol": 3, "mcp server": 3,
    "ai hallucination": 2, "ai governance": 2, "data quality": 2,
    "context window": 2, "data mesh": 2,
    "ai agent": 1, "llm": 1, "rag": 1,
    "atlan": 4, "collibra": 4, "alation": 4, "informatica": 4,
    "unity catalog": 4, "purview": 4, "datahub": 4,
}

HIGH_TERMS = [
    "context engineering", "context layer", "context graph",
    "enterprise context", "ai context", "ontology", "semantic layer",
    "semantic web", "knowledge graph", "data governance", "data catalog",
    "metadata", "data lineage", "active metadata", "data mesh",
    "ai governance", "model context protocol", "mcp server",
]
MEDIUM_TERMS = [
    "data engineering", "data quality", "rag", "retrieval augmented",
    "llm", "ai agent", "business intelligence", "analytics engineering",
    "data pipeline", "mlops", "vector database",
]


# ── Discovery keywords — derived from 50+ Atlan pages on context layer,
# context engineering, semantic layer, ontology, knowledge graphs, AI governance.
# Each day we rotate through a subset to avoid rate limits.
# Full list covers everything from atlan.com/context-layer/, /know/*, /great-data-debate-2026/*
DISCOVERY_KEYWORDS = [
    # Exact category language from Atlan pages
    "context engineering",
    "context layer",
    "context graph",
    "enterprise context layer",
    "AI context stack",
    "context for AI agents",
    "context vacuum data",
    "context preparation",
    "context maturity",
    # Semantic & ontology from Atlan pages
    "semantic layer AI",
    "ontology AI architecture",
    "ontology vs semantic layer",
    "context graph vs knowledge graph",
    "semantic layer failed",
    "ontology first AI",
    # Data governance + AI from Atlan pages
    "AI governance data",
    "data governance AI agents",
    "context engineering governance",
    "AI readiness data",
    "AI production readiness",
    # Problems Atlan solves
    "AI hallucination enterprise",
    "LLM context window limitations",
    "RAG data governance",
    "AI agent metadata",
    "active metadata platform",
    # MCP & infrastructure
    "model context protocol",
    "MCP server AI",
    "metadata lakehouse",
    # Competitor adjacent
    "data catalog AI agents",
    "data lineage AI",
    "knowledge graph enterprise AI",
]

# Known irrelevant subreddits — never suggest these
BLOCKLIST = {
    "conspiracy", "BPD", "HindutvaRises", "GIRLSundPANZER", "DiscoElysium",
    "hingeapp", "PurpleCoco", "tampa", "StarshipPorn", "evilbuildings",
    "CharacterAI", "PygmalionAI", "fetishcai", "aiwars", "ethtrader",
    "Hedera", "JasmyToken", "Iota", "NEO", "oasisnetwork", "ONT", "ONTtrader",
    "OntologyNetwork",  # crypto, not data ontology
    "ChatGPT", "OpenAI", "singularity", "ArtificialIntelligence",
    "MachineLearning", "learnmachinelearning", "deeplearning", "MLQuestions",
    "vibecoding", "VibeCodeDevs", "buildinpublic", "ChatGPTCoding",
    "ChatGPTPromptGenius", "PromptEngineering", "AiGeminiPhotoPrompts",
    "blender", "rust", "godot", "fabricmc", "Unity3D", "ruby", "docker",
    "webdev", "programming", "engineering", "AskEngineers",
    "Maplestory", "resumes", "branding", "agi", "Banksy", "artcollecting",
    "productivity", "DataHoarder", "Metaphysics", "networking",
    "GeminiAI", "GoogleGeminiAI", "ManusOfficial", "ClaudeAI", "windsurf",
    "Taskade", "codex", "AngelInvesting", "polymarket_bets",
    "HOPR", "FetchAI_Community", "spaceandtimecrypto", "aelfofficial",
    "metalworking", "RPChristians",
}


def _search_subreddits(query: str, limit: int = 10) -> list[dict]:
    """Method 1: Search for subreddits by name/description."""
    params = urllib.parse.urlencode({"q": query, "limit": limit, "sort": "relevance"})
    url = f"{REDDIT}/subreddits/search.json?{params}"
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "12", "-A", UA, url],
            capture_output=True, timeout=15)
        if not result.stdout:
            return []
        data = json.loads(result.stdout.decode("utf-8"))
        results = []
        for child in data.get("data", {}).get("children", []):
            d = child.get("data", {})
            name = d.get("display_name", "")
            if name and not d.get("over18", False):
                results.append({
                    "name": name,
                    "subscribers": d.get("subscribers") or 0,
                    "description": (d.get("public_description") or "")[:200],
                })
        return results
    except Exception:
        return []


def discover_new_subreddits(known: set[str]) -> list[dict]:
    """
    Discovery via subreddit name + description matching only.
    Searches Reddit for subreddits whose name or description contains
    our context/AI/data keywords. No global post search (too noisy).

    Runs ALL keywords every day.
    """
    extra_blocked = set()
    if os.path.exists(BLOCKLIST_FILE):
        try:
            with open(BLOCKLIST_FILE) as f:
                extra_blocked = set(json.load(f).get("blocked", []))
        except Exception:
            pass
    blocked = BLOCKLIST | extra_blocked | known

    print(f"\n  {'='*60}", file=sys.stderr)
    print(f"  🔍 Discovery — searching for new subreddits", file=sys.stderr)
    print(f"     Matching by subreddit name + description", file=sys.stderr)
    print(f"     Keywords: {len(DISCOVERY_KEYWORDS)}", file=sys.stderr)
    print(f"  {'='*60}\n", file=sys.stderr)

    found: dict[str, dict] = {}

    for i, kw in enumerate(DISCOVERY_KEYWORDS, 1):
        print(f"  [{i}/{len(DISCOVERY_KEYWORDS)}] \"{kw}\"", end="", file=sys.stderr)
        results = _search_subreddits(kw, limit=10)
        new = 0
        for sub in results:
            name = sub["name"]
            if name in blocked or name in found:
                continue
            # Must match a HIGH term in name or description
            # (MEDIUM terms like "ai agent" are too broad for discovery)
            txt = (sub.get("description", "") + " " + name).lower()
            high_matches = [t for t in HIGH_TERMS if t in txt]
            if high_matches and sub.get("subscribers", 0) >= 10:
                sub["_score"] = len(high_matches) * 15
                sub["_matched"] = high_matches[:3]
                found[name] = sub
                new += 1
        print(f" → {new} new", file=sys.stderr)
        time.sleep(2)

    qualified = sorted(found.values(), key=lambda x: (-x.get("_score", 0), -(x.get("subscribers", 0))))

    print(f"\n  Discovery results:", file=sys.stderr)
    print(f"     Subreddits scanned: {len(found) + len(blocked)}", file=sys.stderr)
    print(f"     New + qualified: {len(qualified)}", file=sys.stderr)
    if qualified:
        for s in qualified[:10]:
            matched = ", ".join(s.get("_matched", []))
            print(f"       r/{s['name']:<25} {s.get('subscribers', 0):>8,} subs  matched: [{matched}]", file=sys.stderr)

    return qualified


def fetch_subreddit(name: str) -> dict:
    """Fetch subreddit info + latest posts from RSS."""
    # Get subscriber count + description
    subs = 0; desc = ""; title = ""
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10", "-A", UA,
             f"{REDDIT}/r/{name}/about.json"],
            capture_output=True, timeout=15)
        if result.stdout:
            d = json.loads(result.stdout.decode("utf-8")).get("data", {})
            subs = d.get("subscribers") or 0
            desc = (d.get("public_description") or "")[:300]
            title = d.get("title") or ""
    except Exception:
        pass
    time.sleep(1)

    # Get RSS feed
    posts = []
    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "10", "-A", UA,
             f"{REDDIT}/r/{name}/new/.rss"],
            capture_output=True, timeout=15)
        if result.stdout:
            root = ET.fromstring(result.stdout)
            for entry in root.findall(f"{{{ATOM}}}entry"):
                t_el = entry.find(f"{{{ATOM}}}title")
                t = (t_el.text or "").strip() if t_el is not None else ""
                p_el = entry.find(f"{{{ATOM}}}published")
                p = p_el.text.strip()[:10] if p_el is not None and p_el.text else ""
                url = ""
                for link in entry.findall(f"{{{ATOM}}}link"):
                    if link.get("rel", "alternate") == "alternate":
                        url = link.get("href", "")
                        break
                posts.append({"title": t, "date": p, "url": url})
    except Exception:
        pass
    time.sleep(1)

    # Latest post info
    last_title = posts[0]["title"] if posts else "(no posts)"
    last_date = posts[0]["date"] if posts else ""
    last_url = posts[0]["url"] if posts else ""
    days_ago = None
    if last_date:
        try:
            dt = datetime.strptime(last_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_ago = (datetime.now(timezone.utc) - dt).days
        except ValueError:
            pass

    # Keyword analysis across all posts
    kw_hits = 0
    weighted_hits = 0
    top_kw_posts = []
    for post in posts[:25]:
        text = post["title"].lower()
        matches = []
        weight = 0
        for kw, w in SIGNAL_TIERS.items():
            if kw in text:
                matches.append(kw)
                weight += w
        if matches:
            kw_hits += len(matches)
            weighted_hits += weight
            top_kw_posts.append({
                "title": post["title"][:60],
                "keywords": matches[:3],
                "date": post["date"],
                "url": post["url"],
            })

    # Relevance score
    txt = (desc + " " + title + " " + name).lower()
    high = [t for t in HIGH_TERMS if t in txt]
    med = [t for t in MEDIUM_TERMS if t in txt]
    rel_score = min(len(high) * 15, 45) + min(len(med) * 8, 25)
    if subs > 0:
        rel_score += min(math.log10(max(subs, 1)) * 5, 20)
    tier = "Tier 1" if rel_score >= 50 else ("Tier 2" if rel_score >= 30 else "Tier 3")

    return {
        "subreddit": name,
        "subscribers": subs,
        "description": desc[:200],
        "relevance_score": round(rel_score, 1),
        "tier": tier,
        "last_post_date": last_date,
        "days_ago": days_ago,
        "last_post_title": last_title,
        "last_post_url": last_url,
        "posts_in_feed": len(posts),
        "keyword_hits": kw_hits,
        "weighted_keyword_hits": weighted_hits,
        "top_keyword_posts": top_kw_posts[:5],
        "rss_url": f"{REDDIT}/r/{name}/new/.rss",
    }


def run(push_sheets: bool = False, sheet_id: str = "") -> None:
    # Load seeds
    with open(SEED_FILE) as f:
        seeds = json.load(f)["subreddits"]

    # Load yesterday's data
    yesterday = {}
    if os.path.exists(YESTERDAY_FILE):
        with open(YESTERDAY_FILE) as f:
            yd = json.load(f)
        for s in yd.get("subreddits", []):
            yesterday[s["subreddit"]] = s

    print(f"\n{'='*70}", file=sys.stderr)
    print(f"  Reddit Ads Daily Brief — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC", file=sys.stderr)
    print(f"  Researching {len(seeds)} subreddits...", file=sys.stderr)
    print(f"{'='*70}\n", file=sys.stderr)

    # Research all subreddits
    today = []
    for i, name in enumerate(seeds, 1):
        print(f"  [{i}/{len(seeds)}] r/{name}...", end="", file=sys.stderr)
        data = fetch_subreddit(name)
        today.append(data)
        d = data["days_ago"]
        icon = "🟢" if d is not None and d <= ACTIVE_DAYS else ("🟡" if d is not None and d <= STALE_DAYS else "🔴")
        print(f" {icon} {data['last_post_date']} ({d or '?'}d) kw={data['keyword_hits']}", file=sys.stderr)

    # ── Discovery — search for new subreddits ─────────────────────────
    known_names = {s["subreddit"] for s in today}
    discoveries = discover_new_subreddits(known_names)

    # Fetch full data for ALL discoveries — track them even if not active yet
    # Agent 3 will only activate the active ones, but we want to catch them
    # the moment they become active
    new_subreddits = []
    for disc in discoveries:
        print(f"  [NEW] r/{disc['name']}...", end="", file=sys.stderr)
        data = fetch_subreddit(disc["name"])
        data["_is_new_discovery"] = True
        new_subreddits.append(data)
        d = data["days_ago"]
        icon = "🟢" if d is not None and d <= ACTIVE_DAYS else "🔴"
        print(f" {icon} ({d or '?'}d ago) kw={data['keyword_hits']}", file=sys.stderr)

    # Auto-add active discoveries to seed file for future runs
    if new_subreddits:
        active_new = [s for s in new_subreddits
                      if s.get("days_ago") is not None and s["days_ago"] <= STALE_DAYS]
        if active_new:
            with open(SEED_FILE) as f:
                seed_data = json.load(f)
            existing = set(seed_data["subreddits"])
            added = []
            for s in active_new:
                if s["subreddit"] not in existing:
                    seed_data["subreddits"].append(s["subreddit"])
                    added.append(s["subreddit"])
            if added:
                seed_data["last_audited"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                with open(SEED_FILE, "w") as f:
                    json.dump(seed_data, f, indent=2)
                print(f"\n  📥 Auto-added {len(added)} new subreddits to seeds: {', '.join(added)}",
                      file=sys.stderr)

    # ── Compare with yesterday → generate actions ────────────────────────
    actions = []
    yesterday_active = {s["subreddit"] for s in yesterday.values()
                        if s.get("days_ago") is not None and s["days_ago"] <= ACTIVE_DAYS}
    today_active = {s["subreddit"] for s in today
                    if s.get("days_ago") is not None and s["days_ago"] <= ACTIVE_DAYS}
    today_stale = {s["subreddit"] for s in today
                   if s.get("days_ago") is None or s["days_ago"] > STALE_DAYS}

    # New activations — active today but wasn't yesterday
    for name in sorted(today_active - yesterday_active):
        s = next(x for x in today if x["subreddit"] == name)
        actions.append({
            "action": "ADD",
            "subreddit": name,
            "reason": f"Became active — last post {s['days_ago']}d ago" +
                      (f", {s['keyword_hits']} context keywords" if s["keyword_hits"] else ""),
            "subscribers": s["subscribers"],
            "last_post": s["last_post_title"][:60],
            "last_post_date": s["last_post_date"],
            "keyword_hits": s["keyword_hits"],
        })

    # Removals — was active yesterday but now stale
    for name in sorted(yesterday_active & today_stale):
        s = next(x for x in today if x["subreddit"] == name)
        actions.append({
            "action": "REMOVE",
            "subreddit": name,
            "reason": f"Went stale — no posts in {s['days_ago'] or '?'} days",
            "subscribers": s["subscribers"],
            "last_post": s["last_post_title"][:60],
            "last_post_date": s["last_post_date"],
            "keyword_hits": s["keyword_hits"],
        })

    # Keep — active both days (no action needed, but show for awareness)
    for name in sorted(today_active & yesterday_active):
        s = next(x for x in today if x["subreddit"] == name)
        actions.append({
            "action": "KEEP",
            "subreddit": name,
            "reason": "Still active",
            "subscribers": s["subscribers"],
            "last_post": s["last_post_title"][:60],
            "last_post_date": s["last_post_date"],
            "keyword_hits": s["keyword_hits"],
        })

    # New discoveries — not in seeds, but active and relevant
    for s in new_subreddits:
        actions.append({
            "action": "NEW",
            "subreddit": s["subreddit"],
            "reason": f"New discovery — {s['subscribers']:,} subs, "
                      f"last post {s['days_ago']}d ago, {s['keyword_hits']} context keywords",
            "subscribers": s["subscribers"],
            "last_post": s["last_post_title"][:60],
            "last_post_date": s["last_post_date"],
            "keyword_hits": s["keyword_hits"],
        })
        # Also add to today's data so it shows in the sheet
        today.append(s)

    # ── Save today's data as "yesterday" for tomorrow ────────────────────
    os.makedirs(os.path.dirname(YESTERDAY_FILE), exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)

    today_data = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "subreddits": today,
    }
    with open(YESTERDAY_FILE, "w") as f:
        json.dump(today_data, f, indent=2)
    with open(TODAY_FILE, "w") as f:
        json.dump(today_data, f, indent=2)

    # Save dated history
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(os.path.join(HISTORY_DIR, f"{date_str}.json"), "w") as f:
        json.dump(today_data, f, indent=2)

    # ── Print results ────────────────────────────────────────────────────
    adds = [a for a in actions if a["action"] == "ADD"]
    removes = [a for a in actions if a["action"] == "REMOVE"]
    keeps = [a for a in actions if a["action"] == "KEEP"]
    news = [a for a in actions if a["action"] == "NEW"]

    print(f"\n{'='*70}", file=sys.stderr)
    print(f"  DAILY ACTIONS", file=sys.stderr)
    print(f"{'='*70}", file=sys.stderr)

    if news:
        print(f"\n  🆕 NEW DISCOVERIES — consider adding to campaign ({len(news)}):", file=sys.stderr)
        for a in news:
            print(f"     r/{a['subreddit']:<25} — {a['reason']}", file=sys.stderr)
            print(f"       Latest: {a['last_post']}", file=sys.stderr)

    if adds:
        print(f"\n  🟢 ADD to campaign ({len(adds)}):", file=sys.stderr)
        for a in adds:
            print(f"     r/{a['subreddit']:<25} — {a['reason']}", file=sys.stderr)

    if removes:
        print(f"\n  🔴 REMOVE from campaign ({len(removes)}):", file=sys.stderr)
        for a in removes:
            print(f"     r/{a['subreddit']:<25} — {a['reason']}", file=sys.stderr)

    if keeps:
        print(f"\n  ⚪ KEEP in campaign ({len(keeps)}):", file=sys.stderr)
        for a in keeps:
            print(f"     r/{a['subreddit']:<25} kw={a['keyword_hits']}", file=sys.stderr)

    if not adds and not removes and not news:
        print(f"\n  No changes from yesterday.", file=sys.stderr)

    # Active subreddit list for copy-paste into Reddit Ads Manager
    active_list = sorted(today_active)
    print(f"\n  📋 Active subreddits for targeting ({len(active_list)}):", file=sys.stderr)
    print(f"     {', '.join(active_list)}", file=sys.stderr)

    # Suggested keywords from top keyword posts
    all_kw = {}
    for s in today:
        for p in s.get("top_keyword_posts", []):
            for kw in p.get("keywords", []):
                all_kw[kw] = all_kw.get(kw, 0) + SIGNAL_TIERS.get(kw, 1)
    top_keywords = sorted(all_kw.items(), key=lambda x: x[1], reverse=True)[:15]
    if top_keywords:
        print(f"\n  🔑 Suggested keywords for targeting:", file=sys.stderr)
        print(f"     {', '.join(kw for kw, _ in top_keywords)}", file=sys.stderr)

    print(f"\n{'='*70}\n", file=sys.stderr)

    # Print full table
    print(f"\n{'#':<3} {'Subreddit':<28} {'Subs':>10} {'Score':>6} {'Tier':<7} "
          f"{'Last Post':<12} {'Days':>5} {'KW':>4}  Latest Conversation")
    print("─" * 120)
    for i, s in enumerate(sorted(today, key=lambda x: (x["days_ago"] or 9999, -(x["keyword_hits"] or 0))), 1):
        d = s["days_ago"]
        icon = "🟢" if d is not None and d <= ACTIVE_DAYS else ("🟡" if d is not None and d <= STALE_DAYS else "🔴")
        days = str(d) if d is not None else "?"
        print(f"{i:<3} r/{s['subreddit']:<26} {s['subscribers']:>10,} {s['relevance_score']:>6.0f} "
              f"{s['tier']:<7} {s['last_post_date']:<12} {days:>5} {s['keyword_hits']:>4}  "
              f"{s['last_post_title'][:50]}")

    # Push to sheets first so we can include the link in Slack
    sheet_url = ""
    if push_sheets:
        sheet_url = _push_to_sheets(today, actions, active_list, top_keywords, sheet_id)

    # Send Slack notification (with sheet link if available)
    _send_slack(actions, active_list, top_keywords, new_subreddits, sheet_url=sheet_url)


def _load_env():
    """Load .env file from repo root."""
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


def _send_slack(actions, active_list, top_keywords, new_subreddits, sheet_url=""):
    """Send daily brief to Slack."""
    _load_env()
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "")
    if not token or not channel:
        print("  [SKIP] Slack — no SLACK_BOT_TOKEN or SLACK_CHANNEL in .env", file=sys.stderr)
        return

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    adds = [a for a in actions if a["action"] == "ADD"]
    removes = [a for a in actions if a["action"] == "REMOVE"]
    keeps = [a for a in actions if a["action"] == "KEEP"]
    news = [a for a in actions if a["action"] == "NEW"]

    # Build Slack message blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Reddit Ads Daily Brief — {date}"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text":
                f"*{len(active_list)}* active subreddits | "
                f"*{len(adds)}* to add | *{len(removes)}* to remove | "
                f"*{len(news)}* new discoveries"}
        },
    ]

    # New discoveries
    if news:
        news_text = "*:new: New Discoveries — consider adding:*\n"
        for a in news:
            news_text += f"• `r/{a['subreddit']}` — {a['subscribers']:,} subs, {a['keyword_hits']} context keywords\n"
            news_text += f"  _{a['last_post']}_\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": news_text}})

    # Adds
    if adds:
        add_text = "*:large_green_circle: ADD to campaign:*\n"
        for a in adds:
            add_text += f"• `r/{a['subreddit']}` — {a['reason']}\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": add_text}})

    # Removes
    if removes:
        rm_text = "*:red_circle: REMOVE from campaign:*\n"
        for a in removes:
            rm_text += f"• `r/{a['subreddit']}` — {a['reason']}\n"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": rm_text}})

    # Active list
    if active_list:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text":
                f"*:clipboard: Active subreddits for targeting ({len(active_list)}):*\n"
                f"```{', '.join(active_list)}```"}
        })

    # Top keywords
    if top_keywords:
        kw_text = ", ".join(kw for kw, _ in top_keywords[:10])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:key: Suggested keywords:*\n`{kw_text}`"}
        })

    # No changes
    if not adds and not removes and not news:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No changes from yesterday. All subreddits same status._"}
        })

    # Google Sheet link
    if sheet_url:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":bar_chart: *<{sheet_url}|Open Google Sheet>*"}
        })

    # Send via curl
    payload = json.dumps({"channel": channel, "blocks": blocks})
    try:
        result = subprocess.run(
            ["curl", "-sL", "-X", "POST", "https://slack.com/api/chat.postMessage",
             "-H", f"Authorization: Bearer {token}",
             "-H", "Content-Type: application/json",
             "-d", payload],
            capture_output=True, timeout=15)
        resp = json.loads(result.stdout.decode("utf-8"))
        if resp.get("ok"):
            print(f"  Slack notification sent to {channel}", file=sys.stderr)
        else:
            print(f"  [WARN] Slack error: {resp.get('error', 'unknown')}", file=sys.stderr)
    except Exception as e:
        print(f"  [WARN] Slack failed: {e}", file=sys.stderr)


def _send_slack_sheet_link(sheet_url):
    """Send the Google Sheet link as a follow-up message."""
    _load_env()
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL", "")
    if not token or not channel or not sheet_url:
        return

    payload = json.dumps({
        "channel": channel,
        "text": f":bar_chart: Google Sheet ready: {sheet_url}",
    })
    try:
        subprocess.run(
            ["curl", "-sL", "-X", "POST", "https://slack.com/api/chat.postMessage",
             "-H", f"Authorization: Bearer {token}",
             "-H", "Content-Type: application/json",
             "-d", payload],
            capture_output=True, timeout=15)
    except Exception:
        pass


def _push_to_sheets(today, actions, active_list, top_keywords, sheet_id):
    """Create Google Sheet with Agent 1 + Agent 2 tabs."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        import google.auth.transport.requests
    except ImportError:
        print("  [ERROR] google-api-python-client not installed", file=sys.stderr)
        return ""

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

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not sheet_id:
        body = {
            "properties": {"title": f"Reddit Ads Daily — {date}"},
            "sheets": [
                {"properties": {"title": "Discovery (Agent 1)", "index": 0}},
                {"properties": {"title": "Latest Posts (Agent 2)", "index": 1}},
            ],
        }
        result = service.spreadsheets().create(body=body).execute()
        sheet_id = result["spreadsheetId"]
        print(f"  Created: {result['spreadsheetUrl']}", file=sys.stderr)
    else:
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": [
                    {"addSheet": {"properties": {"title": f"Discovery {date}", "index": 0}}},
                    {"addSheet": {"properties": {"title": f"Latest Posts {date}", "index": 1}}},
                ]},
            ).execute()
        except Exception as e:
            print(f"  [WARN] {e}", file=sys.stderr)

    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = meta["sheets"]
    disc_tab = sheets[0]["properties"]["title"]
    disc_id = sheets[0]["properties"]["sheetId"]
    posts_tab = sheets[1]["properties"]["title"]
    posts_id = sheets[1]["properties"]["sheetId"]

    # ── Tab 1: Discovery (Agent 1) ──────────────────────────────────────
    disc_header = ["Rank", "Subreddit", "Subscribers", "Relevance Score",
                   "Tier", "Description", "RSS URL", "Score Breakdown"]
    disc_rows = [disc_header]
    for i, s in enumerate(sorted(today, key=lambda x: -x["relevance_score"]), 1):
        disc_rows.append([
            i, f"r/{s['subreddit']}", s["subscribers"], s["relevance_score"],
            s["tier"], s["description"], s["rss_url"], "",
        ])

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{disc_tab}'!A1",
        valueInputOption="RAW", body={"values": disc_rows},
    ).execute()

    # ── Tab 2: Latest Posts (Agent 2) ───────────────────────────────────
    posts_header = ["#", "Subreddit", "Subscribers", "Last Post Date",
                    "Days Ago", "Latest Conversation", "Post URL"]
    posts_rows = [posts_header]
    for i, s in enumerate(sorted(today, key=lambda x: (x["days_ago"] or 9999)), 1):
        posts_rows.append([
            i, f"r/{s['subreddit']}", s["subscribers"],
            s["last_post_date"], s["days_ago"] if s["days_ago"] is not None else "",
            s["last_post_title"], s["last_post_url"],
        ])

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{posts_tab}'!A1",
        valueInputOption="RAW", body={"values": posts_rows},
    ).execute()

    # ── Formatting ──────────────────────────────────────────────────────
    requests = []
    for tab_id, num_cols in [(disc_id, 8), (posts_id, 7)]:
        requests.extend([
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
                              "startIndex": 0, "endIndex": num_cols},
            }},
        ])

    # Color code Latest Posts rows
    for row_idx, row in enumerate(posts_rows[1:], 1):
        d = row[4]
        if isinstance(d, int):
            if d <= 1:
                color = {"red": 0.85, "green": 0.95, "blue": 0.85}
            elif d <= 3:
                color = {"red": 0.9, "green": 0.95, "blue": 0.85}
            elif d <= 7:
                color = {"red": 1.0, "green": 0.95, "blue": 0.8}
            else:
                color = {"red": 0.95, "green": 0.85, "blue": 0.85}
            requests.append({"repeatCell": {
                "range": {"sheetId": posts_id, "startRowIndex": row_idx,
                         "endRowIndex": row_idx + 1},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {**color, "alpha": 1.0},
                }},
                "fields": "userEnteredFormat(backgroundColor)",
            }})

    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body={"requests": requests},
    ).execute()

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    print(f"  Sheet: {sheet_url}", file=sys.stderr)
    return sheet_url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Ads Daily Brief")
    parser.add_argument("--sheets", action="store_true")
    parser.add_argument("--sheet-id", default="")
    args = parser.parse_args()
    run(push_sheets=args.sheets, sheet_id=args.sheet_id)
