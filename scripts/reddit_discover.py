#!/usr/bin/env python3
"""
scripts/reddit_discover.py
Skill: Reddit Subreddit Discovery (Agent 1)

Maintains and grows a curated list of subreddits relevant to Atlan's
context/AI/data narrative. Runs daily as part of the Reddit Ads pipeline.

Two modes:
  1. Refresh — Update info for all seed subreddits (fast, ~2 min)
  2. Discover — Search for NEW subreddits + refresh seeds (slower, ~8 min)

Seed subreddits: intelligence/reddit-ads/seed-subreddits.json
  - Manually curated, audited list of relevant subreddits
  - New discoveries are flagged separately for human review
  - Only added to seeds after explicit approval

Output: intelligence/reddit-ads/subreddits.json (consumed by Agent 2)

Usage:
    # Daily run — refresh seed subreddits only (fast)
    .venv/bin/python3 scripts/reddit_discover.py

    # Weekly run — also search for new subreddits
    .venv/bin/python3 scripts/reddit_discover.py --discover

    # Push results to Google Sheets
    .venv/bin/python3 scripts/reddit_discover.py --discover --sheets

No API key required. Uses Reddit's public JSON endpoints via curl.
"""
import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timezone

REDDIT_BASE = "https://www.reddit.com"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── Paths ────────────────────────────────────────────────────────────────────

SEED_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/seed-subreddits.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/subreddits.json")
NEW_DISCOVERIES_FILE = os.path.join(BASE_DIR, "intelligence/reddit-ads/new-discoveries.json")

# ── Discovery keywords — Atlan's context narrative ──────────────────────────

DISCOVERY_KEYWORDS = [
    # Atlan's exact category language
    "context engineering",
    "context layer",
    "context graph",
    "AI context",
    "enterprise context",
    # Adjacent positioning
    "semantic layer",
    "ontology AI",
    "ontology data",
    "knowledge graph AI",
    "data governance AI",
    "AI governance",
    "metadata management AI",
    "data catalog AI",
    # Problems Atlan solves
    "AI agent context",
    "LLM context window",
    "RAG data governance",
    "AI hallucination enterprise",
    "data lineage AI",
    # Technical overlap
    "model context protocol",
    "active metadata",
    "semantic web",
    "data mesh",
]

# ── Relevance scoring ───────────────────────────────────────────────────────

HIGH_TERMS = [
    "context engineering", "context layer", "context graph",
    "enterprise context", "ai context", "context for ai",
    "ontology", "semantic layer", "semantic web", "knowledge graph",
    "data governance", "data catalog", "metadata", "data lineage",
    "active metadata", "data mesh", "data fabric",
    "ai governance", "ai hallucination",
    "model context protocol", "mcp server",
]

MEDIUM_TERMS = [
    "data engineering", "data quality", "data observability",
    "rag", "retrieval augmented", "llm", "ai agent",
    "business intelligence", "analytics engineering",
    "data warehouse", "data pipeline",
    "mlops", "vector database",
]

# Known false positives — never include these
BLOCKLIST = {
    "conspiracy", "BPD", "PredictionsMarkets", "SMCIDiscussion",
    "tinyMediaManager", "AnalyticsMemes", "aigamedev", "SocialEngineering",
    "AISEOInsider", "BlackboxAI_", "IndicKnowledgeSystems",
    "ArtificialNtelligence", "Maplestory", "resumes", "branding",
    "forhire2", "onlineservicesPH", "FundingTradersCare",
    "DataScienceJobs", "SoftwareEngineerJobs", "dataengineeringjobs",
    "MachineLearningJobs", "ChatGPT", "OpenAI", "singularity",
    "ArtificialIntelligence", "learnmachinelearning", "deeplearning",
    "MLQuestions", "MachineLearning", "artificial", "StableDiffusion",
    "HindutvaRises", "GIRLSundPANZER", "DiscoElysium", "hingeapp",
    "PurpleCoco", "tampa", "StarshipPorn", "evilbuildings",
    "CharacterAI", "PygmalionAI", "fetishcai", "aiwars",
    "ethtrader", "Hedera", "JasmyToken", "Iota", "NEO",
    "oasisnetwork", "spaceandtimeDB", "aelfofficial", "Grass_io",
    "ONT", "ONTtrader", "OntologyNetwork",  # These are crypto, not data ontology
    "polymarket_bets", "Polymarket_news", "AngelInvesting",
    "GeminiAI", "GoogleGeminiAI", "GoogleGemini", "GeminiCLI",
    "vibecoding", "VibeCodeDevs", "AskVibecoders", "buildinpublic",
    "pwnhub", "AskNetsec", "ChatGPTCoding", "ChatGPTPromptGenius",
    "PromptEngineering", "AiGeminiPhotoPrompts",
    "blender", "rust", "godot", "fabricmc", "Unity3D", "ruby",
    "Clojure", "docker", "webdev", "web_design", "programming",
    "engineering", "AskEngineers", "MechanicalEngineering",
    "EngineeringManagers", "SplitDepthGIFS", "tnvisa",
    "productivity", "DataHoarder", "Metaphysics", "networking",
    "cooperatives", "darknetplan", "whereisthis", "Banksy",
    "artcollecting", "AnomalousEvidence", "agi",
    "ManusOfficial", "ManusAiAgent", "ClaudeAI", "windsurf",
    "Taskade", "aipromptprogramming", "codex",
    "takeexamsupport", "Agent_SEO", "heracareerswitch",
    "AiForSmallBusiness", "AIJobs",
}


# ── HTTP helpers ─────────────────────────────────────────────────────────────


def http_get_json(url: str, retries: int = 3) -> dict | None:
    """Fetch URL as JSON via curl with rate-limit handling."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", "15", "-A", _UA,
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=20,
            )
            if result.returncode != 0:
                if attempt < retries:
                    time.sleep(3 * (attempt + 1))
                    continue
                return None
            output = result.stdout.decode("utf-8")
            parts = output.rsplit("\n", 1)
            body = parts[0] if len(parts) > 1 else output
            status = parts[1].strip() if len(parts) > 1 else "200"
            if status == "429":
                wait = 10 * (attempt + 1)
                print(f"  [429] Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue
            if not body.strip():
                return None
            return json.loads(body)
        except (subprocess.TimeoutExpired, json.JSONDecodeError):
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception:
            return None
    return None


# ── Subreddit info ───────────────────────────────────────────────────────────


def fetch_subreddit_info(name: str) -> dict | None:
    """Fetch subreddit metadata from Reddit."""
    url = f"{REDDIT_BASE}/r/{name}/about.json"
    data = http_get_json(url)
    if not data or "data" not in data:
        return None
    d = data["data"]
    return {
        "name": d.get("display_name", name),
        "title": d.get("title", ""),
        "description": (d.get("public_description") or "")[:300],
        "subscribers": d.get("subscribers") or 0,
        "active_users": d.get("accounts_active") or 0,
        "over18": d.get("over18", False),
        "url": f"https://www.reddit.com/r/{d.get('display_name', name)}/",
        "rss_url": f"https://www.reddit.com/r/{d.get('display_name', name)}/new/.rss",
    }


def score_subreddit(sub: dict) -> dict:
    """Score a subreddit for relevance to Atlan's context narrative."""
    name = sub.get("name", "").lower()
    desc = (sub.get("description", "") + " " + sub.get("title", "")).lower()
    subscribers = sub.get("subscribers") or 0

    if sub.get("over18") or sub.get("name", "") in BLOCKLIST:
        sub["relevance_score"] = 0
        return sub

    score = 0.0
    breakdown = []

    # High relevance terms
    high = [t for t in HIGH_TERMS if t in desc or t in name]
    if high:
        score += min(len(high) * 15, 45)
        breakdown.append(f"high: {', '.join(high[:4])}")

    # Medium relevance terms
    med = [t for t in MEDIUM_TERMS if t in desc or t in name]
    if med:
        score += min(len(med) * 8, 25)
        breakdown.append(f"medium: {', '.join(med[:4])}")

    # Subscriber score (log scale)
    if subscribers > 0:
        sub_score = min(math.log10(max(subscribers, 1)) * 5, 20)
        score += sub_score
        breakdown.append(f"subs: {subscribers:,}")

    sub["relevance_score"] = round(score, 1)
    sub["score_breakdown"] = breakdown
    return sub


# ── Discovery: find new subreddits ──────────────────────────────────────────


def search_subreddits(query: str, limit: int = 15) -> list[dict]:
    """Search Reddit for subreddits matching a query."""
    params = urllib.parse.urlencode({"q": query, "limit": limit, "sort": "relevance"})
    url = f"{REDDIT_BASE}/subreddits/search.json?{params}"
    data = http_get_json(url)
    if not data or "data" not in data:
        return []
    results = []
    for child in data["data"].get("children", []):
        d = child.get("data", {})
        results.append({
            "name": d.get("display_name", ""),
            "title": d.get("title", ""),
            "description": (d.get("public_description") or "")[:300],
            "subscribers": d.get("subscribers") or 0,
            "active_users": d.get("accounts_active") or 0,
            "over18": d.get("over18", False),
            "url": f"https://www.reddit.com/r/{d.get('display_name', '')}/",
            "rss_url": f"https://www.reddit.com/r/{d.get('display_name', '')}/new/.rss",
        })
    return results


def discover_new(known_names: set[str]) -> list[dict]:
    """Search for new subreddits not in the known set."""
    new_found: dict[str, dict] = {}

    print(f"\n  Searching for new subreddits ({len(DISCOVERY_KEYWORDS)} keywords)...",
          file=sys.stderr)

    for i, kw in enumerate(DISCOVERY_KEYWORDS, 1):
        print(f"  [{i}/{len(DISCOVERY_KEYWORDS)}] \"{kw}\"", file=sys.stderr)
        results = search_subreddits(kw, limit=10)
        for sub in results:
            name = sub["name"]
            if name not in known_names and name not in BLOCKLIST and name not in new_found:
                scored = score_subreddit(sub)
                if scored["relevance_score"] >= 20 and sub["subscribers"] >= 10:
                    new_found[name] = scored
        time.sleep(3)

    new_list = sorted(new_found.values(), key=lambda s: s["relevance_score"], reverse=True)
    print(f"  Found {len(new_list)} new candidates", file=sys.stderr)
    return new_list


# ── Main pipeline ────────────────────────────────────────────────────────────


def run(discover_mode: bool = False, push_sheets: bool = False) -> None:
    """
    Main discovery pipeline:
    1. Load seed subreddits
    2. Fetch fresh info for each seed
    3. (Optional) Search for new subreddits
    4. Score and output
    """
    # Load seeds
    if not os.path.exists(SEED_FILE):
        print(f"  [ERROR] Seed file not found: {SEED_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(SEED_FILE) as f:
        seed_data = json.load(f)
    seed_names = seed_data["subreddits"]

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Reddit Discover — {'Full Discovery' if discover_mode else 'Seed Refresh'}", file=sys.stderr)
    print(f"  Seeds: {len(seed_names)} subreddits", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # Fetch fresh info for all seeds
    all_subs: list[dict] = []
    seen: set[str] = set()

    for i, name in enumerate(seed_names, 1):
        if name in seen:
            continue
        seen.add(name)
        print(f"  [{i}/{len(seed_names)}] r/{name}...", file=sys.stderr)
        info = fetch_subreddit_info(name)
        if info:
            scored = score_subreddit(info)
            all_subs.append(scored)
        else:
            print(f"    [WARN] Could not fetch r/{name}", file=sys.stderr)
        time.sleep(2)

    # Discover new subreddits
    new_discoveries = []
    if discover_mode:
        new_discoveries = discover_new(seen)
        if new_discoveries:
            # Save new discoveries separately for review
            os.makedirs(os.path.dirname(NEW_DISCOVERIES_FILE), exist_ok=True)
            with open(NEW_DISCOVERIES_FILE, "w") as f:
                json.dump({
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                    "count": len(new_discoveries),
                    "note": "New subreddits found — review and add to seed-subreddits.json if relevant",
                    "subreddits": [
                        {
                            "name": s["name"],
                            "subscribers": s["subscribers"],
                            "relevance_score": s["relevance_score"],
                            "description": s.get("description", ""),
                            "score_breakdown": s.get("score_breakdown", []),
                        }
                        for s in new_discoveries
                    ],
                }, f, indent=2)
            print(f"\n  Saved {len(new_discoveries)} new discoveries to {NEW_DISCOVERIES_FILE}",
                  file=sys.stderr)

    # Sort by relevance score
    all_subs.sort(key=lambda s: s["relevance_score"], reverse=True)

    # Save output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    output = {
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "total_subreddits": len(all_subs),
        "mode": "discover" if discover_mode else "refresh",
        "new_candidates": len(new_discoveries),
        "subreddits": all_subs,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # Summary
    print(f"\n{'='*60}", file=sys.stderr)
    print(f"  Done — {len(all_subs)} subreddits saved to {OUTPUT_FILE}", file=sys.stderr)
    if new_discoveries:
        print(f"  {len(new_discoveries)} NEW candidates for review:", file=sys.stderr)
        for s in new_discoveries[:10]:
            print(f"    r/{s['name']:<28} subs={s['subscribers']:>10,}  "
                  f"score={s['relevance_score']:.0f}  {s.get('description', '')[:40]}",
                  file=sys.stderr)
        if len(new_discoveries) > 10:
            print(f"    ... and {len(new_discoveries) - 10} more in {NEW_DISCOVERIES_FILE}",
                  file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # Push to sheets
    if push_sheets:
        print("  Pushing to Google Sheets...", file=sys.stderr)
        sheets_script = os.path.join(BASE_DIR, "scripts/reddit_ads_to_sheets.py")
        subprocess.run(
            [os.path.join(BASE_DIR, ".venv/bin/python3"), sheets_script, "--agent", "discovery"],
            cwd=BASE_DIR,
        )

    # Print table to stdout
    print(f"\n{'Subreddit':<30} {'Subs':>10} {'Score':>6}  Description")
    print("─" * 80)
    for s in all_subs:
        desc = (s.get("description") or "")[:35]
        print(f"r/{s['name']:<28} {s['subscribers']:>10,} {s['relevance_score']:>6.1f}  {desc}")


def main():
    parser = argparse.ArgumentParser(
        description="Reddit Subreddit Discovery — maintain and grow the curated subreddit list."
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Also search for new subreddits (slower, weekly)",
    )
    parser.add_argument(
        "--sheets",
        action="store_true",
        help="Push results to Google Sheets after completion",
    )
    args = parser.parse_args()

    run(discover_mode=args.discover, push_sheets=args.sheets)


if __name__ == "__main__":
    main()
