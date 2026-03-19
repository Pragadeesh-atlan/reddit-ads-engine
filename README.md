# Reddit Ads Engine

3-agent pipeline that discovers active Reddit communities, monitors conversations, and activates targeted ads.

## Agents

| Agent | Script | What it does |
|-------|--------|-------------|
| **1. Discover** | `scripts/reddit_discover.py` | Maintains 33 curated subreddits focused on AI context, semantic layer, ontology, knowledge graphs |
| **2. Monitor** | `scripts/reddit_monitor.py` | Polls RSS feeds, gets latest conversation + date, classifies active/stale |
| **3. Activate** | `scripts/reddit_activate.py` | Generates targeting list + budget allocation, pushes to Google Sheets |

## Quick start

```bash
# Setup
python3 -m venv .venv
.venv/bin/pip install google-api-python-client google-auth

# Agent 1: Refresh subreddit list
.venv/bin/python3 scripts/reddit_discover.py

# Agent 2: Check what's active
.venv/bin/python3 scripts/reddit_monitor.py

# Agent 3: Generate activation plan
.venv/bin/python3 scripts/reddit_activate.py --budget 50
```

## Google Sheets integration

```bash
# Authenticate (one time)
gcloud auth application-default login \
  --scopes="https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"
gcloud auth application-default set-quota-project YOUR_PROJECT_ID

# Push results to sheets
.venv/bin/python3 scripts/reddit_monitor.py --sheets
.venv/bin/python3 scripts/reddit_activate.py --sheets
```

## Reddit Ads API (pending)

When Reddit approves API access, Agent 3 will be extended to automatically create/pause ads based on subreddit activity. Credentials needed in `.env`:

```
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USERNAME=
REDDIT_PASSWORD=
REDDIT_ADVERTISER_ID=
```

## Data files

| File | What |
|------|------|
| `intelligence/reddit-ads/seed-subreddits.json` | 33 curated subreddits (manually audited) |
| `intelligence/reddit-ads/activity-report.json` | Latest activity per subreddit |
| `intelligence/reddit-ads/activation-plan.json` | Budget allocation + targeting list |

## No API keys needed

Agents 1-2 use Reddit's public RSS/JSON endpoints. No authentication required.
