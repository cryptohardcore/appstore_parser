#!/usr/bin/env python3

import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup

# ======================
# URLs
# ======================
FREE_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
PAID_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-paid"
TSA_URL = "https://www.tsa.gov/travel/passenger-volumes"
APPROVAL_CSV_URL = "https://static.dwcdn.net/data/kSCt4.csv"

# ======================
# State files
# ======================
FREE_STATE_PATH = Path("state/top3_free.json")
PAID_STATE_PATH = Path("state/top1_paid.json")
TSA_STATE_PATH = Path("state/tsa_latest.json")
APPROVAL_STATE_PATH = Path("state/approval_latest.json")
HEARTBEAT_STATE_PATH = Path("state/last_heartbeat.json")

HEARTBEAT_INTERVAL = timedelta(hours=4)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ======================
# Helpers
# ======================
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


# ======================
# App Store parsing
# ======================
def extract_app_name_from_anchor(a) -> str | None:
    title = a.find(["h2", "h3"])
    if title:
        return title.get_text(strip=True)
    return None


def parse_chart_topN(html: str, ranks: List[int]) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    out = []

    for a in anchors:
        txt = a.get_text(" ", strip=True)
        m = re.match(r"^(\d+)\s+", txt)
        if not m:
            continue
        rank = int(m.group(1))
        if rank not in ranks:
            continue
        name = extract_app_name_from_anchor(a)
        if name:
            out.append({"rank": rank, "name": name})

    return sorted(out, key=lambda x: x["rank"])


def format_ranked_list(items):
    return "\n".join(f'{x["rank"]}. {x["name"]}' for x in items)


# ======================
# TSA parsing
# ======================
def parse_tsa_latest(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return None

    for r in table.find_all("tr")[1:3]:
        cols = [c.get_text(strip=True) for c in r.find_all("td")]
        if len(cols) >= 2:
            dt = datetime.strptime(cols[0], "%m/%d/%Y").strftime("%Y-%m-%d")
            pax = int(cols[1].replace(",", ""))
            return {"date": dt, "passengers": pax}
    return None


# ======================
# Trump approval parsing (FIXED)
# ======================
def parse_latest_trump_approval(csv_text: str) -> Dict[str, Any] | None:
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return None

    # Newest rows are appended at END
    for r in reversed(rows):
        approve = (r.get("approve") or "").strip()
        modeldate = (r.get("modeldate") or "").strip()
        if approve and modeldate:
            # Normalize date
            modeldate_iso = None
            for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
                try:
                    modeldate_iso = datetime.strptime(modeldate, fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass

            return {
                "approve": approve,
                "modeldate": modeldate,
                "modeldate_iso": modeldate_iso or modeldate,
            }

    return None


# ======================
# Telegram
# ======================
def send_telegram(msg: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "disable_web_page_preview": True},
        timeout=20,
    )


# ======================
# Main
# ======================
def main():
    prev_free = load_json(FREE_STATE_PATH)
    prev_paid = load_json(PAID_STATE_PATH)
    prev_tsa = load_json(TSA_STATE_PATH)
    prev_app = load_json(APPROVAL_STATE_PATH)
    prev_hb = load_json(HEARTBEAT_STATE_PATH)

    free_top3 = parse_chart_topN(fetch_html(FREE_URL), [1, 2, 3])
    paid_top1 = parse_chart_topN(fetch_html(PAID_URL), [1])

    tsa_latest = parse_tsa_latest(fetch_html(TSA_URL))
    approval_latest = parse_latest_trump_approval(fetch_text(APPROVAL_CSV_URL))

    save_json(FREE_STATE_PATH, free_top3)
    save_json(PAID_STATE_PATH, paid_top1)

    if tsa_latest:
        save_json(TSA_STATE_PATH, tsa_latest)
    if approval_latest:
        save_json(APPROVAL_STATE_PATH, approval_latest)

    # ======================
    # HEARTBEAT (4h)
    # ======================
    now = datetime.utcnow()
    last_ts = (
        datetime.fromisoformat(prev_hb["ts"])
        if isinstance(prev_hb, dict) and "ts" in prev_hb
        else None
    )

    if not last_ts or now - last_ts >= HEARTBEAT_INTERVAL:
        heartbeat = (
            "✅ Monitor health check\n\n"
            "📱 App Store (US iPhone)\n"
            "Top Free:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            "Top Paid:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            "✈️ TSA:\n"
            f"{tsa_latest['date']} — {tsa_latest['passengers']:,} passengers\n\n"
            "Trump Approval:\n"
            f"📊 Approve: {approval_latest['approve']}\n"
            f"🗓️ Modeldate: {approval_latest['modeldate']}"
        )
        send_telegram(heartbeat)
        save_json(HEARTBEAT_STATE_PATH, {"ts": now.isoformat()})

    # ======================
    # CHANGE ALERTS
    # ======================
    if isinstance(prev_free, list) and prev_free != free_top3:
        send_telegram(
            "📲 App Store Top Free changed!\n\n"
            f"{format_ranked_list(free_top3)}"
        )

    if isinstance(prev_paid, list) and prev_paid != paid_top1:
        send_telegram(
            "💰 App Store Top Paid #1 changed!\n\n"
            f"{format_ranked_list(paid_top1)}"
        )

    if tsa_latest and isinstance(prev_tsa, dict):
        if prev_tsa.get("date") != tsa_latest.get("date"):
            send_telegram(
                "✈️ TSA passenger volumes updated!\n\n"
                f"{tsa_latest['date']} — {tsa_latest['passengers']:,} passengers"
            )

    if approval_latest and isinstance(prev_app, dict):
        date_changed = prev_app.get("modeldate_iso") != approval_latest.get("modeldate_iso")
        approve_changed = prev_app.get("approve") != approval_latest.get("approve")

        if date_changed or approve_changed:
            send_telegram(
                "📊 Trump Approval updated!\n\n"
                f"📊 Approve: {prev_app.get('approve')} → {approval_latest.get('approve')}\n"
                f"🗓️ Modeldate: {prev_app.get('modeldate')} → {approval_latest.get('modeldate')}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
