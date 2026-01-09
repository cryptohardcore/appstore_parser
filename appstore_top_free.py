#!/usr/bin/env python3

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup

# Charts to monitor
FREE_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
PAID_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-paid"

# State files
FREE_STATE_PATH = Path("state/top3_free.json")   # stores [{"rank":1,"name":"..."}, ...] for ranks 1-3
PAID_STATE_PATH = Path("state/top1_paid.json")   # stores [{"rank":1,"name":"..."}]
HEARTBEAT_STATE_PATH = Path("state/last_heartbeat.json")  # stores {"ts":"ISO8601"}

# Heartbeat cadence
HEARTBEAT_INTERVAL = timedelta(hours=4)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ------------------------
# Fetch page
# ------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


# ------------------------
# Parse helpers
# ------------------------
def extract_app_name_from_anchor(a) -> str | None:
    # Prefer structured title elements if present
    title_el = a.find(["h3", "h2"])
    if title_el:
        name = title_el.get_text(" ", strip=True)
        return name or None

    # Fallback: parse visible anchor text like "1 ChatGPT ... View"
    text = a.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()

    # Must start with rank
    if not re.match(r"^\d+\s", text):
        return None

    # Remove optional trailing "View"
    text = re.sub(r"\s+View$", "", text, flags=re.IGNORECASE).strip()

    m = re.match(r"^(\d+)\s+(.+)$", text)
    if not m:
        return None

    rest = m.group(2).strip()
    words = rest.split()
    if not words:
        return None

    # Heuristic: first few words are usually the app name
    return " ".join(words[:6]).strip()


def parse_chart_topN(html: str, ranks: List[int]) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)

    candidates: List[Tuple[int, str]] = []

    for a in anchors:
        href = a.get("href", "")
        if "/app/" not in href or "id" not in href:
            continue

        text = a.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()

        m_rank = re.match(r"^(\d+)\s", text)
        if not m_rank:
            continue

        rank = int(m_rank.group(1))
        if rank not in ranks:
            continue

        name = extract_app_name_from_anchor(a)
        if not name:
            continue

        candidates.append((rank, name))

    # De-dupe by rank, first occurrence wins
    by_rank: Dict[int, str] = {}
    for rank, name in candidates:
        if rank not in by_rank:
            by_rank[rank] = name

    out: List[Dict[str, Any]] = []
    for r in ranks:
        if r in by_rank:
            out.append({"rank": r, "name": by_rank[r]})

    return out


def format_ranked_list(items: List[Dict[str, Any]]) -> str:
    return "\n".join([f'{x["rank"]}. {x["name"]}' for x in items])


# ------------------------
# State handling
# ------------------------
def load_json_list(path: Path) -> List[Dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json_list(path: Path, data: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_last_heartbeat() -> datetime | None:
    if not HEARTBEAT_STATE_PATH.exists():
        return None
    try:
        data = json.loads(HEARTBEAT_STATE_PATH.read_text(encoding="utf-8"))
        ts = data.get("ts")
        return datetime.fromisoformat(ts) if ts else None
    except Exception:
        return None


def save_last_heartbeat(ts: datetime) -> None:
    HEARTBEAT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_STATE_PATH.write_text(
        json.dumps({"ts": ts.isoformat()}, indent=2),
        encoding="utf-8",
    )


# ------------------------
# Telegram
# ------------------------
def send_telegram(message: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("Telegram secrets not set; skipping alert.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=30)
        if not r.ok:
            print("Telegram sendMessage failed!")
            print("Status:", r.status_code)
            print("Response:", r.text)
            return
        print("Telegram message delivered.")
    except Exception as e:
        print("Telegram request exception:", repr(e))


# ------------------------
# Main
# ------------------------
def main() -> int:
    # ---- Fetch + parse FREE top 3 ----
    free_html = fetch_html(FREE_URL)
    free_top3 = parse_chart_topN(free_html, ranks=[1, 2, 3])

    if len(free_top3) < 3:
        print("Failed to parse FREE top 3.")
        print("Parsed:", free_top3)
        return 1

    # ---- Fetch + parse PAID top 1 ----
    paid_html = fetch_html(PAID_URL)
    paid_top1 = parse_chart_topN(paid_html, ranks=[1])

    if len(paid_top1) < 1:
        print("Failed to parse PAID top 1.")
        print("Parsed:", paid_top1)
        return 1

    prev_free = load_json_list(FREE_STATE_PATH)
    prev_paid = load_json_list(PAID_STATE_PATH)

    # Save current states (workflow will commit only if changed)
    save_json_list(FREE_STATE_PATH, free_top3)
    save_json_list(PAID_STATE_PATH, paid_top1)

    print("Current FREE Top 3:")
    print(format_ranked_list(free_top3))
    print("\nCurrent PAID Top 1:")
    print(format_ranked_list(paid_top1))

    # ---- Heartbeat (once every 4 hours) ----
    now = datetime.utcnow()
    last_heartbeat = load_last_heartbeat()

    if last_heartbeat is None or now - last_heartbeat >= HEARTBEAT_INTERVAL:
        heartbeat = (
            "✅ App Store Charts (US iPhone) health check\n\n"
            "Top Free:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            "Top Paid:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            f"Free source: {FREE_URL}\n"
            f"Paid source: {PAID_URL}"
        )
        send_telegram(heartbeat)
        save_last_heartbeat(now)
        print("Heartbeat sent.")
    else:
        print("Heartbeat skipped (interval not reached).")

    # ---- Change alerts (immediate) ----
    if prev_free is not None and prev_free != free_top3:
        msg = (
            "📲 App Store Top Free (US iPhone) changed!\n\n"
            "Before:\n"
            f"{format_ranked_list(prev_free)}\n\n"
            "Now:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            f"Source: {FREE_URL}"
        )
        send_telegram(msg)
        print("Free change alert sent.")
    else:
        print("No changes in FREE Top 3 (or first run).")

    if prev_paid is not None and prev_paid != paid_top1:
        msg = (
            "💰 App Store Top Paid #1 (US iPhone) changed!\n\n"
            "Before:\n"
            f"{format_ranked_list(prev_paid)}\n\n"
            "Now:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            f"Source: {PAID_URL}"
        )
        send_telegram(msg)
        print("Paid change alert sent.")
    else:
        print("No changes in PAID Top 1 (or first run).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
