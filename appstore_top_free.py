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

URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
STATE_PATH = Path("state/top3.json")

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
    title_el = a.find(["h3", "h2"])
    if title_el:
        name = title_el.get_text(" ", strip=True)
        return name or None

    text = a.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()

    if not text.lower().endswith(" view"):
        return None

    text = text[:-5].strip()  # drop " View"
    m = re.match(r"^(\d+)\s+(.+)$", text)
    if not m:
        return None

    rest = m.group(2).strip()
    words = rest.split()
    if not words:
        return None

    # Heuristic: first few words are usually the app name
    return " ".join(words[:6]).strip()


def parse_top_free_top3(html: str) -> List[Dict[str, Any]]:
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
        name = extract_app_name_from_anchor(a)
        if not name:
            continue

        candidates.append((rank, name))

    # De-dupe by rank, first occurrence wins
    by_rank: Dict[int, str] = {}
    for rank, name in candidates:
        if rank not in by_rank:
            by_rank[rank] = name

    top3 = []
    for r in [1, 2, 3]:
        if r in by_rank:
            top3.append({"rank": r, "name": by_rank[r]})

    return top3


# ------------------------
# State handling
# ------------------------
def load_prev_state() -> List[Dict[str, Any]] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_state(top3: List[Dict[str, Any]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(top3, ensure_ascii=False, indent=2),
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


def format_top3(top3: List[Dict[str, Any]]) -> str:
    return "\n".join([f'{x["rank"]}. {x["name"]}' for x in top3])


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
    html = fetch_html(URL)
    top3 = parse_top_free_top3(html)

    if len(top3) < 3:
        print("Failed to parse top 3.")
        print("Parsed:", top3)
        return 1

    prev = load_prev_state()
    save_state(top3)

    print("Current Top 3:")
    print(format_top3(top3))

    # Heartbeat (once every 4 hours)
    now = datetime.utcnow()
    last_heartbeat = load_last_heartbeat()

    if last_heartbeat is None or now - last_heartbeat >= HEARTBEAT_INTERVAL:
        heartbeat = (
            "✅ App Store Top Free (US iPhone) health check\n\n"
            f"{format_top3(top3)}\n\n"
            f"Source: {URL}"
        )
        send_telegram(heartbeat)
        save_last_heartbeat(now)
        print("Heartbeat sent.")
    else:
        print("Heartbeat skipped (interval not reached).")

    # Change alert (immediate)
    if prev is not None and prev != top3:
        msg = (
            "📲 App Store Top Free (US iPhone) changed!\n\n"
            "Before:\n"
            f"{format_top3(prev)}\n\n"
            "Now:\n"
            f"{format_top3(top3)}\n\n"
            f"Source: {URL}"
        )
        send_telegram(msg)
        print("Change detected → Telegram alert sent.")
    else:
        print("No changes in Top 3 (or first run).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
