#!/usr/bin/env python3
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup

URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
STATE_PATH = Path("state/top3.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def extract_app_name_from_anchor(a) -> str | None:
    title_el = a.find(["h3", "h2"])
    if title_el:
        name = title_el.get_text(" ", strip=True)
        return name or None

    text = a.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    if not text.lower().endswith(" view"):
        return None

    text = text[:-5].strip()
    m = re.match(r"^(\d+)\s+(.+)$", text)
    if not m:
        return None

    rest = m.group(2).strip()
    words = rest.split()
    if not words:
        return None
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

        name = re.sub(r"\s+View$", "", name, flags=re.IGNORECASE).strip()
        candidates.append((rank, name))

    by_rank: Dict[int, str] = {}
    for rank, name in candidates:
        if rank not in by_rank:
            by_rank[rank] = name

    top3 = []
    for r in [1, 2, 3]:
        if r in by_rank:
            top3.append({"rank": r, "name": by_rank[r]})
    return top3

def load_prev_state() -> List[Dict[str, Any]] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None

def save_state(top3: List[Dict[str, Any]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(top3, ensure_ascii=False, indent=2), encoding="utf-8")

def format_top3(top3: List[Dict[str, Any]]) -> str:
    return "\n".join([f'{x["rank"]}. {x["name"]}' for x in top3])

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
        return

def main() -> int:
    html = fetch_html(URL)
    top3 = parse_top_free_top3(html)

    if len(top3) < 3:
        print("Failed to parse top 3. Apple markup may have changed or request was blocked.")
        print("Parsed:", top3)
        return 1

    prev = load_prev_state()
    save_state(top3)

    print("Current Top 3:")
    print(format_top3(top3))

    if prev is None:
        print("No previous state found. Saved baseline.")
        return 0

    if prev != top3:
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
        print("No changes in Top 3.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
