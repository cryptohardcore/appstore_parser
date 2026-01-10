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

# ------------------------
# URLs to monitor
# ------------------------
FREE_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
PAID_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-paid"
TSA_URL = "https://www.tsa.gov/travel/passenger-volumes"

# ------------------------
# State files
# ------------------------
FREE_STATE_PATH = Path("state/top3_free.json")          # [{"rank":1,"name":"..."}, ...] ranks 1-3
PAID_STATE_PATH = Path("state/top1_paid.json")          # [{"rank":1,"name":"..."}]
TSA_STATE_PATH = Path("state/tsa_latest.json")          # {"date":"YYYY-MM-DD","passengers":123}

HEARTBEAT_STATE_PATH = Path("state/last_heartbeat.json")  # {"ts":"ISO8601"}
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
# Fetch
# ------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


# ------------------------
# App Store parsing
# ------------------------
def extract_app_name_from_anchor(a) -> str | None:
    title_el = a.find(["h3", "h2"])
    if title_el:
        name = title_el.get_text(" ", strip=True)
        return name or None

    text = a.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()

    if not re.match(r"^\d+\s", text):
        return None

    text = re.sub(r"\s+View$", "", text, flags=re.IGNORECASE).strip()
    m = re.match(r"^(\d+)\s+(.+)$", text)
    if not m:
        return None

    rest = m.group(2).strip()
    words = rest.split()
    if not words:
        return None

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
# TSA parsing
# ------------------------
def _parse_tsa_date(s: str) -> str | None:
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def _parse_int(s: str) -> int | None:
    s = s.strip()
    s = re.sub(r"[,\s]", "", s)
    if not s.isdigit():
        return None
    try:
        return int(s)
    except Exception:
        return None


def parse_tsa_latest(html: str) -> Dict[str, Any] | None:
    """
    Tries to find the TSA passenger volumes table and extract the most recent
    row (date + total traveler throughput).
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return None

    best_row = None

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Find header row text to identify the right table
        header_text = " ".join(rows[0].get_text(" ", strip=True).lower().split())
        if "date" not in header_text:
            continue
        # Often includes "total traveler throughput"
        if "throughput" not in header_text and "traveler" not in header_text and "passenger" not in header_text:
            # still might be the right table, but deprioritize
            pass

        # First data row is usually the latest
        for r in rows[1:3]:  # look at first couple data rows for safety
            cells = [c.get_text(" ", strip=True) for c in r.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            date_iso = _parse_tsa_date(cells[0])
            passengers = _parse_int(cells[1])
            if date_iso and passengers is not None:
                best_row = {"date": date_iso, "passengers": passengers}
                break

        if best_row:
            break

    return best_row


def format_tsa(tsa: Dict[str, Any] | None) -> str:
    if not tsa:
        return "TSA: (parse failed)"
    return f"TSA: {tsa['date']} — {tsa['passengers']:,} passengers"


# ------------------------
# State handling
# ------------------------
def load_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_last_heartbeat() -> datetime | None:
    data = load_json(HEARTBEAT_STATE_PATH)
    if not isinstance(data, dict):
        return None
    ts = data.get("ts")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def save_last_heartbeat(ts: datetime) -> None:
    save_json(HEARTBEAT_STATE_PATH, {"ts": ts.isoformat()})


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
    # ---- App Store: FREE top 3 ----
    free_html = fetch_html(FREE_URL)
    free_top3 = parse_chart_topN(free_html, ranks=[1, 2, 3])
    if len(free_top3) < 3:
        print("Failed to parse FREE top 3.")
        print("Parsed:", free_top3)
        return 1

    # ---- App Store: PAID top 1 ----
    paid_html = fetch_html(PAID_URL)
    paid_top1 = parse_chart_topN(paid_html, ranks=[1])
    if len(paid_top1) < 1:
        print("Failed to parse PAID top 1.")
        print("Parsed:", paid_top1)
        return 1

    # ---- TSA: latest date + passengers ----
    tsa_html = fetch_html(TSA_URL)
    tsa_latest = parse_tsa_latest(tsa_html)
    if not tsa_latest:
        print("Failed to parse TSA latest row.")
    else:
        print("TSA latest:", tsa_latest)

    prev_free = load_json(FREE_STATE_PATH)
    prev_paid = load_json(PAID_STATE_PATH)
    prev_tsa = load_json(TSA_STATE_PATH)

    # Save states (workflow commits only if something changes)
    save_json(FREE_STATE_PATH, free_top3)
    save_json(PAID_STATE_PATH, paid_top1)
    if tsa_latest:
        save_json(TSA_STATE_PATH, tsa_latest)

    print("Current FREE Top 3:")
    print(format_ranked_list(free_top3))
    print("\nCurrent PAID Top 1:")
    print(format_ranked_list(paid_top1))
    print("\n" + format_tsa(tsa_latest))

    # ---- Heartbeat (once every 4 hours) ----
    now = datetime.utcnow()
    last_heartbeat = load_last_heartbeat()

    if last_heartbeat is None or now - last_heartbeat >= HEARTBEAT_INTERVAL:
        heartbeat = (
            "✅ Monitor health check\n\n"
            "📱 App Store (US iPhone)\n"
            "Top Free:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            "Top Paid:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            f"{format_tsa(tsa_latest)}\n\n"
            f"Free: {FREE_URL}\n"
            f"Paid: {PAID_URL}\n"
            f"TSA: {TSA_URL}"
        )
        send_telegram(heartbeat)
        save_last_heartbeat(now)
        print("Heartbeat sent.")
    else:
        print("Heartbeat skipped (interval not reached).")

    # ---- Change alerts (immediate) ----
    if isinstance(prev_free, list) and prev_free != free_top3:
        msg = (
            "📲 App Store Top Free changed!\n\n"
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

    if isinstance(prev_paid, list) and prev_paid != paid_top1:
        msg = (
            "💰 App Store Top Paid #1 changed!\n\n"
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

    # TSA change alert: only when DATE changes
    if tsa_latest and isinstance(prev_tsa, dict):
        prev_date = prev_tsa.get("date")
        if prev_date and prev_date != tsa_latest.get("date"):
            msg = (
                "✈️ TSA passenger volumes updated!\n\n"
                f"Previous: {prev_tsa.get('date')} — {int(prev_tsa.get('passengers', 0)):,} passengers\n"
                f"Latest: {tsa_latest.get('date')} — {int(tsa_latest.get('passengers', 0)):,} passengers\n\n"
                f"Source: {TSA_URL}"
            )
            send_telegram(msg)
            print("TSA date-change alert sent.")
        else:
            print("No TSA date change (or first run).")
    else:
        print("No TSA date change (or first run / parse failed).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
