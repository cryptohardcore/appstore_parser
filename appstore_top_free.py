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
# Fetch helpers
# ======================
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


# ======================
# State helpers
# ======================
def load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_json(path: Path, data):
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


# ======================
# App Store parsing
# ======================
def extract_app_name_from_anchor(a) -> str | None:
    title_el = a.find(["h3", "h2"])
    if title_el:
        name = title_el.get_text(" ", strip=True)
        return name or None

    # fallback: attempt to parse from anchor text
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


# ======================
# TSA parsing (robust)
# ======================
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
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return None

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        header_text = " ".join(rows[0].get_text(" ", strip=True).lower().split())
        if "date" not in header_text:
            continue

        # usually first data row is "yesterday"
        for r in rows[1:4]:
            cells = [c.get_text(" ", strip=True) for c in r.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            date_iso = _parse_tsa_date(cells[0])
            passengers = _parse_int(cells[1])
            if date_iso and passengers is not None:
                return {"date": date_iso, "passengers": passengers}

    return None


# ======================
# Trump approval parsing (FIXED: single-row, newest at end)
# ======================
def _is_html(text: str) -> bool:
    head = text.lstrip()[:200].lower()
    return head.startswith("<!doctype") or head.startswith("<html")


def parse_latest_trump_approval(csv_text: str) -> Dict[str, Any] | None:
    """
    Assumption (confirmed): newest update is appended at END of file.

    Rule:
    - Walk from the end and pick the first row where BOTH approve and modeldate exist.
    - Read approve + modeldate from the SAME row (prevents mismatches).
    """
    if _is_html(csv_text):
        return None

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return None

    rows = list(reader)
    if not rows:
        return None

    for r in reversed(rows):
        approve = (r.get("approve") or "").strip()
        modeldate = (r.get("modeldate") or "").strip()
        if approve and modeldate:
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
def send_telegram(message: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("Telegram secrets not set; skipping.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}

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


# ======================
# Main
# ======================
def main() -> int:
    prev_free = load_json(FREE_STATE_PATH)
    prev_paid = load_json(PAID_STATE_PATH)
    prev_tsa = load_json(TSA_STATE_PATH)
    prev_approval = load_json(APPROVAL_STATE_PATH)

    # App Store
    free_top3 = parse_chart_topN(fetch_html(FREE_URL), ranks=[1, 2, 3])
    if len(free_top3) < 3:
        print("Failed to parse FREE top 3:", free_top3)
        return 1

    paid_top1 = parse_chart_topN(fetch_html(PAID_URL), ranks=[1])
    if len(paid_top1) < 1:
        print("Failed to parse PAID top 1:", paid_top1)
        return 1

    # TSA
    tsa_latest = None
    try:
        tsa_latest = parse_tsa_latest(fetch_html(TSA_URL))
    except Exception as e:
        print("TSA fetch/parse exception:", repr(e))

    if tsa_latest:
        save_json(TSA_STATE_PATH, tsa_latest)
    else:
        print("Failed to parse TSA latest row. Using previous TSA state for heartbeat if available.")

    # Trump approval
    approval_latest = None
    try:
        approval_latest = parse_latest_trump_approval(fetch_text(APPROVAL_CSV_URL))
    except Exception as e:
        print("Approval CSV fetch/parse exception:", repr(e))

    if approval_latest:
        save_json(APPROVAL_STATE_PATH, approval_latest)
    else:
        print("Failed to parse Trump approval CSV. Using previous state for heartbeat if available.")

    # Save App Store states
    save_json(FREE_STATE_PATH, free_top3)
    save_json(PAID_STATE_PATH, paid_top1)

    # Heartbeat (every 4h)
    now = datetime.utcnow()
    last_heartbeat = load_last_heartbeat()

    tsa_for_hb = tsa_latest if tsa_latest else prev_tsa
    approval_for_hb = approval_latest if approval_latest else prev_approval

    if last_heartbeat is None or now - last_heartbeat >= HEARTBEAT_INTERVAL:
        # TSA line
        tsa_line = "(no data yet)"
        if isinstance(tsa_for_hb, dict) and tsa_for_hb.get("date") and tsa_for_hb.get("passengers") is not None:
            tsa_line = f"{tsa_for_hb['date']} — {int(tsa_for_hb['passengers']):,} passengers"

        # Trump approval lines
        approve_val = "(no data)"
        modeldate_val = "(no data)"
        if isinstance(approval_for_hb, dict):
            approve_val = str(approval_for_hb.get("approve") or "(no data)")
            modeldate_val = str(approval_for_hb.get("modeldate") or "(no data)")

        heartbeat = (
            "✅ Monitor health check\n\n"
            "📱 App Store (US iPhone)\n"
            "Top Free:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            "Top Paid:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            "✈️ TSA:\n"
            f"{tsa_line}\n\n"
            "Trump Approval:\n"
            f"📊 Approve: {approve_val}\n"
            f"🗓️ Modeldate: {modeldate_val}\n\n"
            f"Free: {FREE_URL}\n"
            f"Paid: {PAID_URL}\n"
            f"TSA: {TSA_URL}\n"
            f"CSV: {APPROVAL_CSV_URL}"
        )
        send_telegram(heartbeat)
        save_last_heartbeat(now)
        print("Heartbeat sent.")
    else:
        print("Heartbeat skipped (interval not reached).")

    # Alerts: App Store changes
    if isinstance(prev_free, list) and prev_free != free_top3:
        send_telegram(
            "📲 App Store Top Free changed!\n\n"
            f"Before:\n{format_ranked_list(prev_free)}\n\n"
            f"Now:\n{format_ranked_list(free_top3)}\n\n"
            f"Source: {FREE_URL}"
        )
        print("Free change alert sent.")

    if isinstance(prev_paid, list) and prev_paid != paid_top1:
        send_telegram(
            "💰 App Store Top Paid #1 changed!\n\n"
            f"Before:\n{format_ranked_list(prev_paid)}\n\n"
            f"Now:\n{format_ranked_list(paid_top1)}\n\n"
            f"Source: {PAID_URL}"
        )
        print("Paid change alert sent.")

    # Alerts: TSA date-change
    if tsa_latest and isinstance(prev_tsa, dict):
        if prev_tsa.get("date") and tsa_latest.get("date") and prev_tsa["date"] != tsa_latest["date"]:
            send_telegram(
                "✈️ TSA passenger volumes updated!\n\n"
                f"Previous: {prev_tsa.get('date')} — {int(prev_tsa.get('passengers', 0)):,} passengers\n"
                f"Latest: {tsa_latest.get('date')} — {int(tsa_latest.get('passengers', 0)):,} passengers\n\n"
                f"Source: {TSA_URL}"
            )
            print("TSA date-change alert sent.")

    # Alerts: Trump approval date/value-change (compare DATE VALUE)
    if approval_latest and isinstance(prev_approval, dict):
        prev_date = str(prev_approval.get("modeldate_iso") or prev_approval.get("modeldate") or "").strip()
        new_date = str(approval_latest.get("modeldate_iso") or approval_latest.get("modeldate") or "").strip()

        prev_approve = str(prev_approval.get("approve") or "").strip()
        new_approve = str(approval_latest.get("approve") or "").strip()

        date_changed = bool(prev_date and new_date and prev_date != new_date)
        approve_changed = bool(prev_approve and new_approve and prev_approve != new_approve)

        if date_changed or approve_changed:
            send_telegram(
                "📊 Trump Approval updated!\n\n"
                f"📊 Approve: {prev_approve or '(empty)'} → {new_approve or '(empty)'}\n"
                f"🗓️ Modeldate: {prev_approval.get('modeldate') or '(empty)'} → {approval_latest.get('modeldate') or '(empty)'}\n\n"
                f"Source: {APPROVAL_CSV_URL}"
            )
            print("Trump approval update alert sent.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
