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

# ------------------------
# URLs to monitor
# ------------------------
FREE_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-free"
PAID_URL = "https://apps.apple.com/us/iphone/charts/36?chart=top-paid"
TSA_URL = "https://www.tsa.gov/travel/passenger-volumes"
APPROVAL_CSV_URL = "https://static.dwcdn.net/data/kSCt4.csv"

# ------------------------
# State files
# ------------------------
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


# ------------------------
# Fetch helpers
# ------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_text(url: str) -> str:
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

        for r in rows[1:4]:
            cells = [c.get_text(" ", strip=True) for c in r.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            date_iso = _parse_tsa_date(cells[0])
            passengers = _parse_int(cells[1])
            if date_iso and passengers is not None:
                return {"date": date_iso, "passengers": passengers}

    return None


def format_tsa(tsa: Dict[str, Any] | None) -> str:
    if not isinstance(tsa, dict):
        return "✈️ TSA: (no data yet)"
    date = tsa.get("date")
    pax = tsa.get("passengers")
    if not date or pax is None:
        return "✈️ TSA: (invalid data)"
    try:
        pax_int = int(pax)
    except Exception:
        return "✈️ TSA: (invalid data)"
    return f"✈️ TSA: {date} — {pax_int:,} passengers"


# ------------------------
# Approval CSV parsing (approve/approval)
# ------------------------
def parse_latest_approve_from_csv(csv_text: str) -> Dict[str, Any] | None:
    """
    Finds the newest non-empty value in the 'approve' column.
    Also supports 'approval' as a fallback.
    Newest = last row in the CSV with non-empty approve value.
    """
    # If we accidentally fetched HTML, bail
    head = csv_text.lstrip()[:200].lower()
    if head.startswith("<!doctype") or head.startswith("<html"):
        return None

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return None

    # handle BOM on first header
    fieldnames = [f.lstrip("\ufeff") if f else f for f in reader.fieldnames]

    # Prefer 'approve', fallback to 'approval'
    col = None
    for target in ("approve", "approval"):
        for c in fieldnames:
            if c and c.strip().lower() == target:
                col = c
                break
        if col:
            break
    if not col:
        return None

    # optional key column
    key_col = None
    for candidate in ("date", "day", "week", "month", "timestamp", "time"):
        for c in fieldnames:
            if c and c.strip().lower() == candidate:
                key_col = c
                break
        if key_col:
            break

    rows = list(reader)
    if not rows:
        return None

    for i in range(len(rows) - 1, -1, -1):
        row = rows[i]
        val = (row.get(col) or "").strip()
        if val == "":
            continue

        row_key = (row.get(key_col) or "").strip() if key_col else ""
        if not row_key:
            row_key = f"row_{i+1}"

        return {"row_key": row_key, "approve": val, "col": col}

    return None


def format_approve(a: Dict[str, Any] | None) -> str:
    if not isinstance(a, dict):
        return "📊 Approve: (no data yet)"
    key = a.get("row_key")
    val = a.get("approve")
    if not val:
        return "📊 Approve: (invalid data)"
    if key:
        return f"📊 Approve: {val} (latest: {key})"
    return f"📊 Approve: {val} (latest)"


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
    # Load previous states early
    prev_free = load_json(FREE_STATE_PATH)
    prev_paid = load_json(PAID_STATE_PATH)
    prev_tsa = load_json(TSA_STATE_PATH)
    prev_approve = load_json(APPROVAL_STATE_PATH)

    # ---- App Store ----
    free_top3 = parse_chart_topN(fetch_html(FREE_URL), ranks=[1, 2, 3])
    if len(free_top3) < 3:
        print("Failed to parse FREE top 3.")
        print("Parsed:", free_top3)
        return 1

    paid_top1 = parse_chart_topN(fetch_html(PAID_URL), ranks=[1])
    if len(paid_top1) < 1:
        print("Failed to parse PAID top 1.")
        print("Parsed:", paid_top1)
        return 1

    # ---- TSA ----
    tsa_latest = None
    try:
        tsa_latest = parse_tsa_latest(fetch_html(TSA_URL))
    except Exception as e:
        print("TSA fetch/parse exception:", repr(e))

    if tsa_latest:
        save_json(TSA_STATE_PATH, tsa_latest)
        print("TSA latest:", tsa_latest)
    else:
        print("Failed to parse TSA latest row. Using previous TSA state for heartbeat if available.")

    # ---- Approve CSV ----
    approve_latest = None
    try:
        approve_latest = parse_latest_approve_from_csv(fetch_text(APPROVAL_CSV_URL))
    except Exception as e:
        print("Approve CSV fetch/parse exception:", repr(e))

    if approve_latest:
        save_json(APPROVAL_STATE_PATH, approve_latest)
        print("Approve latest:", approve_latest)
    else:
        print("Failed to parse approve CSV. Using previous approve state for heartbeat if available.")

    # Save App Store states
    save_json(FREE_STATE_PATH, free_top3)
    save_json(PAID_STATE_PATH, paid_top1)

    # ---- Heartbeat (once every 4 hours) ----
    now = datetime.utcnow()
    last_heartbeat = load_last_heartbeat()

    tsa_for_heartbeat = tsa_latest if tsa_latest else prev_tsa
    approve_for_heartbeat = approve_latest if approve_latest else prev_approve

    if last_heartbeat is None or now - last_heartbeat >= HEARTBEAT_INTERVAL:
        heartbeat = (
            "✅ Monitor health check\n\n"
            "📱 App Store (US iPhone)\n"
            "Top Free:\n"
            f"{format_ranked_list(free_top3)}\n\n"
            "Top Paid:\n"
            f"{format_ranked_list(paid_top1)}\n\n"
            f"{format_tsa(tsa_for_heartbeat)}\n"
            f"{format_approve(approve_for_heartbeat)}\n\n"
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

    # TSA alert: only when DATE changes
    if tsa_latest and isinstance(prev_tsa, dict):
        prev_date = prev_tsa.get("date")
        new_date = tsa_latest.get("date")
        if prev_date and new_date and prev_date != new_date:
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

    # Approve alert: when row_key or value changes
    if approve_latest and isinstance(prev_approve, dict):
        prev_key = (prev_approve.get("row_key") or "").strip()
        prev_val = (str(prev_approve.get("approve") or prev_approve.get("approval") or "")).strip()
        new_key = (approve_latest.get("row_key") or "").strip()
        new_val = (str(approve_latest.get("approve") or "")).strip()

        if (prev_key and new_key and prev_key != new_key) or (prev_val and new_val and prev_val != new_val):
            msg = (
                "📊 Approve data updated!\n\n"
                f"Previous: {prev_key or '(unknown)'} — {prev_val or '(empty)'}\n"
                f"Latest: {new_key or '(unknown)'} — {new_val or '(empty)'}\n\n"
                f"Source: {APPROVAL_CSV_URL}"
            )
            send_telegram(msg)
            print("Approve update alert sent.")
        else:
            print("No approve update (or first run).")
    else:
        print("No approve update (or first run / parse failed).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
