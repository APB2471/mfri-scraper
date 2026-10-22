#!/usr/bin/env python3
"""
MFRI Initial EMT (EMS-106) Schedule Monitor
Stateless, systemd-ready scraper with verified Gmail TLS, DOM health checks,
registration-open transition alerts, and atomic state persistence.
"""

import argparse
import fcntl
import json
import logging
import os
import re
import shutil
import smtplib
import ssl
import sys
import tempfile
from datetime import date, datetime
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Paths & Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
PROD_STATE_PATH = BASE_DIR / "seen_courses.json"
TEST_STATE_PATH = BASE_DIR / "seen_courses.test.json"
HEALTH_PATH = BASE_DIR / "health.json"
LOCK_PATH = BASE_DIR / "scraper.lock"
LOG_PATH = BASE_DIR / "scraper.log"
DEFAULT_MOCK_PATH = BASE_DIR / "mock_schedule.html"

TARGET_URL = "https://www.mfri.org/schedule/msfs/"
REQUEST_TIMEOUT = 15

# Authoritative ID patterns
INITIAL_EMT_ID_RE = re.compile(r"\b(EMS-106-[A-Z0-9]+-\d{4})\b", re.IGNORECASE)
EXCLUDED_CODE_RE = re.compile(r"\bEMS-(?:216|217|218|338|343)\b", re.IGNORECASE)

# Field extraction patterns
START_DATE_RE = re.compile(r"Start Date:\s*(\d{2}-\d{2}-\d{4})", re.IGNORECASE)
END_DATE_RE = re.compile(r"End Date:\s*(\d{2}-\d{2}-\d{4})", re.IGNORECASE)
SINGLE_DATE_RE = re.compile(r"\bDate:\s*(\d{2}-\d{2}-\d{4})", re.IGNORECASE)
CLASS_TIME_RE = re.compile(
    r"(?:First Class Time|Time):\s*(\d{2}:\d{2}\s*-\s*\d{2}:\d{2})", re.IGNORECASE
)
REG_OPEN_RE = re.compile(r"Registration Open:\s*(\d{2}-\d{2}-\d{4})", re.IGNORECASE)
REG_CLOSE_RE = re.compile(r"Registration Close:\s*(\d{2}-\d{2}-\d{4})", re.IGNORECASE)
DAYS_RE = re.compile(
    r"\b((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?"
    r"(?:(?:,\s*|\s+and\s+)(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?)*)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Logging (Rotating File + Stdout for systemd journald)
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("mfri_scraper")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not logger.handlers:
        rotating_handler = RotatingFileHandler(
            LOG_PATH, maxBytes=1_048_576, backupCount=5, encoding="utf-8"
        )
        rotating_handler.setFormatter(formatter)
        logger.addHandler(rotating_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


# ---------------------------------------------------------------------------
# Atomic File I/O & State Management
# ---------------------------------------------------------------------------
def atomic_write_json(target_path: Path, payload: Dict[str, Any]) -> None:
    """Atomically write JSON and fsync both the file and parent directory."""
    parent_dir = target_path.parent
    temp_fd, temp_path = tempfile.mkstemp(
        dir=str(parent_dir), prefix=f".{target_path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as tmp_file:
            json.dump(payload, tmp_file, indent=2, sort_keys=True)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(temp_path, target_path)

        dir_fd = os.open(str(parent_dir), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise


def load_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file missing: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    smtp_cfg = config.get("smtp", {})
    for key in ("host", "port", "username", "password", "sender_email"):
        if not smtp_cfg.get(key):
            raise ValueError(f"Missing required smtp.{key} in {config_path}")

    security = smtp_cfg.get("security", "starttls").lower()
    if security not in ("starttls", "ssl", "none"):
        raise ValueError("smtp.security must be 'starttls', 'ssl', or 'none'")

    to_emails = config.get("to_emails", [])
    if not isinstance(to_emails, list) or not to_emails:
        raise ValueError("'to_emails' must be a non-empty list in config.json")

    cc_emails = config.get("cc_emails", [])
    if not isinstance(cc_emails, list):
        raise ValueError("'cc_emails' must be a list in config.json")

    return config


def load_seen_courses(state_path: Path, logger: logging.Logger) -> Dict[str, Dict[str, str]]:
    if not state_path.exists():
        return {}

    try:
        with state_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        backup_path = state_path.with_suffix(state_path.suffix + ".bak")
        shutil.copy2(state_path, backup_path)
        logger.critical(
            "State file %s is corrupted! Backed up to %s and aborting to prevent duplicate alerts.",
            state_path,
            backup_path,
        )
        raise RuntimeError(f"Corrupted state file: {state_path}") from exc

    if isinstance(data, dict) and "courses" in data and isinstance(data["courses"], dict):
        return data["courses"]
    if isinstance(data, dict) and "seen_ids" in data and isinstance(data["seen_ids"], list):
        return {cid: {"status": "Legacy", "updated_at": ""} for cid in data["seen_ids"]}
    raise RuntimeError(f"Unexpected schema in state file: {state_path}")


def save_seen_courses(
    state_path: Path, courses_state: Dict[str, Dict[str, str]]
) -> None:
    atomic_write_json(state_path, {"courses": courses_state})


def update_health(
    success: bool, error_msg: str, config: Dict[str, Any], logger: logging.Logger
) -> None:
    """Track consecutive fetch/parse failures and email an admin alert at threshold."""
    health = {"consecutive_failures": 0, "last_error": "", "alert_sent": False}
    if HEALTH_PATH.exists():
        try:
            health.update(json.loads(HEALTH_PATH.read_text(encoding="utf-8")))
        except Exception:
            pass

    if success:
        if health["consecutive_failures"] > 0:
            atomic_write_json(
                HEALTH_PATH,
                {"consecutive_failures": 0, "last_error": "", "alert_sent": False},
            )
        return

    health["consecutive_failures"] += 1
    health["last_error"] = error_msg
    threshold = int(config.get("failure_alert_threshold", 3))

    if health["consecutive_failures"] >= threshold and not health.get("alert_sent"):
        try:
            send_email(
                subject=f"[MFRI Scraper ERROR] {health['consecutive_failures']} Consecutive Failures",
                body=(
                    f"The MFRI EMT schedule scraper has failed {health['consecutive_failures']} "
                    f"consecutive times.\n\nLatest Error:\n{error_msg}\n\n"
                    f"Please inspect {LOG_PATH} or run 'journalctl -u mfri-scraper.service'."
                ),
                config=config,
                logger=logger,
            )
            health["alert_sent"] = True
        except Exception as exc:
            logger.error("Failed to send health alert email: %s", exc)

    try:
        atomic_write_json(HEALTH_PATH, health)
    except Exception as exc:
        logger.error("Failed to write health file: %s", exc)


# ---------------------------------------------------------------------------
# HTTP Fetching & HTML Parsing
# ---------------------------------------------------------------------------
def fetch_html(
    test_mode: bool, mock_path: Path, config: Dict[str, Any], logger: logging.Logger
) -> str:
    if test_mode:
        logger.info("Running in LOCAL TEST mode using %s", mock_path)
        if not mock_path.exists():
            raise FileNotFoundError(f"Mock HTML file not found: {mock_path}")
        return mock_path.read_text(encoding="utf-8")

    headers = {
        "User-Agent": config.get(
            "user_agent", "MFRI-EMT-ScheduleMonitor/2.2 (RaspberryPi)"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))

    logger.info("Fetching live MFRI schedule from %s", TARGET_URL)
    response = session.get(TARGET_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def clean_more_arrow(raw_text: str) -> str:
    text = re.sub(r"More\s*(?:→|&rarr;|\u2192).*", "", raw_text, flags=re.IGNORECASE)
    return " ".join(text.split()).strip()


def is_initial_emt_course(block_text: str) -> bool:
    if EXCLUDED_CODE_RE.search(block_text):
        return False
    return bool(INITIAL_EMT_ID_RE.search(block_text))


def resolve_course_title(item_div: Tag) -> str:
    h2 = item_div.find_previous("h2")
    if h2:
        cleaned = clean_more_arrow(h2.get_text(" ", strip=True))
        if cleaned.lower().startswith("emergency medical technician"):
            return cleaned
    return "Emergency Medical Technician"


def extract_location(item_div: Tag) -> str:
    loc_link = item_div.find("a", href=re.compile(r"/location", re.IGNORECASE))
    if loc_link:
        container = loc_link.parent if loc_link.parent else loc_link
        txt = clean_more_arrow(container.get_text(" ", strip=True))
        if txt:
            return txt
    return "Location not specified"


def parse_mmddyyyy(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%m-%d-%Y").date()
    except ValueError:
        return None


def extract_registration_info(
    item_div: Tag,
    block_text: str,
    reg_open_str: Optional[str],
    reg_close_str: Optional[str],
    today: date,
) -> Dict[str, str]:
    reg_url = TARGET_URL
    link_label = ""

    for anchor in item_div.find_all("a", href=True):
        href = anchor["href"].strip()
        text_lower = " ".join(anchor.get_text().split()).lower()
        full_url = urljoin(TARGET_URL, href)

        if "/register/" in href.lower() or text_lower in ("register", "wait list"):
            reg_url = full_url
            link_label = text_lower
            break
        if "/course/" in href.lower() and reg_url == TARGET_URL:
            reg_url = full_url

    open_date = parse_mmddyyyy(reg_open_str)
    close_date = parse_mmddyyyy(reg_close_str)
    text_lower_all = block_text.lower()

    if "registration is closed" in text_lower_all or (close_date and today > close_date):
        status = "Closed"
    elif open_date and today < open_date:
        status = f"Upcoming (Opens {reg_open_str})"
    elif "wait list" in link_label or "wait list" in text_lower_all:
        status = "Wait List"
    elif link_label == "register":
        status = "Open"
    else:
        status = "Unknown"

    return {"registration_url": reg_url, "registration_status": status}


def parse_schedule_html(
    html_content: str, today: date, logger: logging.Logger
) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_content, "html.parser")
    event_items = soup.find_all(
        "div",
        class_=lambda c: c and "row" in c.split() and "event-item" in c.split(),
    )

    if not event_items:
        raise ValueError(
            "DOM check failed: 0 <div class='row event-item'> blocks found on page."
        )

    logger.info("Found %d event-item blocks on schedule page.", len(event_items))

    courses: List[Dict[str, str]] = []
    seen_in_parse = set()

    for item in event_items:
        block_text = item.get_text("\n", strip=True)
        if not is_initial_emt_course(block_text):
            continue

        id_match = INITIAL_EMT_ID_RE.search(block_text)
        if not id_match:
            continue

        course_id = id_match.group(1).upper()
        if course_id in seen_in_parse:
            continue
        seen_in_parse.add(course_id)

        start_match = START_DATE_RE.search(block_text) or SINGLE_DATE_RE.search(block_text)
        end_match = END_DATE_RE.search(block_text)
        time_match = CLASS_TIME_RE.search(block_text)
        reg_open_match = REG_OPEN_RE.search(block_text)
        reg_close_match = REG_CLOSE_RE.search(block_text)
        days_match = DAYS_RE.search(block_text)

        reg_open_str = reg_open_match.group(1) if reg_open_match else None
        reg_close_str = reg_close_match.group(1) if reg_close_match else None

        reg_info = extract_registration_info(
            item, block_text, reg_open_str, reg_close_str, today
        )

        courses.append(
            {
                "course_id": course_id,
                "title": resolve_course_title(item),
                "location": extract_location(item),
                "start_date": start_match.group(1) if start_match else "TBD",
                "end_date": end_match.group(1) if end_match else "TBD",
                "meeting_times": time_match.group(1) if time_match else "TBD",
                "meeting_days": days_match.group(1) if days_match else "TBD",
                "registration_open": reg_open_str or "TBD",
                "registration_deadline": reg_close_str or "TBD",
                "registration_status": reg_info["registration_status"],
                "registration_url": reg_info["registration_url"],
            }
        )

    logger.info("Filtered down to %d initial EMT (EMS-106) course(s).", len(courses))
    return courses


# ---------------------------------------------------------------------------
# Verified SMTP Delivery
# ---------------------------------------------------------------------------
def send_email(
    subject: str, body: str, config: Dict[str, Any], logger: logging.Logger
) -> None:
    smtp_cfg = config["smtp"]
    to_emails: List[str] = config["to_emails"]
    cc_emails: List[str] = config.get("cc_emails", [])
    all_recipients = list(dict.fromkeys(to_emails + cc_emails))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_cfg["sender_email"]
    msg["To"] = ", ".join(to_emails)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg.set_content(body)

    host = smtp_cfg["host"]
    port = int(smtp_cfg["port"])
    security = smtp_cfg.get("security", "starttls").lower()
    ssl_context = ssl.create_default_context()

    if security == "ssl":
        with smtplib.SMTP_SSL(
            host, port, timeout=REQUEST_TIMEOUT, context=ssl_context
        ) as server:
            server.login(smtp_cfg["username"], smtp_cfg["password"])
            refused = server.send_message(
                msg, from_addr=smtp_cfg["sender_email"], to_addrs=all_recipients
            )
    else:
        with smtplib.SMTP(host, port, timeout=REQUEST_TIMEOUT) as server:
            server.ehlo()
            if security == "starttls":
                server.starttls(context=ssl_context)
                server.ehlo()
            server.login(smtp_cfg["username"], smtp_cfg["password"])
            refused = server.send_message(
                msg, from_addr=smtp_cfg["sender_email"], to_addrs=all_recipients
            )

    if refused:
        logger.warning("Partial recipient refusal from SMTP server: %s", refused)


def send_course_alert(
    course: Dict[str, str],
    event_reason: str,
    config: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    subject = (
        f"[MFRI {event_reason}] {course['course_id']} - {course['location']} "
        f"({course['registration_status']})"
    )
    body = (
        f"MFRI Initial Emergency Medical Technician (EMS-106) Alert: {event_reason}\n\n"
        f"Course Details:\n"
        f"------------------------------------------------------------\n"
        f"Course Title          : {course['title']}\n"
        f"Course ID             : {course['course_id']}\n"
        f"Location              : {course['location']}\n"
        f"Start Date            : {course['start_date']}\n"
        f"End Date              : {course['end_date']}\n"
        f"Meeting Times         : {course['meeting_times']}\n"
        f"Meeting Days          : {course['meeting_days']}\n"
        f"Registration Open     : {course['registration_open']}\n"
        f"Registration Deadline : {course['registration_deadline']}\n"
        f"Registration Status   : {course['registration_status']}\n"
        f"Registration Link     : {course['registration_url']}\n"
        f"------------------------------------------------------------\n"
        f"Schedule URL          : {TARGET_URL}\n"
    )
    logger.info(
        "Sending '%s' email for %s (Status: %s)",
        event_reason,
        course["course_id"],
        course["registration_status"],
    )
    send_email(subject, body, config, logger)


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Monitor MFRI schedule for initial EMT (EMS-106) courses."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Parse local mock_schedule.html and use isolated seen_courses.test.json.",
    )
    parser.add_argument(
        "--mock-file",
        type=Path,
        default=DEFAULT_MOCK_PATH,
        help="Custom mock HTML path for --test mode.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log alerts without sending emails or modifying state.",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Seed seen_courses.json with currently posted courses without emailing.",
    )
    args = parser.parse_args()

    logger = setup_logging()

    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.warning("Another scraper instance is currently running; exiting.")
        return 0

    try:
        config = load_config(CONFIG_PATH)
    except Exception as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    state_path = TEST_STATE_PATH if args.test else PROD_STATE_PATH

    try:
        seen_courses = load_seen_courses(state_path, logger)
    except RuntimeError:
        return 3

    try:
        html_content = fetch_html(args.test, args.mock_file, config, logger)
        courses = parse_schedule_html(html_content, date.today(), logger)
        if not args.test:
            update_health(True, "", config, logger)
    except Exception as exc:
        logger.error("Fetch/Parse failure: %s", exc)
        if not args.test:
            update_health(False, str(exc), config, logger)
        return 2

    now_iso = datetime.now().isoformat(timespec="seconds")

    if args.baseline:
        for c in courses:
            seen_courses[c["course_id"]] = {
                "status": c["registration_status"],
                "updated_at": now_iso,
            }
        save_seen_courses(state_path, seen_courses)
        logger.info(
            "Baseline seeded %d course(s) into %s without sending emails.",
            len(courses),
            state_path.name,
        )
        return 0

    smtp_failures = 0

    for course in courses:
        cid = course["course_id"]
        curr_status = course["registration_status"]
        prev_entry = seen_courses.get(cid)

        event_reason: Optional[str] = None
        if prev_entry is None:
            event_reason = "NEW COURSE"
        elif prev_entry.get("status", "").startswith("Upcoming") and curr_status in (
            "Open",
            "Wait List",
        ):
            event_reason = "REGISTRATION OPENED"

        if not event_reason:
            if not args.dry_run and prev_entry.get("status") != curr_status:
                seen_courses[cid] = {"status": curr_status, "updated_at": now_iso}
                save_seen_courses(state_path, seen_courses)
            continue

        if args.dry_run:
            logger.info(
                "[DRY-RUN] Would email '%s' for %s (%s at %s)",
                event_reason,
                cid,
                curr_status,
                course["location"],
            )
            continue

        try:
            send_course_alert(course, event_reason, config, logger)
            seen_courses[cid] = {"status": curr_status, "updated_at": now_iso}
            save_seen_courses(state_path, seen_courses)
        except Exception as exc:
            smtp_failures += 1
            logger.error("SMTP failure for %s (%s). Will retry next run.", cid, exc)

    if smtp_failures > 0:
        return 1

    logger.info("Scraper run finished cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())