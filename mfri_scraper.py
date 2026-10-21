#!/usr/bin/env python3
"""
MFRI Initial EMT (EMS-106) Schedule Monitor
Stateless, cron-friendly scraper with atomic state persistence and SMTP alerts.
"""

import argparse
import json
import logging
import os
import re
import smtplib
import sys
import tempfile
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Constants & Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "seen_courses.json"
LOG_PATH = BASE_DIR / "scraper.log"
DEFAULT_MOCK_PATH = BASE_DIR / "mock_schedule.html"

TARGET_URL = "https://www.mfri.org/schedule/msfs/"
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

# Strict filtering patterns
INITIAL_EMT_ID_RE = re.compile(r"\b(EMS-106-[A-Z0-9]+-\d{4})\b", re.IGNORECASE)
EXCLUDED_CODE_RE = re.compile(r"\bEMS-(?:216|217|218|338|343)\b", re.IGNORECASE)
EXCLUDED_TITLE_KEYWORDS = (
    "recertification",
    "refresher",
    "continuing education",
    "skills",
    "proficiency",
    "online continuing",
)

# Field extraction patterns matching live MFRI text output
START_DATE_RE = re.compile(r"Start Date:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})", re.IGNORECASE)
END_DATE_RE = re.compile(r"End Date:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})", re.IGNORECASE)
SINGLE_DATE_RE = re.compile(r"\bDate:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})", re.IGNORECASE)
CLASS_TIME_RE = re.compile(
    r"(?:First Class Time|Time):\s*([0-9]{2}:[0-9]{2}\s*-\s*[0-9]{2}:[0-9]{2})",
    re.IGNORECASE,
)
REG_CLOSE_RE = re.compile(
    r"Registration Close:\s*([0-9]{2}-[0-9]{2}-[0-9]{4})", re.IGNORECASE
)
DAYS_RE = re.compile(
    r"\b((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?"
    r"(?:(?:,\s*|\s+and\s+)(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)s?)*)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("mfri_scraper")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


# ---------------------------------------------------------------------------
# Configuration & Atomic State Management
# ---------------------------------------------------------------------------
def load_config(config_path: Path, logger: logging.Logger) -> Dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file missing: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    smtp_cfg = config.get("smtp", {})
    required_smtp = ["host", "port", "username", "password", "sender_email"]
    for key in required_smtp:
        if key not in smtp_cfg or smtp_cfg[key] in ("", None):
            raise ValueError(f"Missing required smtp.{key} in {config_path}")

    to_emails = config.get("to_emails", [])
    if not isinstance(to_emails, list) or not to_emails:
        raise ValueError("'to_emails' must be a non-empty list in config.json")

    cc_emails = config.get("cc_emails", [])
    if not isinstance(cc_emails, list):
        raise ValueError("'cc_emails' must be a list in config.json")

    return config


def load_seen_courses(state_path: Path, logger: logging.Logger) -> Set[str]:
    if not state_path.exists():
        return set()
    try:
        with state_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "seen_ids" in data:
                return set(data["seen_ids"])
            if isinstance(data, list):
                return set(data)
            return set()
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Corrupted state file %s (%s). Starting fresh.", state_path, exc)
        return set()


def save_seen_courses_atomic(
    state_path: Path, seen_ids: Set[str], logger: logging.Logger
) -> None:
    """Write state to a temp file, fsync, and atomically replace target file."""
    payload = {"seen_ids": sorted(seen_ids)}
    temp_fd, temp_path = tempfile.mkstemp(
        dir=str(state_path.parent), prefix=".seen_courses_", suffix=".tmp"
    )
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as tmp_file:
            json.dump(payload, tmp_file, indent=2)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(temp_path, state_path)
    except Exception as exc:
        logger.error("Atomic write to %s failed: %s", state_path, exc)
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise


# ---------------------------------------------------------------------------
# Fetching & HTML Parsing
# ---------------------------------------------------------------------------
def fetch_html(
    test_mode: bool, mock_path: Path, logger: logging.Logger
) -> Optional[str]:
    if test_mode:
        logger.info("Running in LOCAL TEST mode using %s", mock_path)
        if not mock_path.exists():
            logger.error("Mock HTML file not found at %s", mock_path)
            return None
        return mock_path.read_text(encoding="utf-8")

    logger.info("Fetching live MFRI schedule from %s", TARGET_URL)
    try:
        response = requests.get(
            TARGET_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as exc:
        logger.error("Network error while fetching MFRI schedule: %s", exc)
        return None


def clean_title_text(raw_text: str) -> str:
    text = re.sub(r"More\s*(?:→|&rarr;|\u2192).*", "", raw_text, flags=re.IGNORECASE)
    return " ".join(text.split()).strip()


def resolve_course_title(item_div: Tag) -> str:
    """
    Locate the course title either inside the event-item div or from the nearest
    preceding heading/section header on the MFRI schedule page.
    """
    for tag_name in ("h2", "h3", "h4"):
        internal_heading = item_div.find(tag_name)
        if internal_heading:
            return clean_title_text(internal_heading.get_text(" ", strip=True))

    prev_heading = item_div.find_previous(["h2", "h3", "h4"])
    if prev_heading:
        return clean_title_text(prev_heading.get_text(" ", strip=True))

    return ""


def is_initial_emt_course(course_title: str, block_text: str) -> bool:
    """
    Strictly allow only initial Emergency Medical Technician (EMS-106) courses.
    Explicitly reject EMS-216, EMS-217, EMS-218, refreshers, recerts, and CEUs.
    """
    if EXCLUDED_CODE_RE.search(block_text):
        return False

    if not INITIAL_EMT_ID_RE.search(block_text):
        return False

    combined_lower = f"{course_title} {block_text}".lower()
    for keyword in EXCLUDED_TITLE_KEYWORDS:
        if keyword in course_title.lower():
            return False

    if course_title:
        normalized_title = course_title.strip().lower()
        if normalized_title != "emergency medical technician":
            return False
    else:
        if "emergency medical technician" not in combined_lower:
            return False

    return True


def extract_location(item_div: Tag, lines: List[str]) -> str:
    """
    Extract facility location. On MFRI schedules, the location appears right after
    'Registration Close:' and before 'Coordinated by:'.
    """
    for idx, line in enumerate(lines):
        if line.lower().startswith("registration close:"):
            for candidate in lines[idx + 1 : idx + 4]:
                cleaned = clean_title_text(candidate)
                if cleaned and not cleaned.lower().startswith("coordinated by"):
                    return cleaned

    for idx, line in enumerate(lines):
        if line.lower().startswith("coordinated by:") and idx > 0:
            cleaned = clean_title_text(lines[idx - 1])
            if cleaned and not cleaned.lower().startswith("registration"):
                return cleaned

    return "Location not specified"


def extract_registration_url(item_div: Tag) -> str:
    """Extract the direct registration URL or fallback to the course details URL."""
    fallback_url = TARGET_URL
    for anchor in item_div.find_all("a", href=True):
        link_text = " ".join(anchor.get_text().split()).lower()
        href = anchor["href"].strip()
        full_url = urljoin(TARGET_URL, href)

        if "register" in link_text and "closed" not in link_text:
            return full_url
        if "more" in link_text and "/course/" in href:
            fallback_url = full_url

    return fallback_url


def parse_emt_courses(html_content: str, logger: logging.Logger) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html_content, "html.parser")
    event_items = soup.find_all(
        "div",
        class_=lambda c: c and "row" in c.split() and "event-item" in c.split(),
    )
    logger.info("Found %d <div class='row event-item'> blocks.", len(event_items))

    courses: List[Dict[str, str]] = []
    seen_in_parse: Set[str] = set()

    for item in event_items:
        block_text = item.get_text("\n", strip=True)
        course_title = resolve_course_title(item)

        if not is_initial_emt_course(course_title, block_text):
            continue

        id_match = INITIAL_EMT_ID_RE.search(block_text)
        if not id_match:
            continue

        course_id = id_match.group(1).upper()
        if course_id in seen_in_parse:
            continue
        seen_in_parse.add(course_id)

        lines = [
            " ".join(line.split())
            for line in block_text.splitlines()
            if line.strip()
        ]

        start_match = START_DATE_RE.search(block_text) or SINGLE_DATE_RE.search(block_text)
        end_match = END_DATE_RE.search(block_text)
        time_match = CLASS_TIME_RE.search(block_text)
        reg_close_match = REG_CLOSE_RE.search(block_text)
        days_match = DAYS_RE.search(block_text)

        reg_closed = "registration is closed" in block_text.lower()
        reg_url = extract_registration_url(item)

        courses.append(
            {
                "course_id": course_id,
                "title": course_title or "Emergency Medical Technician",
                "location": extract_location(item, lines),
                "start_date": start_match.group(1) if start_match else "TBD",
                "end_date": end_match.group(1) if end_match else "TBD",
                "meeting_times": time_match.group(1) if time_match else "TBD",
                "meeting_days": days_match.group(1) if days_match else "TBD",
                "registration_deadline": (
                    reg_close_match.group(1) if reg_close_match else "TBD"
                ),
                "registration_status": "Closed" if reg_closed else "Open",
                "registration_url": reg_url,
            }
        )

    logger.info("Filtered down to %d initial EMT (EMS-106) course(s).", len(courses))
    return courses


# ---------------------------------------------------------------------------
# Email Notification
# ---------------------------------------------------------------------------
def send_email_alert(
    course: Dict[str, str], config: Dict, logger: logging.Logger
) -> None:
    smtp_cfg = config["smtp"]
    to_emails: List[str] = config["to_emails"]
    cc_emails: List[str] = config.get("cc_emails", [])

    # Deduplicate envelope recipients while preserving header lists
    all_recipients = list(dict.fromkeys(to_emails + cc_emails))

    msg = EmailMessage()
    msg["Subject"] = (
        f"[MFRI Alert] New Initial EMT Course Posted: {course['course_id']} "
        f"({course['location']})"
    )
    msg["From"] = smtp_cfg["sender_email"]
    msg["To"] = ", ".join(to_emails)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)

    body = (
        f"A newly posted Initial Emergency Medical Technician (EMS-106) course "
        f"was detected on the MFRI schedule.\n\n"
        f"Course Details:\n"
        f"------------------------------------------------------------\n"
        f"Course Title          : {course['title']}\n"
        f"Course ID             : {course['course_id']}\n"
        f"Location              : {course['location']}\n"
        f"Start Date            : {course['start_date']}\n"
        f"End Date              : {course['end_date']}\n"
        f"Meeting Times         : {course['meeting_times']}\n"
        f"Meeting Days          : {course['meeting_days']}\n"
        f"Registration Deadline : {course['registration_deadline']}\n"
        f"Registration Status   : {course['registration_status']}\n"
        f"Registration Link     : {course['registration_url']}\n"
        f"------------------------------------------------------------\n"
        f"Full Schedule Page    : {TARGET_URL}\n"
    )
    msg.set_content(body)

    host = smtp_cfg["host"]
    port = int(smtp_cfg["port"])
    use_tls = bool(smtp_cfg.get("use_tls", True))

    logger.info(
        "Sending email alert for %s (To: %s | Cc: %s)",
        course["course_id"],
        to_emails,
        cc_emails,
    )

    if port == 465 and not use_tls:
        with smtplib.SMTP_SSL(host, port, timeout=REQUEST_TIMEOUT) as server:
            server.login(smtp_cfg["username"], smtp_cfg["password"])
            server.send_message(msg, from_addr=smtp_cfg["sender_email"], to_addrs=all_recipients)
    else:
        with smtplib.SMTP(host, port, timeout=REQUEST_TIMEOUT) as server:
            server.ehlo()
            if use_tls:
                server.starttls()
                server.ehlo()
            server.login(smtp_cfg["username"], smtp_cfg["password"])
            server.send_message(msg, from_addr=smtp_cfg["sender_email"], to_addrs=all_recipients)

    logger.info("Successfully delivered alert for %s.", course["course_id"])


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Monitor MFRI schedule for new initial EMT (EMS-106) courses."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Parse local mock_schedule.html instead of querying live MFRI website.",
    )
    parser.add_argument(
        "--mock-file",
        type=Path,
        default=DEFAULT_MOCK_PATH,
        help="Path to custom mock HTML file when using --test.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log new courses without sending SMTP emails.",
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Starting MFRI EMT scraper run.")

    try:
        config = load_config(CONFIG_PATH, logger)
    except Exception as exc:
        logger.error("Configuration error: %s", exc)
        return 1

    seen_ids = load_seen_courses(STATE_PATH, logger)
    html_content = fetch_html(args.test, args.mock_file, logger)
    if not html_content:
        logger.warning("No HTML content retrieved; exiting cleanly without state mutation.")
        return 0

    try:
        courses = parse_emt_courses(html_content, logger)
    except Exception as exc:
        logger.exception("Unexpected error parsing HTML: %s", exc)
        return 1

    new_courses = [c for c in courses if c["course_id"] not in seen_ids]
    if not new_courses:
        logger.info("No new initial EMT (EMS-106) courses found.")
        return 0

    state_updated = False
    for course in new_courses:
        cid = course["course_id"]
        logger.info(
            "NEW COURSE DETECTED: %s at %s (%s to %s)",
            cid,
            course["location"],
            course["start_date"],
            course["end_date"],
        )
        if args.dry_run:
            logger.info("[DRY-RUN] Skipping email delivery for %s.", cid)
            seen_ids.add(cid)
            state_updated = True
            continue

        try:
            send_email_alert(course, config, logger)
            seen_ids.add(cid)
            state_updated = True
        except Exception as exc:
            logger.error(
                "SMTP failure for %s (%s). Will retry on next cron run.", cid, exc
            )

    if state_updated:
        try:
            save_seen_courses_atomic(STATE_PATH, seen_ids, logger)
            logger.info("Updated seen_courses.json with %d total course(s).", len(seen_ids))
        except Exception:
            return 1

    logger.info("MFRI EMT scraper run completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())