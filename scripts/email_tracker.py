#!/usr/bin/env python3
"""Application tracker built from Gmail: confirmations, rejections, interviews, assessments, offers.

    python scripts/email_tracker.py            # sync new emails, print the tracker
    python scripts/email_tracker.py --reparse  # re-read stored emails with the current parsers

Reads GMAIL_ADDRESS and GMAIL_APP_PASSWORD from .env (a Google app password, not the account
password). Only new emails are fetched after the first run. State lives in applications/tracker.sqlite;
tailored-resume rows from applications/applications.xlsx are merged in by company and role.
"""

from __future__ import annotations

import email
import html
import imaplib
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_DIR = ROOT / "applications"
DB_PATH = APP_DIR / "tracker.sqlite"
XLSX = APP_DIR / "applications.xlsx"
_IMAP_MON = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

QUERY = (
    "-category:social "
    "-from:substack.com -from:medium.com -from:wellfound.com -from:joinrunway.io "
    "(application OR applying OR applied OR interview OR assessment OR candidacy OR unfortunately "
    'OR "next steps" OR "your candidacy" OR "move forward" OR offer OR "coding challenge" '
    'OR criteriacorp OR hackerrank OR codesignal OR "not selected" OR "invited you" '
    'OR "has been filled" OR "job opening update" OR "status update")'
)

# Not job applications even though they say "application".
IGNORE_FROM = re.compile(r"hackwestern|mlh\.io|devpost|substack|medium\.com|wellfound|joinrunway|"
                         r"jobalerts\.com|job-alerts|jobs-listings|glassdoor|indeed\.com|ziprecruiter|"
                         r"redditmail|beehiiv|arjaythedev|quora|youtube|webull|nepasrepondre|e-sin", re.I)
# A real application email talks about the user's own application.
JOB_CONTEXT = re.compile(r"your (application|candidacy|submission|interest|profile)|applying|applied|"
                         r"application (for|to|with)|the (position|role)|thank(s| you) for (your )?interest|"
                         r"recruit|assessment|interview with|candidature|postul|"
                         r"ứng tuyển|gửi hồ sơ|đã nhận hồ sơ|hồ sơ của bạn", re.I)
IGNORE_SUBJECT = re.compile(r"hackathon|new jobs:|jobs for you|posted a role|recommended jobs|"
                            r"job alert|newsletter|digest|webinar|"
                            r"verification code|otp code|one-time code|"
                            r"careers account|options trading|demande re[cç]ue|"
                            r"employer brand survey|brand survey", re.I)
JUNK_COMPANY = re.compile(
    r"verification code|demande re[cç]ue|^\s*(gmail|unknown|webull|recent|action required)\s*$", re.I)
XLSX_SKIP_COMPANY = re.compile(
    r"unknown\b|via indeed|ziprecruiter|^recent$|^action required$|^gmail$|"
    r"verification code|demande re[cç]ue|webull", re.I)

STATUS_ORDER = ["Applied", "Action needed", "Assessment", "Interview", "Offer", "Rejected", "Withdrawn"]
STATUS_RULES = [
    ("Offer", re.compile(
        r"pleased to offer|offer letter|extend (you )?an offer|offer of employment|"
        r"thư mời nhận việc|đề nghị nhận việc", re.I)),
    ("Rejected", re.compile(
        r"unfortunately|sorry to let you know|"
        r"not (to )?(be )?mov(e|ing) (you |your application )?forward|"
        r"did not see a (great |good )?fit|not (a )?(great |good |right )?fit|"
        r"decided to (move|go) (forward|ahead) with other|"
        r"decided to (move|go) (forward|ahead) with candidates|"
        r"mov(e|ing) forward with other|mov(e|ing) forward with candidates whose|"
        r"decided to (proceed|pursue) with other|pursue other candidates|other candidates whose|"
        r"no longer (under consideration|being considered)|regret to inform|not been selected|"
        r"were not selected|position has been filled|will not be proceeding|decided not to proceed|"
        r"(role|position) has (now )?been filled|"
        r"progress with other candidates|proceed with other candidates|"
        r"other applicants whose|not a match at this time|"
        r"unable to move (your application|you) forward|"
        r"unable to offer you (this |the )?(position|role)|"
        r"does not meet the (basic |minimum )?qualifications|after careful (review|consideration).{0,80}"
        r"we have (decided|determined)|not be (considered|progressing)|won.t be moving forward|"
        r"application (is )?no longer active|assessments have expired|"
        r"rất tiếc|tiếc là|gửi hồ sơ không thành công|"
        r"không được .{0,40}gửi tới nhà tuyển dụng|không phù hợp|"
        r"ứng viên khác|vị trí đã (được tuyển|đóng|khép)", re.I)),
    ("Interview", re.compile(
        r"invite you to (an? )?(interview|chat|call)|schedule (an? |your )?(interview|call|chat)|"
        r"interview invitation|video interview|phone screen|next round|your availability|"
        r"would like to (speak|interview|chat)|book (a|your) time|"
        r"mời .{0,30}phỏng vấn|lịch phỏng vấn|tham gia phỏng vấn|phỏng vấn trực tuyến", re.I)),
    ("Assessment", re.compile(
        r"online assessment|coding (challenge|assessment)|hackerrank|codesignal|codility|"
        r"criteriacorp|ccat|"
        r"technical assessment|take-home|complete (the|your|an) assessment|"
        r"invited you to take an? assessment|take an? assessment|"
        r"bài (đánh giá|kiểm tra|test)|làm bài assessment", re.I)),
    ("Action needed", re.compile(
        r"final step to complete|complete your application|complete (your|the) prescreen|"
        r"confirm your .{0,40}email|action required|verify your email", re.I)),
    ("Applied", re.compile(
        r"thank(s| you) for (applying|your application|your interest|submitting|taking)|"
        r"next steps for your application|first step .{0,40}(career|appl)|"
        r"received your (job )?application|application (has been )?received|we have received|"
        r"application (was )?(sent|submitted)|successfully submitted|"
        r"keep track of your application|we.ve received your|"
        r"cảm ơn bạn đã (gửi hồ sơ|ứng tuyển|quan tâm)|đã nhận hồ sơ|"
        r"gửi hồ sơ thành công|đã gửi hồ sơ của bạn|chờ xét duyệt", re.I)),
]

# Words that only describe the sender's department or tool, never the company.
SENDER_NOISE = re.compile(
    r"\b(the recruiting team at|careers?|recruiting( team)?|recruitment|talent acquisition( team)?|"
    r"talent attraction|talent|workday( notifications)?|notifications?|human resources|hr|hiring team|"
    r"university (recruiting|relations)|early careers?|team|autoreply|auto-?reply|no-?reply|do-?not-?reply|"
    r"donotreply|system|assessments?|people|jobs?|mail|noreply|@ ?icims|via \w+)\b|[-|:]", re.I)
ATS_HOSTS = {"greenhouse", "greenhouse-mail", "lever", "myworkday", "workday", "icims", "successfactors",
             "ashbyhq", "smartrecruiters", "rippling", "jobvite", "taleo", "oraclecloud", "eightfold",
             "myinterview", "vssend", "candidates", "hire", "ats", "talent", "email", "mail", "us", "e",
             "careers", "jobs", "com", "co", "io", "net", "org", "ca", "recruiting", "notifications", "hi"}
GENERIC_LOCAL = re.compile(r"^(no-?reply|do-?not-?reply|donotreply|careers?|jobs?|recruiting|talent|hr|"
                           r"notifications?|apply|team|info|hello|globalhr|system|.*workdaynotifications)$", re.I)
ROLE_WORDS = re.compile(r"\b(intern(ship)?|engineer(ing)?|developer|software|co-?op|position|req|summer|"
                        r"student|program|role|opportunit|analyst|\d{4})\b", re.I)

COMPANY_FROM_SUBJECT = [
    r"vị trí .+? tại (.+?)(?:!|\.|$)",
    r"^([^|:]{2,40}?)\s*\|\s+",                                     # "SPS Commerce | Update to your application"
    r"^([^-–:]{2,40}?)\s+[-–]\s+(?:thank|your |update|application)",  # "CNH - Thank you for ..."
    r"^([^:]{2,40}): ",                                            # "Astranis: Thank You for ..."
    r"(?:applying|application|apply|applied) (?:to|at|with) (?:the )?(.+?)(?:!|\.|$| - | for | \(|,|/ )",
    r"interest in (?:joining )?(.+?)(?:!|\.|,|$| - |/ )",
    r"thank you from (.+?)(?:!|\.|$)",
    r"(?:reminder|message|invitation|update) from (.+?)(?:!|\.|$| - )",
    r"^your (.+?) application(?: status)?$",
    r"^(.+?) application (?:status|update)",
    r"^(.+?) internship update",
    r"update on .+? - (.+?)$",
    r"(?:for|to) .+? at (.+?)(?:!|\.|$)",
    r"^(.+?) application:",
]
# CodeSignal / HackerRank send as themselves; the employer is in the subject or body.
ASSESS_SENDER = re.compile(r"codesignal|hackerrank|codility|criteriacorp", re.I)
ASSESS_COMPANY = [
    r"^([A-Z][\w&.'’, -]{1,40}?) (?:has )?invited you",
    r"invited by ([A-Z][\w&.'’, -]{1,40}?) to take",
    r"invited by someone from ([A-Z][\w&.'’ -]{1,40}?)(?:\s+to\b|$)",
    r"shared with ([A-Z][\w&.'’ -]{1,40}?)(?:\s+soon|\.|$)",
    r"sent from ([A-Z][\w&.'’ -]{1,40}?) through",
    r"(?:completed|take)[: ]+([A-Z][\w&.'’ -]{1,40}?) \d{4} coding",
]
ALIASES = {
    "spscommerce": "SPS Commerce", "sunlife": "Sun Life", "slf": "Sun Life", "lseg": "LSEG",
    "londonstockexchangegroup": "LSEG", "epicgames": "Epic Games", "lockheedmartin": "Lockheed Martin",
    "lockheedmartincorp": "Lockheed Martin", "knowbe4": "KnowBe4", "cnh": "CNH",
    "capitalone": "Capital One", "ngc": "Northrop Grumman", "lnw": "Light & Wonder", "cae": "CAE",
    "mckesson": "McKesson", "motorolasolutions": "Motorola Solutions", "khanacademy": "Khan Academy",
    "rfsmart": "RF-SMART", "onxcached": "onX", "andurilindustries": "Anduril", "amd": "AMD",
    "ibm": "IBM", "docusign": "Docusign", "thesemiosgroup": "Semios",
    "manulifejohnhancock": "Manulife", "johnhancock": "Manulife",
    "jpmorganchase": "JPMorgan Chase", "jpmorganchaseco": "JPMorgan Chase",
    "jpmc": "JPMorgan Chase", "jpmorgan": "JPMorgan Chase",
    "compasscanada": "Compass Group", "compassgroupcanadaltd": "Compass Group",
    "compassgroup": "Compass Group", "compassgroupcanada": "Compass Group",
    "knowbe4inc": "KnowBe4", "l3harris": "L3Harris", "l3harrissuccessfactors": "L3Harris",
    "spacexsi": "SpaceX SI", "thalesgroup": "Thales",
    "lightandwonder": "Light & Wonder", "lightwonder": "Light & Wonder",
}
# Capitalized company names in the body ("career with Northrop Grumman", "joining Electronic Arts").
COMPANY_FROM_BODY = re.compile(
    r"(?:interest in|career with|careers? at|joining|position at|role at|opportunity at|applying to|"
    r"application to|applied to|work(?:ing)? at|chez|team at|[Pp]rograms at|"
    r"vị trí .+? tại|ứng tuyển .{0,40}? tại|đến) (?:a career with |joining )?"
    r"((?:[A-Z][\w&.'’-]*)(?: (?:&|of|and|[A-Z][\w&.'’-]*)){0,3})")
ROLE_FROM_TEXT = [
    r"vị trí (.+?) tại",
    r"cho vị trí (.+?)(?: tại|!|\.|$)",
    r"status update - r\d+ (.+)$",
    r"(?:job opening|application) update\s*[-–:]\s*(.+)$",
    r"application: [\d-]+ - (.+)$",
    r"with .+? - (.+?) \(\d+\)",
    r"we.ve received your application! (.+?) - [A-Z]{1,4}\d+",
    r"interest in .+? and the (.+?) position",
    r"successfully applied for (?:the |our )?(.+?) position",
    r"applied for (?:the |our )?(.+?) position",
    r"application for (?:the |our )?(.+?) has been received",
    r"applying to the (.+?) role",
    r"applying to the (.+?) position",
    r"update on (.+?) - ",
    r"(?:application|applying|interest|submitting|applied)[^.!\n]{0,40}? (?:for|to|in) (?:the |our |a |be a )?"
    r"(?:position of |role of |position |role )?([^.!\n]+?(?:intern|internship|co-?op|stagiaire|student|thực tập)\b[^.!\n]*?)"
    r"(?: position| role| job| at |!|\.|$| - | opportunity)",
    r"for (?:the )?([^.!\n]+?(?:intern|internship|co-?op)\b[^.!\n]*?)(?: position| role| at |!|\.|$| - )",
]
ROLE_JUNK = [
    r"^(?:your |our )?(?:application|interest|submission)(?: and interest)? (?:for|to|in) (?:the )?",
    r"^(?:submitting your application to|applying (?:to|for)|expressing interest in)(?: be a| the| our)? ",
    r"^(?:the )?(?:position|role) of ", r"^(?:the|our|a) ",
    r"^.*\b(?:external )?career site:\s*",
    r"^us internships public careers site for the ",
    r"^[\w&.''’ -]{1,40} and the ",
    r"^[\w&.''’ -]{1,30}'s\s+",
    r"\(?\b(?:req(?:uisition)? ?id:? ?)?(?:R|JR|REQ)?\d{5,}\b\)?", r"\bjob was submitted.*$", r"&nbsp;",
    r"[\u200b\ufeff]", r" opportunity$", r" position$", r" - JR\w+$", r"^open position \(?\s*", r" here$",
    r"\s*,\s+and\b.*$",
    r" has been received\b.*$", r" and we will\b.*$",
    r"^job consideration.*$", r"^of ", r"\s+role$",
]
ROLE_INVALID = re.compile(r"\b(your|thank|hung|and for|taking the time|apply|applying)\b", re.I)


def load_env() -> dict[str, str]:
    env = {}
    for line in (ROOT / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def connect_db() -> sqlite3.Connection:
    APP_DIR.mkdir(exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS emails (uid INTEGER PRIMARY KEY, date TEXT, sender TEXT,
                  subject TEXT, company TEXT, role TEXT, status TEXT, snippet TEXT)""")
    db.execute("CREATE TABLE IF NOT EXISTS overrides (key TEXT PRIMARY KEY, status TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS read_uids (uid INTEGER PRIMARY KEY)")
    return db


def _hdr(msg: email.message.Message, name: str) -> str:
    return re.sub(r"\s+", " ", str(make_header(decode_header(msg.get(name, ""))))).strip()


def _body_text(msg: email.message.Message, limit: int = 6000) -> str:
    plain, rich = "", ""
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype not in ("text/plain", "text/html") or part.get_filename():
            continue
        try:
            text = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", "replace")
        except (AttributeError, LookupError):
            continue
        if ctype == "text/plain" and not plain:
            plain = text
        elif ctype == "text/html" and not rich:
            rich = re.sub(r"(?is)<(script|style).*?</\1>", " ", text)
            rich = html.unescape(re.sub(r"<[^>]+>", " ", rich))
    text = html.unescape(plain or rich)
    if "<" in text and ">" in text:
        text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return re.sub(r"\s+", " ", text)[:limit]


HYPOTHETICAL = re.compile(r"\b(if|or|whether|unless|should|in case|may|might)\b[^.]{0,60}$", re.I)
NOT_A_REJECTION = re.compile(r"unfortunately,? (we|due to)[^.]{0,30}(unable|can.?t|cannot|not able) to "
                             r"(respond|reply|provide feedback|personally)", re.I)
# Workday receipts paste a status glossary ("Not Selected … We will not be moving forward").
STATUS_GLOSSARY = re.compile(r"application status(es)?|what does that mean|status what does", re.I)
RECEIPT_SUBJ = re.compile(
    r"thank(s| you) for (applying|your application|your interest)|"
    r"received your application|application (has been )?received|"
    r"we have received|we.?ve received your|"
    r"đã nhận hồ sơ|cảm ơn bạn đã (ứng tuyển|gửi hồ sơ)", re.I)
COMPLETE_APP = re.compile(
    r"complete your .{0,60}application|submit this (airtable )?form|"
    r"candidate home account pending|verify your candidate account", re.I)


def _matches(rx: re.Pattern, text: str) -> bool:
    """A match that isn't hedged ("if you are not selected") or a status-legend definition."""
    for m in rx.finditer(text):
        before = text[max(0, m.start() - 80):m.start()]
        window = text[max(0, m.start() - 200):m.start()]
        if (HYPOTHETICAL.search(before) or STATUS_GLOSSARY.search(window)
                or NOT_A_REJECTION.search(text[max(0, m.start() - 5):m.end() + 60])):
            continue
        return True
    return False


def classify(subject: str, body: str) -> str | None:
    text = f"{subject} . {body}"
    if not JOB_CONTEXT.search(text):
        return None
    if RECEIPT_SUBJ.search(subject):
        # Receipts often include a status glossary or "if selected" hedges. Only a rejection
        # in the opening (e.g. Anduril's thank-you that is actually a ding) overrides them.
        return "Rejected" if _matches(STATUS_RULES[1][1], body[:700]) else "Applied"
    # Subject is "Complete your application" / Airtable — not a ding, even if the footer
    # later says "if you are not selected".
    if COMPLETE_APP.search(subject):
        return "Action needed"
    if COMPLETE_APP.search(text) and not _matches(STATUS_RULES[1][1], body[:400]):
        return "Action needed"
    for status, rx in STATUS_RULES:
        if _matches(rx, text):
            return status
    return None


_TOOL_WORDS = r"Workday|Notification|DoNotReply|Careers|Recruiting|System|HR"


def _alias_company(name: str) -> str:
    return ALIASES.get(re.sub(r"[^a-z0-9]", "", (name or "").lower()), name)


def _clean_company(name: str) -> str:
    # "CapitalOneHRWorkday" / "WorkdaySystemDoNotReply": split only names glued to tool words, so
    # brands like DoorDash, onX and KnowBe4 stay intact.
    if re.search(_TOOL_WORDS, name):
        name = re.sub(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", name)
    name = re.sub(r"\b\w*workday\w*\b|\b\w*notifications?\b|\bdo ?not ?reply\b|\bno ?reply\b|"
                  r"\bsuccessfactors\b|\b(cached|hr|system)\b",
                  " ", name, flags=re.I)
    name = re.split(r"\s*/\s*", name)[0]  # "Coveo / Merci d'avoir appliqué"
    name = re.sub(r"\s*@\s*icims\b.*", "", name, flags=re.I)
    name = re.split(r"\.\s", name)[0]  # "Northrop Grumman. After careful review"
    name = re.sub(r"[^\w\s&.'’+-]+", " ", name)  # emoji, leftover punctuation
    name = re.sub(r"\s+", " ", name).strip(" !.,-|:\"'’()")
    name = re.sub(r"^(the|a)\s+", "", name, flags=re.I)[:60]
    return _alias_company(name)


def _ok_company(name: str) -> bool:
    return (1 < len(name) <= 45 and not ROLE_WORDS.search(name)
            and not JUNK_COMPANY.search(name)
            and not re.search(r"\b(your|our|this|you|we|us|hung|application|thank|invitation|reminder|"
                              r"update|important|action|required|confirm|welcome|status|received|next|"
                              r"steps|re|fwd|assessment|completed|codesignal|hackerrank|indeed|support|"
                              r"jobs?|verification|internships)\b",
                              name, re.I))


def company_of(sender: str, subject: str, body: str = "") -> str:
    if ASSESS_SENDER.search(sender):
        blob = f"{subject} {body[:800]}"
        for pat in ASSESS_COMPANY:
            m = re.search(pat, blob, re.I)
            if not m:
                continue
            name = _clean_company(m.group(1))
            if _ok_company(name):
                return name
    for pat in COMPANY_FROM_SUBJECT:
        for m in re.finditer(pat, subject, re.I):
            name = _clean_company(m.group(1))
            if _ok_company(name):
                return name
    display, addr = parseaddr(sender)
    name = _clean_company(SENDER_NOISE.sub(" ", display))
    if _ok_company(name):
        return name
    for m in COMPANY_FROM_BODY.finditer(body[:1500]):
        name = _clean_company(m.group(1))
        if _ok_company(name) and name.lower() not in ("our", "the", "a", "an"):
            return name
    local, _, domain = addr.lower().partition("@")
    # expedia@myworkday.com, careers@lockheedmartin.vssend.com, noreply@mail.amazon.jobs
    if not GENERIC_LOCAL.match(local) and re.search(r"workday|icims|vssend", domain):
        return _alias_company(local.title())
    core = [p for p in domain.split(".") if p not in ATS_HOSTS]
    raw = core[0].title() if core else (local.title() if not GENERIC_LOCAL.match(local) else "Unknown")
    return _alias_company(raw)


def _clean_role(role: str) -> str:
    role = re.sub(r"\s+", " ", role or "").strip(" -–!.,:")
    for _ in range(2):
        for junk in ROLE_JUNK:
            role = re.sub(junk, " ", role, flags=re.I).strip(" -–!.,:")
        role = re.split(r"\s*,\s+and\b", role, maxsplit=1)[0].strip(" -–!.,:")
    role = re.sub(r"\(\s*$|^\s*\)", "", role).strip()
    while role.endswith(")") and role.count(")") > role.count("("):
        role = role[:-1].strip()
    if role.count("(") > role.count(")"):
        role += ")"
    return role


def role_of(subject: str, body: str) -> str:
    for text in (subject, body[:1500]):
        for pat in ROLE_FROM_TEXT:
            m = re.search(pat, text, re.I)
            if not m:
                continue
            role = _clean_role(m.group(1))
            if 3 < len(role) < 90 and not ROLE_INVALID.search(role):
                return role
    return ""


_MY_EMAIL = ""


def _my_email() -> str:
    global _MY_EMAIL
    if not _MY_EMAIL:
        _MY_EMAIL = load_env().get("GMAIL_ADDRESS", "").lower()
    return _MY_EMAIL


def _ingest_message(db: sqlite3.Connection, uid: int, msg: email.message.Message) -> bool:
    sender, subject = _hdr(msg, "From"), _hdr(msg, "Subject")
    _, from_addr = parseaddr(sender)
    me = _my_email()
    if ((me and from_addr.lower() == me)
            or IGNORE_FROM.search(sender) or IGNORE_SUBJECT.search(subject)):
        return False
    body = _body_text(msg)
    status = classify(subject, body)
    if not status:
        return False
    company, role = company_of(sender, subject, body), role_of(subject, body)
    if not company or JUNK_COMPANY.search(company):
        return False
    try:
        when = parsedate_to_datetime(msg["Date"]).astimezone(timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    db.execute("INSERT OR REPLACE INTO emails VALUES (?,?,?,?,?,?,?,?)",
               (uid, when, sender, subject, company, role, status, body[:300]))
    return True


def _imap() -> imaplib.IMAP4_SSL:
    env = load_env()
    imap = imaplib.IMAP4_SSL("imap.gmail.com", timeout=180)
    imap.login(env["GMAIL_ADDRESS"], env["GMAIL_APP_PASSWORD"].replace(" ", ""))
    imap.select('"[Gmail]/All Mail"', readonly=True)
    return imap


def _uid_search(imap: imaplib.IMAP4_SSL, *args: str) -> list[int]:
    typ, data = imap.uid("SEARCH", *args)
    if typ != "OK" or not data or not data[0]:
        return []
    return [int(u) for u in data[0].split()]


def _parse_uid_fetch(data: list) -> list[int]:
    uids: list[int] = []
    for item in data or []:
        raw = item[0] if isinstance(item, tuple) else item
        if not raw or raw == b")":
            continue
        blob = raw if isinstance(raw, bytes) else str(raw).encode()
        m = re.search(rb"UID (\d+)", blob)
        if m:
            uids.append(int(m.group(1)))
    return uids


def _uids_after(imap: imaplib.IMAP4_SSL, last_uid: int) -> list[int]:
    """Every UID strictly after last_uid.

    `UID SEARCH 3866:*` is a *sequence* range (and is empty if the mailbox has fewer than 3866
    messages). `UID SEARCH UID 3866:*` and `UID FETCH 3866:* (UID)` are actual UID ranges.
    """
    start = f"{last_uid + 1}:*"
    uids = {u for u in _uid_search(imap, "UID", start) if u > last_uid}
    typ, data = imap.uid("FETCH", start, "(UID)")
    if typ == "OK":
        uids.update(u for u in _parse_uid_fetch(data) if u > last_uid)
    return sorted(uids)


def _mailbox_span(imap: imaplib.IMAP4_SSL) -> tuple[int, int]:
    """(message count, highest UID) for All Mail. (0, 0) if STATUS fails."""
    typ, data = imap.status('"[Gmail]/All Mail"', "(MESSAGES UIDNEXT)")
    if typ != "OK" or not data or not data[0]:
        return 0, 0
    blob = data[0].decode() if isinstance(data[0], bytes) else str(data[0])
    n = re.search(r"MESSAGES (\d+)", blob)
    nxt = re.search(r"UIDNEXT (\d+)", blob)
    count = int(n.group(1)) if n else 0
    high = int(nxt.group(1)) - 1 if nxt and int(nxt.group(1)) > 1 else 0
    return count, high


def _all_uids(imap: imaplib.IMAP4_SSL) -> list[int]:
    """Every UID in All Mail. No date filter. Pages UID ranges so SEARCH cannot truncate."""
    found = set(_uid_search(imap, "ALL"))
    count, high = _mailbox_span(imap)
    if count and len(found) >= count and (not high or max(found) >= high):
        return sorted(found)
    if not high:
        typ, data = imap.uid("FETCH", "*", "(UID)")
        if typ == "OK":
            fetched = _parse_uid_fetch(data)
            high = max(fetched) if fetched else 0
        if not high and found:
            high = max(found)
    for start in range(1, high + 1, 10_000):
        end = min(start + 9_999, high)
        found.update(_uid_search(imap, "UID", f"{start}:{end}"))
        if start > 1:
            try:
                imap.noop()
            except (imaplib.IMAP4.error, OSError):
                pass
    if not found:
        typ, data = imap.uid("FETCH", "1:*", "(UID)")
        if typ == "OK":
            found.update(_parse_uid_fetch(data))
    return sorted(found)


def _imap_day(day: date) -> str:
    return f"{day.day}-{_IMAP_MON[day.month - 1]}-{day.year}"


def _fetch_uids(imap: imaplib.IMAP4_SSL, db: sqlite3.Connection, uids: list[int],
                drop_unmatched: bool = False, log=None) -> tuple[int, set[int], imaplib.IMAP4_SSL]:
    stored = 0
    fetched: set[int] = set()
    for i in range(0, len(uids), 40):
        chunk = uids[i:i + 40]
        data = None
        for _attempt in range(3):
            try:
                typ, data = imap.uid("FETCH", ",".join(map(str, chunk)), "(UID BODY.PEEK[])")
                if typ == "OK" and data:
                    break
            except (imaplib.IMAP4.error, OSError):
                pass
            try:
                imap.logout()
            except (imaplib.IMAP4.error, OSError):
                pass
            time.sleep(2)
            try:
                imap = _imap()
            except (imaplib.IMAP4.error, OSError):
                continue
        else:
            continue
        if i and i % 400 == 0:
            try:
                imap.noop()
            except (imaplib.IMAP4.error, OSError):
                pass
            if log:
                log(f"email sync: opened {min(i, len(uids))}/{len(uids)}")
        for item in data:
            if not isinstance(item, tuple) or not item[1]:
                continue
            m = re.search(rb"UID (\d+)", item[0])
            if not m:
                continue
            uid = int(m.group(1))
            fetched.add(uid)
            db.execute("INSERT OR IGNORE INTO read_uids(uid) VALUES (?)", (uid,))
            msg = email.message_from_bytes(item[1])
            existed = db.execute("SELECT 1 FROM emails WHERE uid=?", (uid,)).fetchone()
            if _ingest_message(db, uid, msg):
                stored += 1
            elif drop_unmatched:
                db.execute("DELETE FROM emails WHERE uid=?", (uid,))
        db.commit()
    return stored, fetched, imap


def sync(log=print, catch_up_days: int = 0) -> int:
    """Open every All Mail UID once (tracked in read_uids). No date cutoff, no keyword search.

    catch_up_days re-opens recent mail even if already read, so parser fixes apply without
    waiting for new mail.
    """
    db = connect_db()
    last_uid = int((db.execute("SELECT v FROM state WHERE k='last_uid'").fetchone() or ["0"])[0])
    already = {u for (u,) in db.execute("SELECT uid FROM read_uids")}
    imap = _imap()
    try:
        uids = set(_all_uids(imap))
        if last_uid:
            uids.update(_uids_after(imap, last_uid))
        unread = sorted(u for u in uids if u not in already)
        if catch_up_days:
            recent = _uid_search(imap, "SINCE",
                                 _imap_day(date.today() - timedelta(days=int(catch_up_days))))
            unread = sorted(set(unread) | set(recent))
        added, fetched, imap = _fetch_uids(imap, db, unread, log=log)
        high = last_uid
        if fetched:
            high = max(high, max(fetched))
        read_now = already | fetched
        if uids and uids <= read_now:
            high = max(high, max(uids))
        db.execute("INSERT OR REPLACE INTO state VALUES ('last_uid', ?)", (str(high),))
        db.execute("INSERT OR REPLACE INTO state VALUES ('last_sync', ?)",
                   (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
        db.commit()
        log(f"email sync: {len(uids)} in All Mail, {len(unread)} opened, {added} application updates")
        return added
    finally:
        try:
            imap.logout()
        except (imaplib.IMAP4.error, OSError):
            pass
        db.close()


def reparse(log=print) -> int:
    """Re-read every stored email with the current parsers (company, role, status)."""
    db = connect_db()
    uids = [u for (u,) in db.execute("SELECT uid FROM emails ORDER BY uid")]
    imap = _imap()
    try:
        n, _, imap = _fetch_uids(imap, db, uids, drop_unmatched=True, log=log)
        db.execute("INSERT OR REPLACE INTO state VALUES ('last_sync', ?)",
                   (datetime.now(timezone.utc).isoformat(timespec="seconds"),))
        db.commit()
        log(f"email reparse: {n}/{len(uids)} still look like application updates")
        return n
    finally:
        try:
            imap.logout()
        except (imaplib.IMAP4.error, OSError):
            pass
        db.close()


def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _ckey(company: str) -> str:
    base = re.split(r"\s+[-–|(/@]\s*|,|:", company or "")[0]
    base = re.sub(r"\s+(&|and)\s+", " ", base, flags=re.I)
    base = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|company|technologies|technology|group|industries|"
                  r"the|usds jv|solutions|holdings)\b\.?", "", base, flags=re.I)
    key = _norm(base) or _norm(company)
    if key in ALIASES:
        key = _norm(ALIASES[key])
    return key


_ROLE_STOP = {"intern", "internship", "summer", "winter", "fall", "spring", "the", "and", "of", "for", "a",
              "co", "op", "coop", "2026", "2027", "2028", "student", "program", "usa", "us", "canada"}
_ROLE_GENERIC = _ROLE_STOP | {"software", "engineer", "engine", "develop", "developer", "programm",
                              "full", "stack", "fullstack", "remote", "hybrid", "onsite",
                              "undergraduate", "undergrad", "campus"}
_ROLE_JUNK_RX = re.compile(r" and the |has been received|careers site|job consideration", re.I)


def _role_better(new: str, old: str) -> bool:
    if not new:
        return False
    if not old:
        return True
    if _ROLE_JUNK_RX.search(old) and not _ROLE_JUNK_RX.search(new):
        return True
    if _ROLE_JUNK_RX.search(new):
        return False
    return len(_role_tokens(new)) > len(_role_tokens(old))


def _role_tokens(role: str) -> set[str]:
    tokens = set()
    for t in re.findall(r"[a-z0-9]+", role.lower()):
        if t in _ROLE_STOP or t.isdigit():
            continue
        for _ in range(2):  # engineering → engineer → engine, development → develop
            t = re.sub(r"(ing|ment|er)$", "", t) if len(t) > 5 else t
        tokens.add(t)
    return tokens


def _xlsx_rows() -> list[dict]:
    if not XLSX.exists():
        return []
    try:
        from openpyxl import load_workbook

        ws = load_workbook(XLSX, read_only=True).active
        keys = ["date", "company", "role", "location", "link", "resume", "status", "notes"]
        return [dict(zip(keys, (str(v) if v is not None else "" for v in r)))
                for r in ws.iter_rows(min_row=2, values_only=True) if any(r)]
    except Exception:  # noqa: BLE001 — missing openpyxl or a mid-write xlsx must not blank the tracker
        return []


def tracker_rows() -> dict:
    """One row per application: email history grouped by company (and role when known), merged with
    the tailored-resume rows, newest activity first."""
    db = connect_db()
    emails = db.execute("SELECT date, sender, subject, company, role, status, snippet FROM emails "
                        "ORDER BY date").fetchall()
    overrides = dict(db.execute("SELECT key, status FROM overrides").fetchall())
    last_sync = (db.execute("SELECT v FROM state WHERE k='last_sync'").fetchone() or [None])[0]
    db.close()

    apps: dict[str, dict] = {}

    def same_role(a: str, b: str) -> bool:
        if not a or not b:
            return False
        ta, tb = _role_tokens(a), _role_tokens(b)
        extra_a, extra_b = ta - _ROLE_GENERIC, tb - _ROLE_GENERIC
        # Ads vs E-commerce, AI vs SWE: keep separate. A generic "intern" receipt still
        # attaches to the one specific job at that company.
        if extra_a and extra_b and not (extra_a & extra_b):
            return False
        if not ta or not tb:
            return _norm(a) == _norm(b)
        if a in b or b in a:
            return True
        return len(ta & tb) / len(ta | tb) >= 0.5

    def find(company: str, role: str) -> dict | None:
        ck, rk = _ckey(company), _norm(role)
        if role and not _role_tokens(role):
            rk = ""
        same = [a for a in apps.values() if a["ckey"] == ck]
        if rk:
            for a in same:
                if a["role"] and same_role(a["role"].lower(), role.lower()):
                    return a
            return next((a for a in same if not a["rkey"]), None)
        return max(same, key=lambda a: a["updated"]) if same else None

    def new_app(company: str, role: str, when: str) -> dict:
        if role and not _role_tokens(role):
            role = ""
        key = f"{_ckey(company)}|{_norm(role)}"
        while key in apps:
            key += "+"
        apps[key] = {"key": key, "ckey": _ckey(company), "rkey": _norm(role),
                     "company": _alias_company(company), "role": role, "applied": when,
                     "updated": when, "status": "Applied", "resume": "",
                     "link": "", "location": "", "last_subject": "", "last_snippet": "", "emails": 0}
        return apps[key]

    def attach(a: dict, company: str, role: str, prefer_role: bool = False) -> None:
        company = _alias_company(company)
        if role and not _role_tokens(role):
            role = ""
        if role and (not a["role"] or prefer_role or _role_better(role, a["role"])):
            a["role"], a["rkey"] = role, _norm(role)
        if " " in (company or "") and " " not in (a["company"] or ""):
            a["company"] = company
        elif company in ALIASES.values() and a["company"] not in ALIASES.values():
            a["company"] = company

    def _strip_company_prefix(company: str, role: str) -> str:
        if company and role:
            role = re.sub(r"^" + re.escape(company) + r"\s*[-–|:]*\s*", "", role, flags=re.I)
            role = _clean_role(role)
            if _norm(role) == _norm(company):
                return ""
        return role

    for date, sender, subject, company, role, status, snippet in emails:
        company = _clean_company(company)
        role = _strip_company_prefix(company, _clean_role(role))
        a = find(company, role) or new_app(company, role, date[:10])
        attach(a, company, role)
        a["emails"] += 1
        a["applied"] = min(a["applied"], date[:10])
        if date[:10] >= a["updated"][:10]:
            a["updated"] = date
            a["last_subject"], a["last_snippet"] = subject, snippet
        # Status only moves forward. Action needed is not undone by a later receipt.
        if STATUS_ORDER.index(status) >= STATUS_ORDER.index(a["status"]):
            a["status"] = status

    for r in _xlsx_rows():
        company = _clean_company(re.sub(r"^the\s+", "", r["company"], flags=re.I))
        role = _strip_company_prefix(company, _clean_role(re.sub(r"\s+opportunity$", "", r["role"], flags=re.I)))
        status = r["status"] if r["status"] in STATUS_ORDER else "Applied"
        if not company or XLSX_SKIP_COMPANY.search(company) or JUNK_COMPANY.search(company):
            continue
        a = find(company, role)
        if a:
            attach(a, company, role)
            a["applied"] = min(a["applied"], r["date"][:10] or a["applied"])
            if r["resume"]:
                a["resume"] = r["resume"]
                attach(a, company, role, prefer_role=True)
            if r["link"]:
                a["link"] = a["link"] or r["link"]
            if r["location"]:
                a["location"] = a["location"] or r["location"]
            # Excel status from the old mail sync is stale; only Withdrawn (manual) or
            # rows with no email yet should set it.
            if status == "Withdrawn" or (not a["emails"] and
                    STATUS_ORDER.index(status) > STATUS_ORDER.index(a["status"])):
                a["status"] = status
            continue
        a = new_app(company, role, (r["date"] or "")[:10] or "9999-99-99")
        a.update(resume=r["resume"], link=r["link"], location=r["location"], status=status)

    rows = []
    for a in apps.values():
        a["auto_status"] = a["status"]
        a["status"] = overrides.get(a["key"], a["status"])
        a["updated"] = a["updated"][:10]
        rows.append({k: v for k, v in a.items() if k not in ("ckey", "rkey")})
    rows.sort(key=lambda a: a["updated"], reverse=True)
    return {"last_sync": last_sync, "statuses": STATUS_ORDER, "rows": rows}


def set_status(key: str, status: str | None) -> None:
    db = connect_db()
    if status:
        db.execute("INSERT OR REPLACE INTO overrides VALUES (?, ?)", (key, status))
    else:
        db.execute("DELETE FROM overrides WHERE key=?", (key,))
    db.commit()
    db.close()


if __name__ == "__main__":
    if "--reparse" in sys.argv:
        reparse()
    else:
        sync()
    data = tracker_rows()
    for r in data["rows"]:
        print(f"{r['updated']}  {r['status']:13} {r['company'][:28]:28} {r['role'][:50]}")
    print(f"\n{len(data['rows'])} applications", file=sys.stderr)
