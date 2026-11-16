#!/usr/bin/env python3
"""Upcoming in-person events in Toronto, Montreal and Ottawa hosted/organized/sponsored by an
organization (any topic), for the /events page.

Sources (public pages/APIs only; a source that fails or blocks just contributes 0 rows):
  luma        api.lu.ma discover feed per city (organizer = the event's non-personal calendar)
  eventbrite  city search pages (all, business, science & tech) + each event page's organizer
  meetup      city "find events" pages (organizer = the Meetup group)
  allevents   allevents.in city/category pages (schema.org organizer)
  devpost     upcoming in-person hackathons whose venue is in one of the cities
  mlh         MLH season schedule

Which organizers are on the outreach lists is decided by view_local.py at request time, so this
file only stores raw organizer names.

Cached to engineering-ats/events.json. Eventbrite organizer lookups are cached separately
(engineering-ats/.event_organizers.json) so a refresh only opens new event pages.

    .venv/bin/python scripts/events.py           # refresh now, print per-source counts
"""
from __future__ import annotations

import html
import json
import re
import sys
import threading
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402
from metros import metros_in  # noqa: E402

CACHE = core.OUT_DIR / "events.json"
ORG_CACHE = core.OUT_DIR / ".event_organizers.json"
REFRESH = 6 * 60 * 60
HORIZON_DAYS = 120
PAUSE = 0.3
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/130 Safari/537.36")

CITIES = {  # name -> (lat, lon, luma featured place id or None)
    "Toronto": (43.6532, -79.3832, "discplace-Cx3JMS6vXKAbhV5"),
    "Montreal": (45.5019, -73.5674, "discplace-CXKKcJmNkbj6ikW"),
    "Ottawa": (45.4215, -75.6972, None),
}
EVENTBRITE_SLUGS = {"Toronto": "canada--toronto", "Montreal": "canada--montreal",
                    "Ottawa": "canada--ottawa"}
EVENTBRITE_CATS = [("all-events", 6), ("business--events", 4), ("science-and-tech--events", 4)]
MEETUP_LOC = {"Toronto": "ca--on--Toronto", "Montreal": "ca--qc--Montreal",
              "Ottawa": "ca--on--Ottawa"}
MEETUP_CATS = ["", "546", "405"]  # all, technology, career & business
ALLEVENTS_SLUGS = {"Toronto": "toronto", "Montreal": "montreal", "Ottawa": "ottawa"}
ALLEVENTS_CATS = ["all", "tech", "business", "workshops", "conferences"]
# Venues that show up without a city name (Devpost/MLH).
VENUE_ALIASES = {
    "bayview yards": "Ottawa", "university of ottawa": "Ottawa", "carleton university": "Ottawa",
    "uottawa": "Ottawa", "invest ottawa": "Ottawa",
    "university of toronto": "Toronto", "myhal": "Toronto", "toronto metropolitan": "Toronto",
    "york university": "Toronto", "mars discovery": "Toronto", "bahen": "Toronto",
    "mcgill": "Montreal", "concordia": "Montreal", "polytechnique montr": "Montreal",
    "ets montr": "Montreal", "école de technologie supérieure": "Montreal", "notman": "Montreal",
}
WANTED = set(CITIES)
_refresh_lock = threading.Lock()


def city_of(*texts: str) -> str:
    """First of Toronto/Montreal/Ottawa found in any of the texts (metro suburbs included)."""
    for t in texts:
        if not t:
            continue
        for m in metros_in(t):
            if m in WANTED:
                return m
        low = t.lower()
        for alias, city in VENUE_ALIASES.items():
            if alias in low:
                return city
    return ""


def _iso(s: str) -> str:
    return (s or "").strip()


def _client() -> httpx.Client:
    return httpx.Client(timeout=30, follow_redirects=True,
                        headers={"User-Agent": UA, "Accept-Language": "en-CA,en;q=0.9"})


def _ld_json(page: str) -> Iterable[dict]:
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', page, re.S):
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        stack = data if isinstance(data, list) else [data]
        while stack:
            d = stack.pop()
            if isinstance(d, list):
                stack.extend(d)
            elif isinstance(d, dict):
                if d.get("@type") == "Event":
                    yield d
                for k in ("itemListElement", "item", "@graph"):
                    if k in d:
                        stack.append(d[k])


def _price_from_offers(offers) -> str:
    offers = offers if isinstance(offers, list) else [offers] if offers else []
    lows = []
    for o in offers:
        if not isinstance(o, dict):
            continue
        p = o.get("lowPrice", o.get("price"))
        try:
            lows.append(float(p))
        except (TypeError, ValueError):
            continue
    if not lows:
        return ""
    low = min(lows)
    return "Free" if low == 0 else f"${low:,.2f}"


def _row(**kw) -> dict:
    kw["organizers"] = [o for o in dict.fromkeys(
        html.unescape(x).strip() for x in kw.get("organizers") or []) if o]
    kw["title"] = html.unescape(kw.get("title") or "").strip()
    return kw


# ---- sources --------------------------------------------------------------------------------

def luma(client: httpx.Client) -> list[dict]:
    out: list[dict] = []
    for city, (lat, lon, place) in CITIES.items():
        base = ({"discover_place_api_id": place} if place
                else {"latitude": lat, "longitude": lon})
        cursor = None
        for _ in range(30):
            params = {**base, "pagination_limit": 50}
            if cursor:
                params["pagination_cursor"] = cursor
            r = client.get("https://api.lu.ma/discover/get-paginated-events", params=params)
            if r.status_code != 200:
                break
            data = r.json()
            for e in data.get("entries") or []:
                ev, cal = e.get("event") or {}, e.get("calendar") or {}
                if ev.get("location_type") == "online":
                    continue
                geo = ev.get("geo_address_info") or {}
                c = city_of(geo.get("city_state") or "", geo.get("full_address") or "",
                            geo.get("city") or "")
                if not c:
                    continue
                orgs = []
                if cal.get("name") and not cal.get("personal_user_api_id"):
                    orgs.append(cal["name"])
                if not orgs:
                    continue  # personal calendar: an individual, not an organization
                ti = e.get("ticket_info") or {}
                cents = ((ti.get("price") or {}).get("cents"))
                price = "Free" if ti.get("is_free") else (f"${cents / 100:,.2f}" if cents else "")
                out.append(_row(title=ev.get("name"), start=_iso(ev.get("start_at")),
                                end=_iso(ev.get("end_at")), city=c,
                                venue=geo.get("address") or geo.get("short_address") or "",
                                price=price, organizers=orgs,
                                url=f"https://luma.com/{ev.get('url')}", source="luma"))
            cursor = data.get("next_cursor")
            if not data.get("has_more") or not cursor:
                break
            time.sleep(PAUSE)
    return out


def _load_org_cache() -> dict[str, list[str]]:
    try:
        return json.loads(ORG_CACHE.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def eventbrite(client: httpx.Client) -> list[dict]:
    found: dict[str, dict] = {}
    for city, slug in EVENTBRITE_SLUGS.items():
        for cat, pages in EVENTBRITE_CATS:
            for page in range(1, pages + 1):
                r = client.get(f"https://www.eventbrite.ca/d/{slug}/{cat}/", params={"page": page})
                if r.status_code != 200:
                    break
                items = list(_ld_json(r.text))
                if not items:
                    break
                for it in items:
                    url = (it.get("url") or "").split("?")[0]
                    if not url or url in found:
                        continue
                    if "Online" in (it.get("eventAttendanceMode") or ""):
                        continue
                    addr = ((it.get("location") or {}).get("address") or {})
                    c = city_of(f"{addr.get('addressLocality', '')}, {addr.get('addressRegion', '')}",
                                (it.get("location") or {}).get("name") or "") or city
                    found[url] = dict(title=it.get("name"), start=_iso(it.get("startDate")),
                                      end=_iso(it.get("endDate")), city=c,
                                      venue=(it.get("location") or {}).get("name") or "",
                                      url=url)
                time.sleep(PAUSE)
    orgs = _load_org_cache()
    fetched = 0
    for url, ev in found.items():
        if url not in orgs:
            try:
                page = client.get(url).text
            except httpx.HTTPError:
                continue
            names = re.findall(r'"organizer":\{"@type":"Organization","name":"((?:[^"\\]|\\.)*)"', page)
            price = ""
            for d in _ld_json(page):
                price = _price_from_offers(d.get("offers")) or price
            if not price and re.search(r'"is_free"\s*:\s*true', page):
                price = "Free"
            orgs[url] = [json.loads(f'"{n}"') for n in names[:1]] + ([f"__price:{price}"] if price else [])
            fetched += 1
            time.sleep(PAUSE)
            if fetched % 50 == 0:
                ORG_CACHE.write_text(json.dumps(orgs))
        ev["organizers"] = [o for o in orgs.get(url, []) if not o.startswith("__price:")]
        ev["price"] = next((o.split(":", 1)[1] for o in orgs.get(url, []) if o.startswith("__price:")), "")
    ORG_CACHE.write_text(json.dumps(orgs))
    return [_row(source="eventbrite", **ev) for ev in found.values() if ev.get("organizers")]


def meetup(client: httpx.Client) -> list[dict]:
    out: dict[str, dict] = {}
    for city, loc in MEETUP_LOC.items():
        for cat in MEETUP_CATS:
            params = {"location": loc, "source": "EVENTS", "eventType": "inPerson"}
            if cat:
                params["categoryId"] = cat
            r = client.get("https://www.meetup.com/find/", params=params)
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if r.status_code != 200 or not m:
                continue
            state = json.loads(m.group(1)).get("props", {}).get("pageProps", {}).get("__APOLLO_STATE__", {})
            for k, e in state.items():
                if not k.startswith("Event:") or e.get("eventType") == "ONLINE":
                    continue
                url = e.get("eventUrl") or ""
                venue = e.get("venue") or {}
                c = city_of(f"{venue.get('city', '')}, {venue.get('state', '')}", venue.get("name") or "")
                if not url or not c:
                    continue
                group = state.get(((e.get("group") or {}).get("__ref")) or "", {})
                fee = e.get("feeSettings") or {}
                price = f"${float(fee['amount']):,.2f}" if fee.get("amount") else "Free"
                out[url] = _row(title=e.get("title"), start=_iso(e.get("dateTime")), end="",
                                city=c, venue=venue.get("name") or "", price=price,
                                organizers=[group.get("name") or ""], url=url, source="meetup")
            time.sleep(PAUSE)
    return [r for r in out.values() if r["organizers"]]


def allevents(client: httpx.Client) -> list[dict]:
    out: dict[str, dict] = {}
    for city, slug in ALLEVENTS_SLUGS.items():
        for cat in ALLEVENTS_CATS:
            r = client.get(f"https://allevents.in/{slug}/{cat}")
            if r.status_code != 200:
                continue
            for it in _ld_json(r.text):
                url = (it.get("url") or "").split("?")[0]
                loc = it.get("location") or {}
                addr = loc.get("address") or {} if isinstance(loc, dict) else {}
                c = city_of(f"{addr.get('addressLocality', '')}, {addr.get('addressRegion', '')}",
                            (loc.get("name") if isinstance(loc, dict) else "") or "")
                orgs = it.get("organizer") or []
                orgs = orgs if isinstance(orgs, list) else [orgs]
                names = [o.get("name", "") for o in orgs if isinstance(o, dict)]
                if not url or not c or not any(names):
                    continue
                out[url] = _row(title=it.get("name"), start=_iso(it.get("startDate")),
                                end=_iso(it.get("endDate")), city=c,
                                venue=(loc.get("name") if isinstance(loc, dict) else "") or "",
                                price=_price_from_offers(it.get("offers")), organizers=names,
                                url=url, source="allevents")
            time.sleep(PAUSE)
    return list(out.values())


def devpost(client: httpx.Client) -> list[dict]:
    out = []
    for page in range(1, 20):
        r = client.get("https://devpost.com/api/hackathons",
                       params={"status[]": ["upcoming", "open"], "challenge_type[]": "in-person",
                               "page": page})
        if r.status_code != 200:
            break
        hs = r.json().get("hackathons") or []
        if not hs:
            break
        for h in hs:
            loc = (h.get("displayed_location") or {}).get("location") or ""
            c = city_of(loc, h.get("title") or "")
            if not c:
                continue
            dates = h.get("submission_period_dates") or ""
            out.append(_row(title=h.get("title"), start=dates, end="", city=c, venue=loc,
                            price="Free", organizers=[h.get("organization_name") or h.get("title")],
                            url=h.get("url") or "", source="devpost"))
        time.sleep(PAUSE)
    return out


_MLH_EVENT = re.compile(
    r'"name":"(?P<name>[^"]+)","status":"[^"]*","startsAt":"(?P<start>[^"]+)","endsAt":"(?P<end>[^"]+)"'
    r'.*?"location":"(?P<loc>[^"]*)","formatType":"(?P<fmt>[^"]*)".*?"websiteUrl":"(?P<url>[^"]*)"', re.S)


def mlh(client: httpx.Client) -> list[dict]:
    out = []
    year = datetime.now(UTC).year
    for season in (year, year + 1):
        r = client.get(f"https://www.mlh.com/seasons/{season}/events")
        if r.status_code != 200:
            continue
        for m in _MLH_EVENT.finditer(r.text):
            if m["fmt"] == "digital":
                continue
            c = city_of(m["loc"])
            if not c:
                continue
            out.append(_row(title=m["name"], start=m["start"], end=m["end"], city=c,
                            venue=m["loc"], price="Free", organizers=[m["name"], "Major League Hacking"],
                            url=m["url"].replace("\\/", "/"), source="mlh"))
    return out


SOURCES: dict[str, Callable[[httpx.Client], list[dict]]] = {
    "luma": luma, "eventbrite": eventbrite, "meetup": meetup, "allevents": allevents,
    "devpost": devpost, "mlh": mlh,
}


# ---- build / cache --------------------------------------------------------------------------

def _start_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=UTC)
    except ValueError:
        pass
    m = re.match(r"([A-Z][a-z]{2}) (\d{1,2})(?: - .*)?, (\d{4})", s)  # devpost "Nov 21 - 23, 2026"
    if m:
        try:
            return datetime.strptime(f"{m[1]} {m[2]} {m[3]}", "%b %d %Y").replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def build() -> dict:
    rows: list[dict] = []
    counts: dict[str, int | str] = {}
    with _client() as client:
        for name, fn in SOURCES.items():
            try:
                got = fn(client)
                counts[name] = len(got)
                rows.extend(got)
            except Exception as e:  # noqa: BLE001 - one broken source must not stop the rest
                counts[name] = f"error: {type(e).__name__}"
    now = datetime.now(UTC)
    horizon = now + timedelta(days=HORIZON_DAYS)
    seen: dict[tuple, dict] = {}
    for r in rows:
        d = _start_dt(r.get("start") or "")
        if d is None or d < now - timedelta(hours=6) or d > horizon:
            continue
        r["start_sort"] = d.isoformat()
        key = (re.sub(r"[^a-z0-9]", "", r["title"].lower())[:40], d.date().isoformat(), r["city"])
        if key in seen:  # same event on several sources: keep one, merge organizers
            have = seen[key]
            have["organizers"] = list(dict.fromkeys(have["organizers"] + r["organizers"]))
            have.setdefault("also_on", []).append(r["source"])
            continue
        seen[key] = r
    out = sorted(seen.values(), key=lambda r: r["start_sort"])
    return {"updated": now.isoformat(timespec="seconds"), "counts": counts, "events": out}


def refresh() -> dict:
    with _refresh_lock:
        data = build()
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE.with_name(f".{CACHE.name}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False))
        tmp.replace(CACHE)
        return data


def load() -> dict:
    """Cached events; starts a background refresh when the cache is over REFRESH old."""
    data: dict = {}
    try:
        data = json.loads(CACHE.read_text())
    except (OSError, json.JSONDecodeError):
        pass
    stale = not CACHE.exists() or time.time() - CACHE.stat().st_mtime > REFRESH
    if stale and not _refresh_lock.locked():
        threading.Thread(target=refresh, daemon=True).start()
        data["refreshing"] = True
    return data or {"events": [], "counts": {}, "updated": None, "refreshing": True}


def main() -> None:
    data = refresh()
    print(f"{len(data['events'])} upcoming events -> {CACHE}")
    for k, v in data["counts"].items():
        print(f"  {k:<11} {v}")


if __name__ == "__main__":
    main()
