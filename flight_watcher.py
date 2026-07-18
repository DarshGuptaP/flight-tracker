#!/usr/bin/env python3
"""
flight_watcher.py - a personal "find me anywhere cheap" deal watcher.

Anomaly detection on a per-route price series, with a live cross-check:
    Travelpayouts (discover many routes) -> Google via fast-flights (confirm the
    promising ones) -> store history -> score -> alert on the good ones

Two data sources, each doing what it is best at:
  - Travelpayouts prices_for_dates (v3) -> "cheapest destinations from <origin>".
    Wide discovery across Aviasales' cached cheapest fares. This is the
    consistent daily signal we track history on.
  - fast-flights (Google Flights scraper) -> live price for a specific route.
    Used only on the most promising candidates (bounded, to avoid getting
    rate-limited). Gives a live, closer-to-bookable price for two jobs:
      * cross-check shown in the alert, and
      * a REALITY-CHECK VETO: if Travelpayouts says cheap but Google's live price is
        much higher, the cached fare is stale/unbookable, so we suppress it.
    This is the automated stand-in for a human verifying the deal is real.

Detection (all thresholds on the tracked Travelpayouts series unless noted):
  VETO       - drop if Google live price > STALE_FACTOR x the listed fare
  RULE 1     - fare (prefer Google live price if we have it) <= hard floor
  RULE 2     - listed fare is robust-z below the route's trailing median
  RULE 3     - Google live price is itself robust-z below that median

State is one JSON file (STATE_PATH), committed back to the repo each run so
the history rules have memory on GitHub Actions' ephemeral filesystem.

Quick start
-----------
    export TRAVELPAYOUTS_TOKEN=...     # travelpayouts.com affiliate token (free)
    export ALERT_SINK=stdout           # stdout | discord | slack | telegram
    export USE_GOOGLE=1                # set 0 to skip the Google cross-check
    python flight_watcher.py

No keys needed to see it work on synthetic data:
    python flight_watcher.py --demo
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

# ----------------------------------------------------------------------------
# Config - edit here or override via env.
# ----------------------------------------------------------------------------

ORIGINS = os.getenv("ORIGINS", "SFO,SJC").split(",")

HARD_FLOOR = {          # USD, round-trip, by haul class. At/under = interesting.
    "domestic": 120,
    "medium":   350,
    "long":     550,
}

Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "3.5"))   # robust-z below median
MIN_HISTORY = int(os.getenv("MIN_HISTORY", "6"))       # points before z-rule arms
DEDUP_HOURS = float(os.getenv("DEDUP_HOURS", "18"))    # suppress repeat alerts
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "40"))      # cap points per route

# Google cross-check (fast-flights)
USE_GOOGLE       = os.getenv("USE_GOOGLE", "1") not in ("0", "false", "False", "")
GOOGLE_ENRICH_TOP = int(os.getenv("GOOGLE_ENRICH_TOP", "6"))  # cheapest N/origin
GOOGLE_SLEEP     = float(os.getenv("GOOGLE_SLEEP", "2.0"))    # politeness delay
STALE_FACTOR     = float(os.getenv("STALE_FACTOR", "1.25"))   # veto threshold

# Live watchlist: routes we check live via Google EVERY run, independent of
# Travelpayouts. These are "born live", so no cache staleness to veto. To keep
# scraper volume down, each run probes ONE rotating date-offset across the whole
# watchlist (run counter picks it), so hourly runs sweep the horizon over time.
WATCHLIST_PATH = os.getenv("WATCHLIST_PATH", "watchlist.json")
WATCH_OFFSETS  = [int(x) for x in
                  os.getenv("WATCH_OFFSETS", "3,10,17,24,38,52,66").split(",")]
WATCH_TRIP_LEN = int(os.getenv("WATCH_TRIP_LEN", "3"))       # nights per probe

STATE_PATH   = os.getenv("STATE_PATH", "state.json")
TP_BASE = os.getenv("TP_BASE", "https://api.travelpayouts.com")


# ----------------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------------

@dataclass
class Fare:
    origin: str
    destination: str
    price: float            # the tracked (Travelpayouts) price
    depart: str
    ret: str | None
    haul: str
    google_price: float | None = None   # live cross-check price, if fetched
    floor_override: float | None = None  # per-route target price (watchlist)
    source: str = "travelpayouts"

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"


# ----------------------------------------------------------------------------
# Storage - JSON file, loaded once, flushed once. Swap for DynamoDB/S3 to run
# on Lambda; nothing else changes.
# ----------------------------------------------------------------------------

class Store:
    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.history: dict[str, list[float]] = {}
        self.alerts: dict[str, float] = {}
        self.run_counter: int = 0
        if path != ":memory:" and os.path.exists(path):
            try:
                with open(path) as f:
                    blob = json.load(f)
                self.history = blob.get("history", {})
                self.alerts = blob.get("alerts", {})
                self.run_counter = blob.get("run_counter", 0)
            except (json.JSONDecodeError, OSError) as e:
                print(f"[warn] could not read {path}: {e}", file=sys.stderr)

    def record(self, fare: Fare, now: float) -> None:
        h = self.history.setdefault(fare.route, [])
        h.append(round(fare.price, 2))
        if len(h) > MAX_HISTORY:
            del h[: len(h) - MAX_HISTORY]

    def price_history(self, route: str) -> list[float]:
        return list(self.history.get(route, []))

    @staticmethod
    def _key(route: str, price: float) -> str:
        return f"{route}:{int(price // 25)}"

    def recently_alerted(self, route: str, price: float, now: float) -> bool:
        last = self.alerts.get(self._key(route, price))
        return last is not None and (now - last) < DEDUP_HOURS * 3600

    def mark_alerted(self, route: str, price: float, now: float) -> None:
        self.alerts[self._key(route, price)] = now

    def flush(self, now: float | None = None) -> None:
        if self.path == ":memory:":
            return
        now = now or time.time()
        cutoff = now - 2 * DEDUP_HOURS * 3600
        self.alerts = {k: v for k, v in self.alerts.items() if v >= cutoff}
        self.run_counter += 1
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"history": self.history, "alerts": self.alerts,
                       "run_counter": self.run_counter}, f, indent=1)
        os.replace(tmp, self.path)


# ----------------------------------------------------------------------------
# Detection - pure functions.
# ----------------------------------------------------------------------------

def robust_z(value: float, history: list[float]) -> float | None:
    """Median + MAD z-score. Negative => cheaper than usual. None until warm."""
    if len(history) < MIN_HISTORY:
        return None
    med = statistics.median(history)
    mad = statistics.median([abs(x - med) for x in history]) or 1.0
    return (value - med) / (1.4826 * mad)


def is_deal(fare: Fare, history: list[float]) -> tuple[bool, str]:
    # Per-route target price wins over the generic haul-class floor when set.
    if fare.floor_override is not None:
        floor, floor_label = fare.floor_override, "target"
    else:
        floor, floor_label = HARD_FLOOR.get(fare.haul, HARD_FLOOR["long"]), f"{fare.haul} floor"
    gp = fare.google_price

    # VETO: cached fare looks cheap but Google's live price is much higher -> stale.
    if gp is not None and gp > STALE_FACTOR * fare.price:
        return False, ""

    # RULE 1: hard floor. Prefer the live Google price when we have one.
    ref = gp if gp is not None else fare.price
    tag = "Google live" if gp is not None else "listed"
    if ref <= floor:
        return True, f"{tag} ${ref:.0f} under ${floor:.0f} {floor_label}"

    # RULE 2: listed fare anomalously below the route's trailing median.
    z = robust_z(fare.price, history)
    if z is not None and z <= -Z_THRESHOLD:
        med = statistics.median(history)
        pct = round((1 - fare.price / med) * 100)
        extra = f", Google live ${gp:.0f}" if gp is not None else ""
        return True, f"{pct}% below ${med:.0f} trailing median (z={z:.1f}){extra}"

    # RULE 3: Google's live price is itself anomalously low vs history.
    if gp is not None:
        zg = robust_z(gp, history)
        if zg is not None and zg <= -Z_THRESHOLD:
            med = statistics.median(history)
            pct = round((1 - gp / med) * 100)
            return True, f"Google live ${gp:.0f} is {pct}% below ${med:.0f} median (z={zg:.1f})"

    return False, ""


# ----------------------------------------------------------------------------
# Travelpayouts (Aviasales) Data API - discovery source. Stdlib only.
# Free with an affiliate token. Uses the v3 `prices_for_dates` endpoint (the
# supported successor to the retired `v1/city-directions`): querying by origin
# alone with unique=true returns the cheapest fare per destination route.
# Data is CACHED (Aviasales search history, up to 7 days old), which is why the
# fast-flights live veto below matters: it catches cached fares that have died.
# ----------------------------------------------------------------------------

def _http_json(url: str, data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _iso_date(s: str | None) -> str:
    return (s or "")[:10]                     # "2026-03-08T16:35:00Z" -> date


def _expired(expires_at: str | None, now_dt) -> bool:
    if not expires_at:
        return False
    try:
        exp = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return exp < now_dt
    except ValueError:
        return False


def classify_haul(dest_iata: str) -> str:
    """Placeholder. Swap for a great-circle distance calc off a lat/long table."""
    far = {"LHR", "CDG", "NRT", "HND", "SYD", "BKK", "FCO", "BCN", "DXB"}
    med = {"HNL", "OGG", "LIR", "SJD", "CUN"}
    if dest_iata in far:
        return "long"
    if dest_iata in med:
        return "medium"
    return "domestic"


def fetch_discovery(token: str, origin: str) -> list[Fare]:
    """Cheapest fare per destination route from `origin`.

    Hits Aviasales Data API v3 `prices_for_dates` with origin-only + unique=true,
    which collapses the response to one cheapest fare per route (the supported
    stand-in for the retired v1/city-directions "anywhere cheap" call). No dates
    are passed, so the cache is scanned across the whole horizon. market=us
    targets the US price cache, since these origins are SFO/SJC.
    """
    q = urllib.parse.urlencode({
        "origin":   origin,
        "currency": "usd",
        "unique":   "true",     # one cheapest fare per destination route
        "sorting":  "price",    # cheapest first (order only; we keep them all)
        "one_way":  "false",    # round-trip, to match the round-trip floors
        "market":   "us",       # US price cache; drop this if results come thin
        "limit":    "1000",     # take the whole list, not the default 30
        "page":     "1",
    })
    url = f"{TP_BASE}/aviasales/v3/prices_for_dates?{q}"
    out = _http_json(url, headers={"X-Access-Token": token})
    # v3 wraps results in {success, data:[...], error}. data is a FLAT LIST of
    # fare objects (unlike v1/city-directions' {dest: info} dict), and carries
    # no expires_at, so the expiry filter below is a harmless no-op here.
    if not out.get("success", True):
        print(f"[warn] travelpayouts {origin}: {out.get('error')}", file=sys.stderr)
        return []
    now_dt = datetime.now(timezone.utc)
    fares = []
    for info in (out.get("data") or []):
        dest = info.get("destination")
        price = info.get("price")
        if not dest or not price:
            continue
        if _expired(info.get("expires_at"), now_dt):   # absent in v3; stays safe
            continue
        fares.append(Fare(
            origin=origin,
            destination=dest,
            price=float(price),
            depart=_iso_date(info.get("departure_at")),
            ret=_iso_date(info.get("return_at")) or None,
            haul=classify_haul(dest),
            source="travelpayouts",
        ))
    return fares


# ----------------------------------------------------------------------------
# Google cross-check via fast-flights. Optional dependency, lazily imported,
# and every call is defensive: a scraper failure must never break a run.
# ----------------------------------------------------------------------------

def _google_min_price(origin: str, dest: str, depart: str,
                      ret: str | None) -> float | None:
    """Cheapest live Google itinerary price for a specific route/date(s), or
    None if fast-flights isn't installed, nothing is found, or scraping fails."""
    try:
        import fast_flights as ff
    except ImportError:
        return None
    try:
        legs = [ff.FlightQuery(date=depart, from_airport=origin, to_airport=dest)]
        trip = "one-way"
        if ret:
            legs.append(ff.FlightQuery(date=ret, from_airport=dest,
                                       to_airport=origin))
            trip = "round-trip"
        query = ff.create_query(
            flights=legs, trip=trip, seat="economy",
            passengers=ff.Passengers(adults=1), currency="USD",
        )
        result = ff.get_flights(query)
        prices = [f.price for f in result
                  if isinstance(getattr(f, "price", None), (int, float)) and f.price > 0]
        return float(min(prices)) if prices else None
    except Exception as e:                       # noqa: BLE001 - stay alive
        print(f"[warn] google {origin}-{dest}: {e}", file=sys.stderr)
        return None


def google_lookup(fare: Fare) -> float | None:
    """Live cross-check price for an existing (Travelpayouts) candidate fare."""
    return _google_min_price(fare.origin, fare.destination, fare.depart, fare.ret)


def enrich_with_google(fares: list[Fare]) -> None:
    """Attach a live Google price to the cheapest GOOGLE_ENRICH_TOP candidates
    per origin. Bounded on purpose: hammering the scraper gets you blocked."""
    if not USE_GOOGLE:
        return
    by_origin: dict[str, list[Fare]] = {}
    for f in fares:
        by_origin.setdefault(f.origin, []).append(f)
    for origin, group in by_origin.items():
        for fare in sorted(group, key=lambda x: x.price)[:GOOGLE_ENRICH_TOP]:
            fare.google_price = google_lookup(fare)
            time.sleep(GOOGLE_SLEEP)


def load_watchlist() -> list[tuple[str, str, float | None]]:
    """Read watchlist.json. Each entry is either a plain "ORIG-DEST" string, or
    an object {"route": "ORIG-DEST", "floor": 250} to set a per-route target
    price. Returns (origin, dest, floor_override). Bad entries are skipped."""
    if not os.path.exists(WATCHLIST_PATH):
        return []
    try:
        with open(WATCHLIST_PATH) as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[warn] watchlist: {e}", file=sys.stderr)
        return []
    routes = []
    for item in raw:
        if isinstance(item, dict):
            route, floor = item.get("route", ""), item.get("floor")
        else:
            route, floor = item, None
        parts = str(route).upper().split("-")
        if len(parts) == 2 and all(len(p) == 3 for p in parts):
            routes.append((parts[0], parts[1],
                           float(floor) if floor is not None else None))
    return routes


def fetch_watchlist(store: Store) -> list[Fare]:
    """Live-check every watchlist route at ONE rotating date-offset this run.
    These fares are born live off Google, so they carry no google_price (nothing
    to veto) and are judged directly on the live price."""
    if not USE_GOOGLE:
        return []
    routes = load_watchlist()
    if not routes:
        return []
    offset = WATCH_OFFSETS[store.run_counter % len(WATCH_OFFSETS)]
    depart_dt = datetime.now(timezone.utc) + timedelta(days=offset)
    ret_dt = depart_dt + timedelta(days=WATCH_TRIP_LEN)
    depart, ret = depart_dt.strftime("%Y-%m-%d"), ret_dt.strftime("%Y-%m-%d")
    print(f"[watchlist] probing {len(routes)} routes at +{offset}d ({depart})",
          file=sys.stderr)
    fares = []
    for origin, dest, floor in routes:
        price = _google_min_price(origin, dest, depart, ret)
        time.sleep(GOOGLE_SLEEP)
        if price is None:
            continue
        fares.append(Fare(origin=origin, destination=dest, price=price,
                          depart=depart, ret=ret, haul=classify_haul(dest),
                          source="google-live", floor_override=floor))
    return fares


# ----------------------------------------------------------------------------
# Alerting - pluggable sink via ALERT_SINK.
# ----------------------------------------------------------------------------

def alert(fare: Fare, reason: str) -> None:
    link = (
        "https://www.google.com/travel/flights?q="
        + urllib.parse.quote(
            f"flights from {fare.origin} to {fare.destination} on {fare.depart}"
        )
    )
    cross = f"  [Google live ${fare.google_price:.0f}]" if fare.google_price else ""
    msg = (
        f"\u2708\ufe0f  {fare.origin}->{fare.destination}  ${fare.price:.0f}{cross}  "
        f"({fare.haul}, depart {fare.depart or 'flex'})\n"
        f"    why: {reason}\n    {link}"
    )
    sink = os.getenv("ALERT_SINK", "stdout")
    if sink == "discord":
        _post_discord(msg)
    elif sink == "slack":
        _post_slack(msg)
    elif sink == "telegram":
        _post_telegram(msg)
    else:
        print(msg)


# Discord (and Slack) sit behind Cloudflare, which 403s the default urllib
# User-Agent ("Python-urllib/x.y"). Sending a real UA is required, not optional.
# .strip() guards against a stray newline pasted into the webhook secret.
_ALERT_UA = "flight-watcher/1.0 (+https://github.com/DarshGuptaP/flight-tracker)"


def _post_discord(text: str) -> None:
    body = json.dumps({"content": text}).encode()
    urllib.request.urlopen(
        urllib.request.Request(
            os.environ["DISCORD_WEBHOOK_URL"].strip(), data=body,
            headers={"Content-Type": "application/json", "User-Agent": _ALERT_UA},
        ),
        timeout=15,
    )


def _post_slack(text: str) -> None:
    body = json.dumps({"text": text}).encode()
    urllib.request.urlopen(
        urllib.request.Request(
            os.environ["SLACK_WEBHOOK_URL"].strip(), data=body,
            headers={"Content-Type": "application/json", "User-Agent": _ALERT_UA},
        ),
        timeout=15,
    )


def _post_telegram(text: str) -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    body = urllib.parse.urlencode({
        "chat_id": os.environ["TELEGRAM_CHAT_ID"],
        "text": text,
        "disable_web_page_preview": "true",
    }).encode()
    urllib.request.urlopen(
        f"https://api.telegram.org/bot{token}/sendMessage", data=body, timeout=15
    )


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------

def process(fares: list[Fare], store: Store) -> int:
    now = time.time()
    hits = 0
    for fare in fares:
        history = store.price_history(fare.route)   # read BEFORE recording
        store.record(fare, now)
        hit, reason = is_deal(fare, history)
        if not hit or store.recently_alerted(fare.route, fare.price, now):
            continue
        alert(fare, reason)
        store.mark_alerted(fare.route, fare.price, now)
        hits += 1
    return hits


def run_once() -> int:
    store = Store()
    token = os.environ["TRAVELPAYOUTS_TOKEN"]
    fares: list[Fare] = []
    for origin in ORIGINS:
        try:
            fares.extend(fetch_discovery(token, origin.strip()))
        except Exception as e:
            print(f"[warn] {origin}: {e}", file=sys.stderr)
    enrich_with_google(fares)                    # live cross-check on TP candidates
    fares.extend(fetch_watchlist(store))         # born-live watchlist routes
    hits = process(fares, store)
    store.flush()                                # also bumps the run counter
    return hits


# ----------------------------------------------------------------------------
# Demo - synthetic data, no keys, no network.
# ----------------------------------------------------------------------------

def run_demo() -> int:
    import random
    random.seed(7)
    store = Store(":memory:")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    incoming = [
        # Cached cheap + Google confirms cheap -> FIRE (with cross-check).
        Fare("SFO", "LHR", 452, today, today, "long", google_price=430),
        # Under floor, no Google price fetched -> FIRE (listed).
        Fare("SJC", "BUR", 58,  today, today, "domestic"),
        # Under floor BUT Google live is much higher -> VETO, ignore.
        Fare("SFO", "MIA", 110, today, today, "domestic", google_price=205),
        # Above floor, no history -> ignore (normal).
        Fare("SFO", "LAS", 180, today, today, "domestic"),
    ]
    print("=== demo ===")
    print("expect 2 alerts: LHR (Google-confirmed), BUR (listed)")
    print("expect 2 ignored: MIA (Google reality-check veto), LAS (normal)\n")
    hits = process(incoming, store)
    print(f"\n=== {hits} alert(s) fired ===")
    return hits


def main() -> None:
    ap = argparse.ArgumentParser(description="Personal cheap-flight watcher")
    ap.add_argument("--demo", action="store_true", help="run on synthetic data")
    args = ap.parse_args()
    if args.demo:
        run_demo()
    else:
        print(f"done - {run_once()} alert(s)")


if __name__ == "__main__":
    main()