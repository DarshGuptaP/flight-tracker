#!/usr/bin/env python3
"""
flight_watcher.py - a personal "find me a RARE deal" flight watcher.

Anomaly detection on a per-route price series, with a live cross-check:
    Travelpayouts (discover many routes) -> Google via fast-flights (confirm the
    promising ones) -> store history -> score -> alert on the best few

Two data sources, each doing what it is best at:
  - Travelpayouts prices_for_dates (v3) -> "cheapest destinations from <origin>".
    Wide discovery across Aviasales' cached cheapest fares. This is the
    consistent signal we track history on.
  - fast-flights (Google Flights scraper) -> live price for a specific route.
    Spent on the highest-SCORING candidates (bounded, to avoid getting
    rate-limited). Gives a live, closer-to-bookable price for two jobs:
      * CONFIRMATION: with REQUIRE_LIVE on, a candidate that Google cannot
        confirm never alerts, and the live price is what gets shown.
      * a REALITY-CHECK VETO: if Travelpayouts says cheap but Google's live price
        is much higher, the cached fare is stale/unbookable, so we suppress it.

WHAT COUNTS AS A DEAL
---------------------
The question is never "is this fare cheap?" but "is this fare RARE *for this
route*?" A $58 SJC-BUR is not a deal, it is Tuesday. A $600 SFO-NRT on a route
that never goes below $900 is a deal. So absolute price is a sanity rail, not
the trigger. Two independent paths can fire:

  PATH A - UNICORN: price <= an absolute floor so low it is rare regardless of
           history (e.g. long-haul under $349). Needs no history, so new routes
           and cold starts are still covered.
  PATH B - RARE: needs history, and must clear ALL of:
             * discount   >= MIN_DISCOUNT_PCT below the route's trailing median
             * savings    >= MIN_SAVING_USD in actual dollars
             * rarity     <= MAX_PCT_RANK percentile of everything seen
             * price      <= a generous per-haul sanity ceiling
           The three-way gate is the point: percentile alone fires on any dip in
           a flat series, and discount alone fires on 30% off a $40 fare.

Survivors are SCORED (depth, rarity, savings, confidence), ranked, and only the
top MAX_ALERTS_PER_RUN clear MIN_SCORE and reach you. Tune it all at once with
PROFILE=rare|balanced|aggressive.

WHY NOT ROBUST-Z (the old rule)
-------------------------------
Travelpayouts' cache is static between hourly runs, so MAD collapses to 0 on
~97% of routes. The old `mad or 1.0` fallback then made ANY drop over ~$5.19
score z<=-3.5. Backtested on real history that fired on 0.5% discounts - a
$1646 fare alerting because it moved $8. Percentile + discount depth replaces
it; nothing here divides by a spread that can be zero.

State is one JSON file (STATE_PATH), committed back to the repo each run so
the history rules have memory on GitHub Actions' ephemeral filesystem.

Quick start
-----------
    export TRAVELPAYOUTS_TOKEN=...     # travelpayouts.com affiliate token (free)
    export ALERT_SINK=stdout           # stdout | discord | slack | telegram
    export PROFILE=balanced            # rare | balanced | aggressive
    python flight_watcher.py

No keys needed to see it work on synthetic data:
    python flight_watcher.py --demo

Replay your real saved history to tune thresholds before committing to them:
    python flight_watcher.py --backtest
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

# PATH A - unicorn floors. Absolute prices rare enough to alert with no history.
# Calibrated so each sits BELOW the cheapest price ever observed in its class
# (domestic $48 SJC-LAX, medium $226 SFO-YYC, long $535 SFO-SHA). A floor above
# that line is not a unicorn detector, it is a subscription to whichever route
# is permanently cheapest - the $79 domestic floor fired on SFO-LAX every run.
UNICORN_FLOOR = {
    "domestic":  45,
    "medium":   199,
    "long":     399,
}

# Sanity ceiling for PATH B. A rare-for-the-route fare above this is still not
# something to wake you up for. Generous on purpose - the discount, savings and
# rarity gates do the real filtering, so this is only a backstop against absurd
# "deals" like the $1646 fare that used to alert. Note domestic runs to $450:
# the median domestic route here never drops below $318 (small regional fields
# like ITH and TLH drag it up), so a tighter cap silently disqualified them.
CEILING = {
    "domestic":  450,
    "medium":    800,
    "long":     1400,
}

# Strictness presets. PROFILE picks a row; any single knob can still be
# overridden by its own env var.
PROFILES = {
    #              discount%  pct_rank  saving$  min_score  max_alerts
    "rare":       (30.0,      10.0,     100.0,   60.0,      3),
    "balanced":   (20.0,      25.0,      60.0,   45.0,      5),
    "aggressive": (12.0,      40.0,      35.0,   30.0,     10),
}
PROFILE = os.getenv("PROFILE", "balanced").strip().lower()
_P = PROFILES.get(PROFILE, PROFILES["balanced"])

MIN_DISCOUNT_PCT   = float(os.getenv("MIN_DISCOUNT_PCT", _P[0]))  # vs median
MAX_PCT_RANK       = float(os.getenv("MAX_PCT_RANK",     _P[1]))  # 0=all-time low
MIN_SAVING_USD     = float(os.getenv("MIN_SAVING_USD",   _P[2]))  # real dollars
MIN_SCORE          = float(os.getenv("MIN_SCORE",        _P[3]))  # 0..~115
MAX_ALERTS_PER_RUN = int(os.getenv("MAX_ALERTS_PER_RUN", _P[4]))

MIN_HISTORY = int(os.getenv("MIN_HISTORY", "8"))       # points before PATH B arms
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "200"))     # cap points per series
HISTORY_DAYS = float(os.getenv("HISTORY_DAYS", "120"))  # age out older than this
# Hourly runs against a cache that barely moves would otherwise make the whole
# "history" span MAX_HISTORY hours. Collapse each window to one observation
# (the cheapest seen in it) so the baseline covers months, not days.
OBS_BUCKET_HOURS = float(os.getenv("OBS_BUCKET_HOURS", "6"))

DEDUP_HOURS = float(os.getenv("DEDUP_HOURS", "72"))    # suppress repeat alerts
# ...unless the price improved by at least this much, which is real news.
DEDUP_IMPROVE_PCT = float(os.getenv("DEDUP_IMPROVE_PCT", "10"))

# Google cross-check (fast-flights)
USE_GOOGLE       = os.getenv("USE_GOOGLE", "1") not in ("0", "false", "False", "")
GOOGLE_ENRICH_TOP = int(os.getenv("GOOGLE_ENRICH_TOP", "6"))  # top N/origin BY SCORE
GOOGLE_SLEEP     = float(os.getenv("GOOGLE_SLEEP", "2.0"))    # politeness delay
STALE_FACTOR     = float(os.getenv("STALE_FACTOR", "1.25"))   # veto threshold
# With this on, a candidate Google cannot confirm live never alerts.
REQUIRE_LIVE     = os.getenv("REQUIRE_LIVE", "1") not in ("0", "false", "False", "")

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
    price: float            # the tracked price, in this series' own terms
    depart: str
    ret: str | None
    haul: str
    google_price: float | None = None   # live cross-check price, if fetched
    floor_override: float | None = None  # per-route target price (watchlist)
    source: str = "travelpayouts"
    lane: str = "tp"        # "tp" | "w<offset>" - see .series

    @property
    def route(self) -> str:
        return f"{self.origin}-{self.destination}"

    @property
    def series(self) -> str:
        """History bucket key. Comparing a fare only against LIKE observations
        is what makes the median mean anything: a cached Travelpayouts fare and
        a live Google probe 38 days out are different distributions, and mixing
        them (as this used to, keying on route alone) inflates the spread until
        nothing looks anomalous."""
        return f"{self.route}|{self.lane}"


@dataclass
class Deal:
    """A fare that cleared the gates, with the numbers behind the decision."""
    fare: Fare
    score: float
    reason: str
    price: float            # what you would actually pay (live if confirmed)
    path: str               # "unicorn" | "rare"


# ----------------------------------------------------------------------------
# Storage - JSON file, loaded once, flushed once. Swap for DynamoDB/S3 to run
# on Lambda; nothing else changes.
# ----------------------------------------------------------------------------

class Store:
    """History is {series_key: [{"p": price, "t": unix_ts, "d": depart}, ...]}.

    v1 stored bare floats keyed on route, which lost both WHEN a price was seen
    (so the window could only be counted, not dated) and WHICH lane produced it.
    _migrate() upgrades old state in place on first load.
    """

    def __init__(self, path: str = STATE_PATH):
        self.path = path
        self.history: dict[str, list[dict]] = {}
        self.alerts: dict[str, dict] = {}
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
        self._migrate()

    def _migrate(self) -> None:
        """Upgrade v1 state: bare float lists keyed on bare routes."""
        now = time.time()
        migrated: dict[str, list[dict]] = {}
        changed = False
        for key, vals in self.history.items():
            if vals and isinstance(vals[0], dict):
                migrated[key] = vals
                continue
            changed = True
            # v1 had no lane suffix; Travelpayouts produced the overwhelming
            # majority of these, so land them in the "tp" lane. Timestamps are
            # backfilled hourly (matching the cron) so age-pruning keeps them.
            n = len(vals)
            migrated[f"{key}|tp" if "|" not in key else key] = [
                {"p": float(v), "t": now - (n - i) * 3600.0, "d": ""}
                for i, v in enumerate(vals)
                if isinstance(v, (int, float))
            ]
        if changed:
            print(f"[migrate] upgraded {len(migrated)} price series to v2 format",
                  file=sys.stderr)
        self.history = migrated
        # v1 alert values were bare floats (a timestamp); v2 stores price too.
        self.alerts = {k: (v if isinstance(v, dict) else {"t": v, "p": 0.0})
                       for k, v in self.alerts.items()}

    def record(self, fare: Fare, now: float) -> None:
        h = self.history.setdefault(fare.series, [])
        price = round(fare.price, 2)
        # Collapse rapid re-samples into one observation per bucket, keeping the
        # cheapest. Without this, hourly runs against a static cache stuff the
        # series with near-duplicates and the window shrinks to MAX_HISTORY hours.
        if h and (now - h[-1].get("t", 0)) < OBS_BUCKET_HOURS * 3600:
            if price < h[-1].get("p", price):
                h[-1]["p"], h[-1]["d"] = price, fare.depart
            return
        h.append({"p": price, "t": now, "d": fare.depart})
        cutoff = now - HISTORY_DAYS * 86400
        if len(h) > MAX_HISTORY or (h and h[0].get("t", now) < cutoff):
            fresh = [o for o in h if o.get("t", now) >= cutoff]
            del h[:]
            h.extend(fresh[-MAX_HISTORY:])

    def price_history(self, series: str) -> list[float]:
        """Prices in this series that are still inside the age window."""
        cutoff = time.time() - HISTORY_DAYS * 86400
        return [o["p"] for o in self.history.get(series, [])
                if isinstance(o, dict) and o.get("t", 0) >= cutoff]

    def recently_alerted(self, route: str, price: float, now: float) -> bool:
        """Dedup on the ROUTE, not on a price bucket. The old $25-bucket key let
        a route re-alert every time it drifted across a bucket edge; here a
        repeat only gets through if it beats the last alerted price outright."""
        last = self.alerts.get(route)
        if not last or (now - last.get("t", 0)) >= DEDUP_HOURS * 3600:
            return False
        prev = last.get("p", 0.0)
        return not (prev and price <= prev * (1 - DEDUP_IMPROVE_PCT / 100))

    def mark_alerted(self, route: str, price: float, now: float) -> None:
        self.alerts[route] = {"t": now, "p": round(price, 2)}

    def flush(self, now: float | None = None) -> None:
        if self.path == ":memory:":
            return
        now = now or time.time()
        cutoff = now - 2 * DEDUP_HOURS * 3600
        self.alerts = {k: v for k, v in self.alerts.items()
                       if v.get("t", 0) >= cutoff}
        self.run_counter += 1
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"history": self.history, "alerts": self.alerts,
                       "run_counter": self.run_counter, "version": 2}, f, indent=1)
        os.replace(tmp, self.path)


# ----------------------------------------------------------------------------
# Detection - pure functions.
# ----------------------------------------------------------------------------

def pct_rank(value: float, history: list[float]) -> float:
    """What fraction of past observations were at or below `value`, as 0-100.
    0 means an all-time low. Unlike a z-score this has no spread in the
    denominator, so a route whose price never moves cannot blow it up."""
    if not history:
        return 100.0
    return 100.0 * sum(1 for x in history if x <= value) / len(history)


def unicorn_floor(fare: Fare) -> float:
    """Absolute price rare enough to alert with no history at all. A watchlist
    target is a price the user would happily book, so a unicorn is well under."""
    if fare.floor_override is not None:
        return fare.floor_override * 0.75
    return UNICORN_FLOOR.get(fare.haul, UNICORN_FLOOR["long"])


def deal_score(discount: float, saving: float, rank: float,
               n: int, live: bool, all_time_low: bool) -> float:
    """Blend the evidence into one 0..~115 number so deals can be RANKED.
    Depth dominates, rarity is next, and dollars saved break ties between a
    deep discount on a cheap fare and a shallow one on an expensive fare."""
    s = min(discount / 50.0, 1.0) * 45          # 50%+ off maxes this out
    s += max(0.0, 1 - rank / 25.0) * 30         # full credit at an all-time low
    s += min(saving / 300.0, 1.0) * 15          # $300+ saved maxes this out
    s += min(n / 30.0, 1.0) * 10                # confidence in the baseline
    if all_time_low:
        s += 8
    if live:
        s += 5
    return s


def evaluate(fare: Fare, history: list[float]) -> Deal | None:
    """Decide whether a fare is worth waking someone up for. None = no."""
    gp = fare.google_price

    # VETO: cached fare looks cheap but Google's live price is much higher, so
    # the cached fare is stale/unbookable. Runs before anything else.
    if gp is not None and gp > STALE_FACTOR * fare.price:
        return None

    # A deal you cannot confirm is a rumour. Watchlist fares are born live, so
    # they carry their own google_price and pass this for free.
    live = gp is not None
    if REQUIRE_LIVE and USE_GOOGLE and not live:
        return None
    payable = gp if live else fare.price

    # ---- PATH A: unicorn absolute price. -----------------------------------
    # History-free ONLY when there is no history to consult. Once a route has a
    # baseline it gets a vote, because an absolute floor cannot know that a
    # route is simply always cheap: SJC-BUR lives at $60, so a $79 "domestic
    # unicorn" would fire on it every single run. That is the exact noise this
    # rework exists to kill, so a route with history must also be having an
    # unusually cheap day - at half the PATH B bar, since the price is already
    # objectively rare.
    uf = unicorn_floor(fare)
    n = len(history)
    if payable <= uf:
        if n < MIN_HISTORY:
            return Deal(fare, deal_score(50.0, uf - payable, 0.0, n, live, False),
                        f"${payable:.0f} is under the ${uf:.0f} {fare.haul} "
                        f"unicorn floor - rare at any time (no history yet)",
                        payable, "unicorn")
        med = statistics.median(history)
        discount = (1 - fare.price / med) * 100 if med > 0 else 0.0
        saving = med - fare.price
        if discount >= MIN_DISCOUNT_PCT / 2 and saving >= MIN_SAVING_USD / 2:
            rank = pct_rank(fare.price, history)
            score = deal_score(max(discount, 50.0), saving, rank, n, live,
                               fare.price < min(history))
            return Deal(fare, score, f"${payable:.0f} is under the ${uf:.0f} "
                        f"{fare.haul} unicorn floor, and {discount:.0f}% below "
                        f"this route's ${med:.0f} typical", payable, "unicorn")
        # Cheap but normal for this route - fall through to the relative test.

    # ---- PATH B: rare for THIS route. Needs a baseline. --------------------
    if n < MIN_HISTORY:
        return None
    if payable > CEILING.get(fare.haul, CEILING["long"]):
        return None

    med = statistics.median(history)
    if med <= 0:
        return None

    # Measure the discount in the series' own terms (fare.price is what the
    # history is made of), so the comparison stays apples-to-apples.
    discount = (1 - fare.price / med) * 100
    saving = med - fare.price
    rank = pct_rank(fare.price, history)

    if discount < MIN_DISCOUNT_PCT:
        return None
    if saving < MIN_SAVING_USD:      # 30% off a $40 fare is not a deal
        return None
    if rank > MAX_PCT_RANK:          # must be rare, not merely below average
        return None

    # The live price has to be genuinely good too, not just less-bad than a
    # stale cache. Demand it clear at least half the required discount.
    if live and gp > med * (1 - MIN_DISCOUNT_PCT / 200.0):
        return None

    low = min(history)
    score = deal_score(discount, saving, rank, n, live, fare.price < low)
    if score < MIN_SCORE:
        return None

    bits = [f"{discount:.0f}% below ${med:.0f} typical (saves ${saving:.0f})"]
    bits.append("all-time low" if fare.price < low
                else f"cheapest {100 - rank:.0f}% of {n} samples")
    if live:
        bits.append(f"Google live ${gp:.0f}")
    return Deal(fare, score, ", ".join(bits), payable, "rare")


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


# Haul classes from the US West Coast. The old 20-code table defaulted
# everything unknown to "domestic", which labelled FRA, MOW and ATH domestic and
# let $1000+ fares past a $120 floor. Unknown codes now default to "long":
# from SFO/SJC, anything not in the two tables below is almost always
# transoceanic (FRA, MOW, PRG, WAW, DXB, NAN all showed up that way in practice).
# Includes METRO codes (CHI, NYC, WAS) - Travelpayouts returns those alongside
# airport codes, and omitting them was enough to make a $145 Chicago fare look
# like a transoceanic unicorn.
_DOMESTIC = {
    "LAX", "SAN", "SNA", "ONT", "BUR", "LGB", "PSP", "SMF", "SBA", "FAT",
    "OAK", "BFL", "LAS", "PHX", "TUS", "SLC", "DEN", "ABQ", "BOI", "RNO",
    "IDA", "FCA", "PDX", "SEA", "PAE", "GEG", "DFW", "DAL", "IAH", "HOU",
    "AUS", "SAT", "MSP", "ORD", "MDW", "CHI", "DTW", "CLE", "CVG", "CMH",
    "STL", "MCI", "MKC", "MSY", "MEM", "BNA", "ATL", "CLT", "IND", "RDU",
    "IAD", "DCA", "BWI", "WAS", "PHL", "JFK", "LGA", "EWR", "NYC", "SWF",
    "ITH", "HAR", "PVD", "BOS", "PIT", "BUF", "MCO", "ORL", "TPA", "MIA",
    "FLL", "JAX", "RSW", "PBI", "GNV", "TLH", "MLU", "OKC", "TUL", "OMA",
    "DSM", "ICT", "LIT", "BHM", "GSP", "RIC", "ORF",
}
_MEDIUM = {          # Hawaii, Mexico, Caribbean, Central America, Canada, AK
    "HNL", "OGG", "KOA", "LIH", "ITO", "ANC", "FAI", "JNU",
    "CUN", "SJD", "PVR", "GDL", "MEX", "MZT", "ZIH", "HUX", "BJX", "MTY",
    "ACA", "AGU", "CEN", "LAP", "MLM", "QRO",
    "LIR", "SJO", "PTY", "SAL", "GUA", "BZE", "MGA", "TGU", "SAP", "RTB",
    "XPL", "NAS", "MBJ", "PUJ", "SDQ", "SJU", "AUA", "CUR", "BGI", "HAV",
    "PLS", "YVR", "YYC", "YEG", "YYZ", "YTO", "YUL", "YMQ", "YOW", "YWG",
    "YHZ", "YCG",
}


def classify_haul(dest_iata: str) -> str:
    """Rough distance class from SFO/SJC. Drives only the unicorn floor and the
    sanity ceiling; the relative rules do the real work, so approximate is fine."""
    if dest_iata in _DOMESTIC:
        return "domestic"
    if dest_iata in _MEDIUM:
        return "medium"
    return "long"


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


def prospect_score(fare: Fare, history: list[float]) -> float:
    """Cheap, history-only estimate of how promising a fare is BEFORE spending a
    scraper call on it. Same shape as the real score, minus anything live."""
    if fare.price <= unicorn_floor(fare):
        return 100.0
    n = len(history)
    if n < MIN_HISTORY:
        return 0.0
    med = statistics.median(history)
    if med <= 0 or fare.price > CEILING.get(fare.haul, CEILING["long"]):
        return 0.0
    discount = (1 - fare.price / med) * 100
    saving = med - fare.price
    # Loosened gates: worth a look at 60% of the bar, since the live price can
    # come back better than the cached one and push it over.
    if discount < MIN_DISCOUNT_PCT * 0.6 or saving < MIN_SAVING_USD * 0.6:
        return 0.0
    rank = pct_rank(fare.price, history)
    if rank > MAX_PCT_RANK * 1.5:
        return 0.0
    return deal_score(discount, saving, rank, n, False, fare.price < min(history))


def enrich_with_google(fares: list[Fare], store: Store) -> None:
    """Attach a live Google price to the most PROMISING GOOGLE_ENRICH_TOP
    candidates per origin.

    This used to sort by absolute price, so every run spent its whole scraper
    budget re-checking the same handful of cheap short-hauls and never looked at
    the actual anomalies. Ranking by prospect score points the budget at fares
    that could plausibly alert. Still bounded: hammering the scraper gets you
    blocked, and Actions IPs are already suspect.
    """
    if not USE_GOOGLE:
        return
    by_origin: dict[str, list[Fare]] = {}
    for f in fares:
        by_origin.setdefault(f.origin, []).append(f)
    for origin, group in by_origin.items():
        ranked = sorted(
            ((prospect_score(f, store.price_history(f.series)), f) for f in group),
            key=lambda pair: -pair[0],
        )
        picked = [f for s, f in ranked if s > 0][:GOOGLE_ENRICH_TOP]
        if picked:
            print(f"[google] {origin}: live-checking "
                  + ", ".join(f.destination for f in picked), file=sys.stderr)
        for fare in picked:
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
        # lane="w<offset>" keeps each lead time in its own series: a fare 3 days
        # out and one 66 days out are different markets, and averaging them
        # together produced a median that described neither.
        # google_price mirrors price because these ARE the live price - that
        # satisfies REQUIRE_LIVE without a second lookup, and the stale-veto is
        # a no-op on them by construction.
        fares.append(Fare(origin=origin, destination=dest, price=price,
                          depart=depart, ret=ret, haul=classify_haul(dest),
                          source="google-live", floor_override=floor,
                          google_price=price, lane=f"w{offset}"))
    return fares


# ----------------------------------------------------------------------------
# Alerting - pluggable sink via ALERT_SINK.
# ----------------------------------------------------------------------------

def alert(deal: Deal) -> None:
    fare = deal.fare
    link = (
        "https://www.google.com/travel/flights?q="
        + urllib.parse.quote(
            f"flights from {fare.origin} to {fare.destination} on {fare.depart}"
        )
    )
    badge = "\U0001f984" if deal.path == "unicorn" else "\u2708\ufe0f"
    msg = (
        f"{badge}  {fare.origin}->{fare.destination}  ${deal.price:.0f}  "
        f"(score {deal.score:.0f}, {fare.haul}, depart {fare.depart or 'flex'})\n"
        f"    why: {deal.reason}\n    {link}"
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
    """Evaluate every fare, then alert on only the best few.

    Ranking before alerting is what makes this "best of the best" rather than
    "everything that cleared the bar": on a run where twenty routes qualify you
    want the top handful, not twenty notifications.
    """
    now = time.time()
    deals: list[Deal] = []
    for fare in fares:
        history = store.price_history(fare.series)   # read BEFORE recording
        store.record(fare, now)
        deal = evaluate(fare, history)
        if deal is not None:
            deals.append(deal)

    deals.sort(key=lambda d: -d.score)
    if deals:
        print(f"[rank] {len(deals)} qualified; alerting on up to "
              f"{MAX_ALERTS_PER_RUN}", file=sys.stderr)
    hits = 0
    for deal in deals:
        if hits >= MAX_ALERTS_PER_RUN:
            break
        if store.recently_alerted(deal.fare.route, deal.price, now):
            continue
        alert(deal)
        store.mark_alerted(deal.fare.route, deal.price, now)
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
    enrich_with_google(fares, store)             # live check on TOP-SCORING candidates
    fares.extend(fetch_watchlist(store))         # born-live watchlist routes
    hits = process(fares, store)
    store.flush()                                # also bumps the run counter
    return hits


# ----------------------------------------------------------------------------
# Demo - synthetic data, no keys, no network.
# ----------------------------------------------------------------------------

def run_demo() -> int:
    store = Store(":memory:")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()

    def seed(series: str, price: float, n: int = 14) -> None:
        """Give a route a flat trading history at `price`."""
        store.history[series] = [
            {"p": price, "t": now - (n - i) * 86400.0, "d": ""} for i in range(n)
        ]

    seed("SFO-NRT|tp", 950)     # Tokyo normally ~$950
    seed("SJC-BUR|tp", 60)      # Burbank is always cheap
    seed("SFO-LAS|tp", 180)
    seed("SFO-MIA|tp", 300)
    seed("SJC-MOW|tp", 1654)    # the real-history fare that used to alert on -$8

    incoming = [
        # Deep discount on an expensive route, live-confirmed -> FIRE.
        Fare("SFO", "NRT", 600, today, today, "long", google_price=612),
        # Genuinely rare absolute price -> FIRE via the unicorn path.
        Fare("SFO", "CDG", 320, today, today, "long", google_price=331),
        # Cheap in absolute terms but utterly normal for the route -> ignore.
        Fare("SJC", "BUR", 58, today, today, "domestic", google_price=58),
        # Cached cheap BUT Google live is far higher -> stale, veto.
        Fare("SFO", "MIA", 110, today, today, "domestic", google_price=205),
        # The old bug: -$8 on a flat series scored z=-3.5 and alerted. Now the
        # discount and savings gates reject it outright.
        Fare("SJC", "MOW", 1646, today, today, "long", google_price=1646),
        # A real 15% drop, but on a cheap route it saves ~$27 -> not worth it.
        Fare("SFO", "LAS", 153, today, today, "domestic", google_price=153),
    ]
    seed("SFO-CDG|tp", 700)    # Paris normally ~$700, so $331 is a real unicorn

    print(f"=== demo (PROFILE={PROFILE}) ===")
    print("expect 2 alerts: NRT (37% off $950), CDG (unicorn AND 53% off)")
    print("expect 4 ignored: BUR ($58 is cheap but NORMAL for a $60 route),")
    print("                  MIA (stale-price veto), MOW (0.5% 'discount'),")
    print("                  LAS (real 15% drop, but saves only ~$27)\n")
    hits = process(incoming, store)
    print(f"\n=== {hits} alert(s) fired ===")
    return hits


def run_backtest() -> int:
    """Replay the saved history through the CURRENT thresholds.

    Every stored observation is scored against only the observations that
    preceded it, so this answers "what would this profile have sent me?" without
    burning a single scraper call. Google is assumed unavailable, so the live
    requirement is waived here - treat the counts as an upper bound.
    """
    global REQUIRE_LIVE
    store = Store()
    if not store.history:
        print("no history in state.json - nothing to replay")
        return 0
    REQUIRE_LIVE = False           # no live prices available when replaying

    # Replay in TIME order, grouped into runs, so per-run ranking and the dedup
    # window apply exactly as they would live. Evaluating series-by-series
    # instead would count the same persistent fare once per observation and
    # wildly overstate how many messages you would actually receive.
    events: list[tuple[float, str, int]] = []
    for series, obs in store.history.items():
        for i, o in enumerate(obs):
            if isinstance(o, dict):
                events.append((o.get("t", 0.0), series, i))
    events.sort()

    runs: dict[int, list[tuple[str, int]]] = {}
    for t, series, i in events:
        runs.setdefault(int(t // 3600), []).append((series, i))

    sent: list[tuple[float, Deal]] = []
    alerts: dict[str, dict] = {}
    qualified = 0
    for bucket in sorted(runs):
        now = bucket * 3600.0
        deals: list[Deal] = []
        for series, i in runs[bucket]:
            route, _, lane = series.partition("|")
            origin, _, dest = route.partition("-")
            if not dest:
                continue
            obs = store.history[series]
            prices = [o["p"] for o in obs[:i] if isinstance(o, dict)]
            fare = Fare(origin, dest, obs[i]["p"], obs[i].get("d", ""), None,
                        classify_haul(dest), lane=lane or "tp")
            deal = evaluate(fare, prices)
            if deal is not None:
                deals.append(deal)
        qualified += len(deals)
        deals.sort(key=lambda d: -d.score)
        hits = 0
        for deal in deals:
            if hits >= MAX_ALERTS_PER_RUN:
                break
            last = alerts.get(deal.fare.route)
            if last and (now - last["t"]) < DEDUP_HOURS * 3600 and not (
                    last["p"] and deal.price <= last["p"]
                    * (1 - DEDUP_IMPROVE_PCT / 100)):
                continue
            alerts[deal.fare.route] = {"t": now, "p": deal.price}
            sent.append((now, deal))
            hits += 1

    span_h = (events[-1][0] - events[0][0]) / 3600 if len(events) > 1 else 0
    print(f"=== backtest: PROFILE={PROFILE} "
          f"(discount>={MIN_DISCOUNT_PCT:.0f}%, rank<={MAX_PCT_RANK:.0f}, "
          f"saving>=${MIN_SAVING_USD:.0f}, score>={MIN_SCORE:.0f}) ===")
    print(f"{len(events)} observations across {len(store.history)} series, "
          f"{len(runs)} runs spanning {span_h:.0f}h")
    print(f"{qualified} qualified -> {len(sent)} ACTUALLY SENT after ranking "
          f"+ dedup")
    if span_h > 0:
        print(f"  ~{len(sent) / max(span_h / 168, 0.01):.1f} messages per week "
              f"at this rate\n")
    for now, deal in sorted(sent, key=lambda p: -p[1].score):
        print(f"  {deal.score:5.1f}  {deal.fare.route:9} ${deal.price:7.0f}  "
              f"[{deal.path}] {deal.reason}")
    return len(sent)


def main() -> None:
    ap = argparse.ArgumentParser(description="Personal rare-flight-deal watcher")
    ap.add_argument("--demo", action="store_true", help="run on synthetic data")
    ap.add_argument("--backtest", action="store_true",
                    help="replay saved history through the current thresholds")
    args = ap.parse_args()
    if args.demo:
        run_demo()
    elif args.backtest:
        run_backtest()
    else:
        print(f"done - {run_once()} alert(s)")


if __name__ == "__main__":
    main()