from __future__ import annotations

import gzip
import json
import os
import re
import sys
import time
from datetime import datetime
from difflib import SequenceMatcher
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from yt_dlp import YoutubeDL


ROOT = Path(__file__).resolve().parent
APP_VERSION = "20260503-5"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0 Safari/537.36"
)
WATCH_CACHE = {}
TRAILER_CACHE = {}
IMDB_ID_CACHE = {}
IMDB_RATING_CACHE = {}
IMDB_DATASET_FILE = ROOT / ".imdb-title-ratings.tsv.gz"
IMDB_DATASET_TTL = 60 * 60 * 24 * 7
WATCH_TTL = 60 * 60 * 6
TRAILER_TTL = 60 * 60


def now_label() -> str:
    return datetime.now().astimezone().strftime("%B %-d, %Y at %-I:%M %p")


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    with urlopen(request, timeout=25) as response:
        return response.read().decode("utf-8", "ignore")


def fetch_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    with urlopen(request, timeout=25) as response:
        return json.loads(response.read().decode("utf-8", "ignore"))


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def parse_ld_json(html: str) -> dict:
    matches = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for raw in matches:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and entry.get("@type") in {"Movie", "TVSeries"}:
                    return entry
        if isinstance(data, dict) and data.get("@type") in {"Movie", "TVSeries"}:
            return data
    raise ValueError("No usable ld+json payload found")


def score_candidate(title: str, year: str, type_hint: str, path: str, ld: dict) -> float:
    candidate_title = str(ld.get("name", ""))
    score = SequenceMatcher(None, normalize(title), normalize(candidate_title)).ratio() * 5
    wanted_tokens = set(normalize(title).split())
    candidate_tokens = set(normalize(candidate_title).split())
    if wanted_tokens:
        score += len(wanted_tokens & candidate_tokens) / len(wanted_tokens)
    if normalize(title) == normalize(candidate_title):
        score += 2
    if year:
        created = str(ld.get("dateCreated", ""))[:4]
        if created == year:
            score += 1.5
    if type_hint in {"show", "series"} and "/tv-show/" in path:
        score += 1
    if type_hint == "movie" and "/movie/" in path:
        score += 1
    return score


def resolve_justwatch_page(title: str, type_hint: str, year: str) -> tuple[str, dict]:
    search_url = f"https://www.justwatch.com/us/search?q={quote(title)}"
    search_html = fetch_text(search_url)
    paths = []
    for path in re.findall(r"/us/(?:movie|tv-show)/[^\"?#< ]+", search_html):
        if path not in paths:
            paths.append(path)

    best_path = ""
    best_ld = {}
    best_score = float("-inf")

    for path in paths[:8]:
        try:
            page_html = fetch_text(f"https://www.justwatch.com{path}")
            ld = parse_ld_json(page_html)
            score = score_candidate(title, year, type_hint, path, ld)
        except Exception:
            continue
        if score > best_score:
            best_score = score
            best_path = path
            best_ld = ld

    if not best_path:
        raise ValueError("Could not resolve JustWatch page")

    return best_path, best_ld


def price_label(price: object, currency: str, billing_period: str) -> str:
    if price in (None, ""):
        return "Price not listed"
    try:
        numeric = float(price)
    except (TypeError, ValueError):
        return str(price)
    if currency == "USD":
        label = f"${numeric:,.2f}".rstrip("0").rstrip(".")
    else:
        label = f"{currency} {numeric:,.2f}".rstrip("0").rstrip(".")
    if billing_period:
        return f"{label} / {billing_period.lower()}"
    return label


def extract_offers(ld: dict) -> list[dict]:
    offers = []
    seen = set()
    actions = ld.get("potentialAction", [])
    if not isinstance(actions, list):
        actions = [actions]
    for action in actions:
        if not isinstance(action, dict):
            continue
        offer = action.get("expectsAcceptanceOf") or {}
        if not isinstance(offer, dict):
            continue
        category = str(offer.get("category", "")).lower()
        if "dvd" in category or "blu" in category:
            continue
        business = str(offer.get("businessFunction", "")).lower()
        action_type = str(action.get("@type", "")).lower()
        if "rentaction" in business:
            kind = "Rent"
        elif "sellaction" in business or "buyaction" in action_type or "sell" in business:
            kind = "Buy"
        else:
            kind = "Streaming"
        provider = offer.get("offeredBy", {}).get("name") or offer.get("seller", {}).get("name") or "Provider"
        url = action.get("target", {}).get("urlTemplate") or offer.get("url") or ""
        currency = str(offer.get("priceCurrency") or "USD")
        billing_period = ""
        quality = ""
        for extra in offer.get("additionalProperty", []) or []:
          if not isinstance(extra, dict):
            continue
          name = extra.get("name")
          if name == "BillingPeriod":
            billing_period = str(extra.get("value", ""))
          if name == "videoFormat":
            quality = str(extra.get("value", ""))
        key = (kind, provider, str(offer.get("price")), billing_period)
        if key in seen:
            continue
        seen.add(key)
        offers.append(
            {
                "kind": kind,
                "provider": provider,
                "priceLabel": price_label(offer.get("price"), currency, billing_period),
                "billingPeriod": billing_period.title() if billing_period else "",
                "quality": quality,
                "url": url,
            }
        )

    order = {"Streaming": 0, "Rent": 1, "Buy": 2}
    offers.sort(key=lambda entry: (order.get(entry["kind"], 9), entry["provider"]))
    return offers


def fetch_watch_data(title: str, type_hint: str, year: str) -> dict:
    cache_key = (title, type_hint, year)
    cached = WATCH_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < WATCH_TTL:
        return cached[1]

    path, ld = resolve_justwatch_page(title, type_hint, year)
    data = {
        "resolvedTitle": ld.get("name", title),
        "summary": ld.get("description", ""),
        "offers": extract_offers(ld),
        "updatedAt": now_label(),
        "sourceUrl": f"https://www.justwatch.com{path}",
    }
    WATCH_CACHE[cache_key] = (time.time(), data)
    return data


def imdb_query_slug(title: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", "", title.lower())
    return compact or "title"


def imdb_type_bonus(type_hint: str, candidate: dict) -> float:
    candidate_type = str(candidate.get("qid") or candidate.get("q") or "").lower()
    if type_hint in {"show", "series"}:
        if any(token in candidate_type for token in ("tv", "series", "episode")):
            return 1.4
        return -0.3
    if type_hint == "movie":
        if any(token in candidate_type for token in ("movie", "feature", "tvmovie")):
            return 1.2
        return -0.3
    return 0


def imdb_year_bonus(year: str, candidate: dict) -> float:
    if not year:
        return 0
    try:
        wanted = int(year)
        found = int(candidate.get("y") or 0)
    except (TypeError, ValueError):
        return 0
    if wanted == found:
        return 1.6
    gap = abs(wanted - found)
    if gap == 1:
        return 0.5
    if gap >= 4:
        return -0.8
    return 0


def score_imdb_candidate(title: str, year: str, type_hint: str, candidate: dict) -> float:
    candidate_title = str(candidate.get("l", ""))
    score = SequenceMatcher(None, normalize(title), normalize(candidate_title)).ratio() * 6
    if normalize(title) == normalize(candidate_title):
        score += 2
    wanted_tokens = set(normalize(title).split())
    candidate_tokens = set(normalize(candidate_title).split())
    if wanted_tokens:
        score += len(wanted_tokens & candidate_tokens) / len(wanted_tokens)
    score += imdb_year_bonus(year, candidate)
    score += imdb_type_bonus(type_hint, candidate)
    return score


def resolve_imdb_title(title: str, type_hint: str, year: str) -> dict:
    cache_key = (title, type_hint, year)
    cached = IMDB_ID_CACHE.get(cache_key)
    if cached:
        return cached

    slug = imdb_query_slug(title)
    endpoint = f"https://v3.sg.media-imdb.com/suggestion/{slug[0]}/{quote(slug)}.json"
    payload = fetch_json(endpoint)
    candidates = payload.get("d", []) or []
    best = None
    best_score = float("-inf")
    for candidate in candidates[:12]:
        score = score_imdb_candidate(title, year, type_hint, candidate)
        if score > best_score:
            best_score = score
            best = candidate

    if not best or not best.get("id"):
        raise ValueError(f"IMDb title lookup failed for {title}")

    resolved = {
        "id": str(best.get("id")),
        "title": str(best.get("l") or title),
        "year": str(best.get("y") or year or ""),
    }
    IMDB_ID_CACHE[cache_key] = resolved
    return resolved


def ensure_imdb_dataset() -> Path:
    is_fresh = IMDB_DATASET_FILE.exists() and (time.time() - IMDB_DATASET_FILE.stat().st_mtime) < IMDB_DATASET_TTL
    if is_fresh:
        return IMDB_DATASET_FILE

    request = Request("https://datasets.imdbws.com/title.ratings.tsv.gz", headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=60) as response:
        IMDB_DATASET_FILE.write_bytes(response.read())
    return IMDB_DATASET_FILE


def lookup_imdb_ratings(title_ids: set[str]) -> dict[str, dict]:
    missing = {title_id for title_id in title_ids if title_id and title_id not in IMDB_RATING_CACHE}
    if missing:
        dataset_path = ensure_imdb_dataset()
        with gzip.open(dataset_path, "rt", encoding="utf-8", newline="") as handle:
            next(handle, None)
            for line in handle:
                title_id, rating, votes = line.rstrip("\n").split("\t")
                if title_id not in missing:
                    continue
                IMDB_RATING_CACHE[title_id] = {"rating": rating, "votes": votes}
                missing.remove(title_id)
                if not missing:
                    break

    return {title_id: IMDB_RATING_CACHE.get(title_id, {}) for title_id in title_ids}


def fetch_imdb_ratings(items: list[dict]) -> dict:
    resolved_by_label = {}
    wanted_ids = set()

    for item in items:
        label = str(item.get("label") or item.get("title") or "").strip()
        title = str(item.get("title") or label).strip()
        type_hint = str(item.get("type") or "movie").strip().lower()
        year = str(item.get("year") or "").strip()
        if not label or not title:
            continue
        try:
            resolved = resolve_imdb_title(title, type_hint, year)
        except (HTTPError, ValueError, json.JSONDecodeError):
            resolved_by_label[label] = {"rating": "", "votes": "", "titleId": "", "resolvedTitle": title}
            continue
        resolved_by_label[label] = {
            "rating": "",
            "votes": "",
            "titleId": resolved["id"],
            "resolvedTitle": resolved["title"],
        }
        wanted_ids.add(resolved["id"])

    ratings = lookup_imdb_ratings(wanted_ids) if wanted_ids else {}
    for payload in resolved_by_label.values():
        title_id = payload.get("titleId")
        if not title_id:
            continue
        rating = ratings.get(title_id, {})
        payload["rating"] = str(rating.get("rating") or "")
        payload["votes"] = str(rating.get("votes") or "")

    return {
        "items": resolved_by_label,
        "source": "IMDb title.ratings dataset + IMDb suggestion lookup",
        "updatedAt": now_label(),
    }


def pick_trailer_stream(info: dict) -> str:
    candidates = []
    for fmt in info.get("formats", []):
        if fmt.get("vcodec") == "none" or fmt.get("acodec") == "none" or not fmt.get("url"):
            continue
        candidates.append(fmt)
    if not candidates:
        return ""

    def rank(fmt: dict) -> tuple:
        ext_penalty = 0 if fmt.get("ext") == "mp4" else 1
        height = int(fmt.get("height") or 480)
        height_penalty = 0 if height <= 720 else height
        return (ext_penalty, height_penalty, abs(height - 480))

    candidates.sort(key=rank)
    return candidates[0].get("url", "")


def fetch_trailer_data(title: str, youtube_id: str, search: str) -> dict:
    cache_key = (youtube_id or title, search)
    cached = TRAILER_CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < TRAILER_TTL:
        return cached[1]

    source = f"https://www.youtube.com/watch?v={youtube_id}" if youtube_id else f"ytsearch1:{search}"
    with YoutubeDL({"quiet": True, "skip_download": True, "noplaylist": True}) as ydl:
        info = ydl.extract_info(source, download=False)
    if info.get("_type") == "playlist":
        entries = [entry for entry in info.get("entries", []) if entry]
        info = entries[0] if entries else {}

    data = {
        "title": info.get("title", title),
        "youtubeId": info.get("id", youtube_id),
        "thumbnail": info.get("thumbnail", ""),
        "streamUrl": pick_trailer_stream(info),
    }
    TRAILER_CACHE[cache_key] = (time.time(), data)
    return data


class WatchlistHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/trailer":
            return self.handle_trailer(parsed)
        if parsed.path == "/api/watch-options":
            return self.handle_watch_options(parsed)
        if parsed.path == "/api/imdb-ratings":
            return self.handle_imdb_ratings(parsed)
        if parsed.path == "/":
            params = parse_qs(parsed.query)
            if params.get("v", [""])[0] != APP_VERSION:
                return self.redirect(f"/?v={APP_VERSION}")
            self.path = "/index.html"
        return super().do_GET()

    def log_message(self, format: str, *args) -> None:
        return

    def end_headers(self) -> None:
        # Keep the local app fresh across Safari and in-app browser reloads.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def handle_trailer(self, parsed):
        query = parse_qs(parsed.query)
        title = query.get("title", ["Trailer"])[0]
        youtube_id = query.get("id", [""])[0]
        search = query.get("search", [title])[0]
        try:
            payload = fetch_trailer_data(title, youtube_id, search)
            self.send_json(payload)
        except Exception as error:
            self.send_json({"error": str(error), "streamUrl": ""}, status=500)

    def handle_watch_options(self, parsed):
        query = parse_qs(parsed.query)
        title = query.get("title", [""])[0]
        type_hint = query.get("type", ["movie"])[0]
        year = query.get("year", [""])[0]
        try:
            payload = fetch_watch_data(title, type_hint, year)
            self.send_json(payload)
        except Exception as error:
            self.send_json({"error": str(error), "offers": [], "summary": "", "updatedAt": ""}, status=500)

    def handle_imdb_ratings(self, parsed):
        query = parse_qs(parsed.query)
        raw_items = query.get("items", ["[]"])[0]
        try:
            items = json.loads(raw_items)
            payload = fetch_imdb_ratings(items if isinstance(items, list) else [])
            self.send_json(payload)
        except Exception as error:
            self.send_json({"error": str(error), "items": {}, "updatedAt": ""}, status=500)


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("WATCHLIST_PORT", "8765"))
    server = ThreadingHTTPServer(("127.0.0.1", port), WatchlistHandler)
    print(f"Serving Ryan's Watchlist at http://127.0.0.1:{port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
