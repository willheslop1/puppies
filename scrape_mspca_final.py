"""
MSPCA dog scraper + hypoallergenic-focused email report.

Primary path uses MSPCA's AJAX search endpoint and enriches missing breed data
from detail pages, with cached breed reuse from prior state to reduce requests.
"""

import argparse
import difflib
import html
import json
import logging
import os
import re
import smtplib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.mspca.org/adoption-search/"
DOGS_QUERY = "?type=dog"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; mspca-adoption-scraper/2.0; +https://example.com)"
}
MSPCA_AJAX_CONFIG_RE = re.compile(r"var\s+mspca_ajax\s*=\s*(\{.*?\});", re.S)

HYPO_RULE_THRESHOLD = 65
NEAR_HYPO_MAX_ITEMS = 5
MODEL_SCORE_HISTORY_MAX = 5000
DETAIL_FETCH_WORKERS = 8
HYPO_REGEX_RULES = [
    ("explicit_hypoallergenic", re.compile(r"\bhypoallergenic\b", re.IGNORECASE), 95),
    ("doodle_family", re.compile(r"\b[a-z]*doodle\b", re.IGNORECASE), 72),
    ("poo_family", re.compile(r"\b[a-z]+poo\b", re.IGNORECASE), 72),
    ("hairless_keyword", re.compile(r"\bhairless\b", re.IGNORECASE), 75),
]
LOW_SHEDDING_CANONICAL_BREEDS = [
    "affenpinscher",
    "afghan hound",
    "airedale terrier",
    "american hairless terrier",
    "australian terrier",
    "barbet",
    "basenji",
    "bedlington terrier",
    "bichon frise",
    "bolognese",
    "border terrier",
    "cairn terrier",
    "chinese crested",
    "coton de tulear",
    "dandie dinmont terrier",
    "giant schnauzer",
    "havanese",
    "irish water spaniel",
    "kerry blue terrier",
    "komondor",
    "lagotto romagnolo",
    "lakeland terrier",
    "lhasa apso",
    "lowchen",
    "maltese",
    "miniature schnauzer",
    "norfolk terrier",
    "norwich terrier",
    "peruvian inca orchid",
    "poodle",
    "portuguese water dog",
    "puli",
    "scottish terrier",
    "sealyham terrier",
    "shih tzu",
    "silky terrier",
    "soft coated wheaten terrier",
    "spanish water dog",
    "standard schnauzer",
    "tibetan terrier",
    "west highland white terrier",
    "welsh terrier",
    "wire fox terrier",
    "xoloitzcuintli",
    "yorkshire terrier",
]
LOW_SHEDDING_FUZZY_TERMS = list(LOW_SHEDDING_CANONICAL_BREEDS) + [
    "lagotto",
    "wheaten terrier",
    "wheaten",
    "mini schnauzer",
    "toy poodle",
    "standard poodle",
    "miniature poodle",
]
LOW_SHEDDING_BREED_SET = set(LOW_SHEDDING_CANONICAL_BREEDS)
HEAVY_SHEDDING_CANONICAL_BREEDS = [
    "akita",
    "alaskan malamute",
    "australian cattle dog",
    "beagle",
    "bernese mountain dog",
    "bloodhound",
    "border collie",
    "boxer",
    "bulldog",
    "cane corso",
    "chow chow",
    "cocker spaniel",
    "doberman pinscher",
    "english setter",
    "german shepherd",
    "golden retriever",
    "great dane",
    "great pyrenees",
    "husky",
    "keeshond",
    "labrador retriever",
    "mastiff",
    "newfoundland",
    "pit bull terrier",
    "pointer",
    "pug",
    "rottweiler",
    "saint bernard",
    "samoyed",
    "shiba inu",
    "siberian husky",
]
HEAVY_SHEDDING_BREED_SET = set(HEAVY_SHEDDING_CANONICAL_BREEDS)
LOW_SHEDDING_ALIASES = {
    "westie": "west highland white terrier",
    "yorkie": "yorkshire terrier",
    "scottie": "scottish terrier",
    "xolo": "xoloitzcuintli",
    "portie": "portuguese water dog",
    "lagotto": "lagotto romagnolo",
    "wheaten terrier": "soft coated wheaten terrier",
    "wheaten": "soft coated wheaten terrier",
    "mini schnauzer": "miniature schnauzer",
    "miniature schnauzer": "miniature schnauzer",
    "standard schnauzer": "standard schnauzer",
    "giant schnauzer": "giant schnauzer",
    "mini poodle": "poodle",
    "toy poodle": "poodle",
    "standard poodle": "poodle",
    "miniature poodle": "poodle",
    "shitzu": "shih tzu",
    "shihtzu": "shih tzu",
    "shi tzu": "shih tzu",
    "l owchen": "lowchen",
}


def _normalize_breed_text(s: str) -> str:
    lowered = _clean(s).lower()
    lowered = re.sub(r"[^a-z0-9/,+&;|() -]", " ", lowered)
    lowered = lowered.replace("(", " ").replace(")", " ")
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _breed_segments(normalized_breed: str) -> List[str]:
    cleaned = re.sub(
        r"\b(mix(?:ed)?|cross(?:breed)?|xbreed|hybrid|blend)\b",
        "/",
        normalized_breed,
    )
    cleaned = re.sub(r"\b(with|and)\b", "/", cleaned)
    cleaned = re.sub(r"\s+x\s+", "/", cleaned)
    cleaned = cleaned.replace("&", "/").replace("+", "/").replace(";", "/").replace("|", "/")
    parts = [p.strip() for p in re.split(r"[\/,]", cleaned) if p.strip()]
    return parts


def _canonicalize_breed(part: str) -> str:
    p = part.strip()
    if not p:
        return ""
    if p in LOW_SHEDDING_ALIASES:
        return LOW_SHEDDING_ALIASES[p]
    return p


def _contains_any_breed_name(part: str, breed_set: set[str]) -> str:
    for breed_name in breed_set:
        if f" {breed_name} " in f" {part} ":
            return breed_name
    return ""


def _hypo_confidence(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= HYPO_RULE_THRESHOLD:
        return "medium"
    return "low"


def _score_hypoallergenic_proxy(breed: str) -> Dict[str, Any]:
    """
    Evidence-informed proxy for low-shedding "hypoallergenic-like" breeds.
    Studies show no breed is reliably hypoallergenic for Can f 1 exposure:
    - 10.2500/ajra.2011.25.3606
    - 10.1016/j.jaci.2012.05.013
    - 10.1111/j.1398-9995.2005.00824.x
    """
    normalized = _normalize_breed_text(breed)
    if not normalized:
        return {
            "score": 0,
            "confidence": "low",
            "is_candidate": 0,
            "reasons": [],
        }

    candidates = [_canonicalize_breed(normalized)] + [
        _canonicalize_breed(p) for p in _breed_segments(normalized)
    ]
    candidates = [p for p in dict.fromkeys(candidates) if p]

    reasons: List[str] = []
    seen_reasons = set()

    def add_reason(reason: str) -> None:
        if reason not in seen_reasons:
            reasons.append(reason)
            seen_reasons.add(reason)

    regex_points = 0
    for rule_name, regex, weight in HYPO_REGEX_RULES:
        if regex.search(normalized):
            regex_points = max(regex_points, weight)
            add_reason(f"{rule_name}(+{weight})")

    breed_points = 0
    penalties = 0

    for part in candidates:
        if part in LOW_SHEDDING_BREED_SET:
            breed_points = max(breed_points, 82)
            add_reason(f"exact_low_shedding:{part}(+82)")
        else:
            contained_low = _contains_any_breed_name(part, LOW_SHEDDING_BREED_SET)
            if contained_low:
                breed_points = max(breed_points, 68)
                add_reason(f"contains_low_shedding:{contained_low}(+68)")
            else:
                fuzzy_match = difflib.get_close_matches(
                    part,
                    LOW_SHEDDING_FUZZY_TERMS,
                    n=1,
                    cutoff=0.90,
                )
                if fuzzy_match:
                    breed_points = max(breed_points, 50)
                    add_reason(f"fuzzy_low_shedding:{fuzzy_match[0]}(+50)")

        if part in HEAVY_SHEDDING_BREED_SET:
            penalties += 10
            add_reason(f"heavy_shedding_penalty:{part}(-10)")
        else:
            contained_heavy = _contains_any_breed_name(part, HEAVY_SHEDDING_BREED_SET)
            if contained_heavy:
                penalties += 6
                add_reason(f"contains_heavy_shedding:{contained_heavy}(-6)")

    positive_points = max(regex_points, breed_points)
    if positive_points > 0 and penalties > 0:
        penalties += 4
        add_reason("mixed_with_heavy_shedding_penalty(-4)")

    penalties = min(penalties, 40)
    score = max(0, min(100, positive_points - penalties))
    confidence = _hypo_confidence(score)
    is_candidate = 1 if score >= HYPO_RULE_THRESHOLD else 0

    return {
        "score": score,
        "confidence": confidence,
        "is_candidate": is_candidate,
        "reasons": reasons,
    }


def _clean(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _progress(page: int, max_pages: int) -> None:
    bar_width = 24
    ratio = min(page / max_pages, 1.0)
    filled = int(bar_width * ratio)
    bar = "#" * filled + "-" * (bar_width - filled)
    print(f"\rProgress: [{bar}] page {page}/{max_pages}", end="", flush=True)


def _setup_logging(log_path: str, verbose: bool) -> None:
    handlers = [
        logging.FileHandler(log_path),
        logging.StreamHandler(),
    ]
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


@dataclass
class Dog:
    name: str
    breed: str
    location: str
    gender: str
    age: str
    source_url: str
    detail_url: str
    image_url: str
    raw_stats: Dict[str, str]


def fetch(url: str, max_retries: int = 3, backoff_s: float = 1.0) -> str:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} on {url}", response=resp
                )
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_for = backoff_s * (2 ** (attempt - 1))
                logging.warning(
                    "fetch failed (attempt %s/%s): %s; retrying in %.1fs",
                    attempt,
                    max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
            else:
                logging.error("fetch failed (attempt %s/%s): %s", attempt, max_retries, exc)
                raise
    raise last_exc


def fetch_json_post(url: str, data: Dict[str, Any], max_retries: int = 3, backoff_s: float = 1.0) -> Dict[str, Any]:
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=HEADERS, data=data, timeout=30)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} on POST {url}", response=resp
                )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_for = backoff_s * (2 ** (attempt - 1))
                logging.warning(
                    "post failed (attempt %s/%s): %s; retrying in %.1fs",
                    attempt,
                    max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
            else:
                logging.error("post failed (attempt %s/%s): %s", attempt, max_retries, exc)
                raise
    raise last_exc


def get_mspca_ajax_config(max_retries: int = 3, backoff_s: float = 1.0) -> Dict[str, str]:
    html = fetch(BASE, max_retries=max_retries, backoff_s=backoff_s)
    match = MSPCA_AJAX_CONFIG_RE.search(html)
    if not match:
        raise RuntimeError("mspca_ajax config not found in adoption-search HTML")
    cfg = json.loads(match.group(1))
    ajax_url = _clean(cfg.get("ajax_url", ""))
    nonce = _clean(cfg.get("nonce", ""))
    if not ajax_url or not nonce:
        raise RuntimeError("mspca_ajax config missing ajax_url/nonce")
    return {"ajax_url": ajax_url, "nonce": nonce}


def _extract_background_image_url(style_value: str) -> str:
    style = style_value or ""
    match = re.search(r"url\((['\"]?)(.*?)\1\)", style, re.IGNORECASE)
    if not match:
        return ""
    return _clean(match.group(2))


def parse_dogs_from_ajax_html(html_fragment: str, page_url: str, debug: bool = False) -> List[Dog]:
    soup = BeautifulSoup(html_fragment or "", "html.parser")
    dogs: List[Dog] = []
    cards = soup.select("div.mspca-pet-card")

    for card in cards:
        link_el = card.select_one("a.mspca-pet-card-link[href]")
        if not link_el:
            continue
        detail_url = urljoin(page_url, link_el.get("href", ""))

        image_div = card.select_one("div.mspca-pet-card-image")
        image_url = ""
        if image_div:
            image_url = _extract_background_image_url(image_div.get("style", ""))
            image_url = urljoin(page_url, image_url) if image_url else ""

        name_el = card.select_one(".mspca-pet-name")
        loc_el = card.select_one(".mspca-pet-location")
        age_el = card.select_one(".mspca-pet-age")
        name = _clean(name_el.get_text() if name_el else "")
        location = _clean(loc_el.get_text() if loc_el else "")
        age = _clean(age_el.get_text() if age_el else "")

        if not detail_url or not location:
            continue

        stats = {"location": location, "age": age}
        dogs.append(Dog(
            name=name,
            breed="",
            location=location,
            gender="",
            age=age,
            source_url=page_url,
            detail_url=detail_url,
            image_url=image_url,
            raw_stats=stats,
        ))

    if debug:
        logging.debug("ajax cards parsed=%s", len(dogs))
    return dogs


def enrich_dog_from_detail(dog: Dog, max_retries: int = 3, backoff_s: float = 1.0) -> Dog:
    if dog.breed:
        return dog
    if not dog.detail_url:
        return dog
    try:
        html = fetch(dog.detail_url, max_retries=max_retries, backoff_s=backoff_s)
    except Exception:
        return dog
    soup = BeautifulSoup(html, "html.parser")
    breed_el = soup.select_one(".mspca-pet-breed")
    if breed_el:
        dog.breed = _clean(breed_el.get_text())
    return dog


def enrich_missing_breeds(
    dogs: List[Dog],
    max_retries: int = 3,
    backoff_s: float = 1.0,
    max_workers: int = DETAIL_FETCH_WORKERS,
) -> None:
    missing = [dog for dog in dogs if dog.detail_url and not dog.breed]
    if not missing:
        return

    workers = max(1, min(max_workers, len(missing)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_dog = {
            executor.submit(enrich_dog_from_detail, dog, max_retries, backoff_s): dog
            for dog in missing
        }
        for future in as_completed(future_to_dog):
            try:
                future.result()
            except Exception as exc:
                dog = future_to_dog[future]
                logging.debug("detail enrichment failed for %s: %s", dog.detail_url, exc)


def build_breed_cache(prior_state: Dict[str, Any]) -> Dict[str, str]:
    cache: Dict[str, str] = {}
    if not isinstance(prior_state, dict):
        return cache

    for section in ("dogs", "all_dogs"):
        state_map = prior_state.get(section, {})
        if not isinstance(state_map, dict):
            continue
        for _, snapshot in state_map.items():
            if not isinstance(snapshot, dict):
                continue
            detail_url = _safe_text(snapshot.get("detail_url", ""))
            breed = _safe_text(snapshot.get("breed", ""))
            if detail_url and breed:
                cache[detail_url] = breed
    return cache


def parse_dogs_from_page(html: str, page_url: str, debug: bool = False) -> List[Dog]:
    soup = BeautifulSoup(html, "html.parser")
    dogs: List[Dog] = []
    skipped_no_cardinfo = 0
    skipped_no_location = 0
    empty_detail_url = 0

    # Heuristic: adoption "cards" are anchors that contain a div.cardInfo
    for a in soup.find_all("a", href=True):
        card_info = a.find("div", class_="cardInfo")
        if not card_info:
            skipped_no_cardinfo += 1
            continue

        detail_url = urljoin(page_url, a["href"])
        if not detail_url:
            empty_detail_url += 1

        img = a.find("img", class_="petImageMain")
        image_url = urljoin(page_url, img["src"]) if img and img.get("src") else ""

        # Name is typically: <h1><strong>NAME</strong></h1>
        h1 = card_info.find("h1")
        strong = h1.find("strong") if h1 else None
        name = _clean(strong.get_text()) if strong else _clean(h1.get_text() if h1 else "")

        # Breed line is typically in <h2>
        h2 = card_info.find("h2")
        breed = _clean(h2.get_text() if h2 else "")

        # Stats live under div.petStats with children like div.petStatContent-location/gender/age...
        stats: Dict[str, str] = {}
        stats_wrap = card_info.find("div", class_="petStats")

        if stats_wrap:
            # Capture any stat blocks; this will automatically include new stats MSPCA adds later.
            for stat_div in stats_wrap.find_all("div", class_=re.compile(r"^petStatContent-")):
                label_el = stat_div.find("label")
                value_el = stat_div.find("span")

                key = _clean(label_el.get_text()).lower() if label_el else ""
                key = key.rstrip(":")
                val = _clean(value_el.get_text()) if value_el else ""

                # Fallback: derive key from class suffix if label missing
                if not key:
                    classes = stat_div.get("class", [])
                    suffix = next(
                        (c.split("petStatContent-")[-1] for c in classes if c.startswith("petStatContent-")),
                        ""
                    )
                    key = suffix.lower()

                if key:
                    stats[key] = val

        location = stats.get("location", "")
        gender = stats.get("gender", "")
        age = stats.get("age", "")

        # Require location to treat as a valid card
        if not location:
            skipped_no_location += 1
            continue

        dogs.append(Dog(
            name=name,
            breed=breed,
            location=location,
            gender=gender,
            age=age,
            source_url=page_url,
            detail_url=detail_url,
            image_url=image_url,
            raw_stats=stats
        ))

    if debug:
        print(
            "debug: "
            f"cards_found={len(dogs)} "
            f"skipped_no_cardinfo={skipped_no_cardinfo} "
            f"skipped_no_location={skipped_no_location} "
            f"empty_detail_url={empty_detail_url}"
        )

    # De-dup: best key is detail_url, but keep a fallback composite key if needed.
    uniq: Dict[str, Dog] = {}
    for d in dogs:
        key = d.detail_url or "|".join([
            d.name.lower(),
            d.breed.lower(),
            d.location.lower(),
            d.gender.lower(),
            d.age.lower(),
        ])
        uniq[key] = d

    return list(uniq.values())


def page_url(page: int) -> str:
    # Page 1 is the base URL. Page 2+ uses /page/N/
    if page == 1:
        return f"{BASE}{DOGS_QUERY}"
    return f"{BASE}page/{page}/{DOGS_QUERY}"


def _scrape_all_dogs_legacy(
    max_pages: int = 50,
    sleep_s: float = 1.0,
    debug: bool = False,
    max_zero_new_pages: int = 3,
    show_progress: bool = True,
    max_retries: int = 3,
    backoff_s: float = 1.0,
) -> List[Dog]:
    all_dogs: List[Dog] = []
    seen_urls = set()
    zero_new_pages = 0

    for p in range(1, max_pages + 1):
        url = page_url(p)
        try:
            html = fetch(url, max_retries=max_retries, backoff_s=backoff_s)
        except requests.HTTPError as exc:
            resp = exc.response
            if resp is not None and resp.status_code == 404:
                break
            raise

        dogs = parse_dogs_from_page(html, url, debug=debug)
        new_urls = {
            d.detail_url
            for d in dogs
            if d.detail_url and d.detail_url not in seen_urls
        }
        if debug:
            logging.debug(
                "legacy page=%s new_urls=%s total_seen=%s", p, len(new_urls), len(seen_urls)
            )
        if show_progress:
            _progress(p, max_pages)

        if p > 1 and len(dogs) == 0:
            break
        if p > 1 and len(new_urls) == 0:
            zero_new_pages += 1
            if zero_new_pages >= max_zero_new_pages:
                break
        else:
            zero_new_pages = 0

        seen_urls.update(new_urls)
        all_dogs.extend(dogs)
        time.sleep(sleep_s)

    return all_dogs


def scrape_all_dogs(
    max_pages: int = 50,
    sleep_s: float = 1.0,
    debug: bool = False,
    max_zero_new_pages: int = 3,
    show_progress: bool = True,
    max_retries: int = 3,
    backoff_s: float = 1.0,
    breed_cache: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    all_dogs: List[Dog] = []
    seen_urls = set()
    zero_new_pages = 0

    try:
        cfg = get_mspca_ajax_config(max_retries=max_retries, backoff_s=backoff_s)
        random_seed = int(time.time())
        ajax_max_pages = max_pages

        for p in range(1, max_pages + 1):
            payload = {
                "action": "mspca_filter_pets",
                "nonce": cfg["nonce"],
                "animal": "dog",
                "breed": "",
                "location": "",
                "sex": "",
                "age": "",
                "paged": p,
                "random_seed": random_seed,
            }
            ajax_resp = fetch_json_post(
                cfg["ajax_url"],
                payload,
                max_retries=max_retries,
                backoff_s=backoff_s,
            )
            if not ajax_resp.get("success"):
                break
            data = ajax_resp.get("data", {}) or {}
            ajax_max_pages = max(1, _safe_int(data.get("max_pages", ajax_max_pages), ajax_max_pages))
            html_fragment = data.get("html", "")
            dogs = parse_dogs_from_ajax_html(html_fragment, BASE, debug=debug)
            if breed_cache:
                for dog in dogs:
                    cached_breed = breed_cache.get(dog.detail_url, "")
                    if cached_breed:
                        dog.breed = cached_breed

            enrich_missing_breeds(
                dogs,
                max_retries=max_retries,
                backoff_s=backoff_s,
            )
            if breed_cache is not None:
                for dog in dogs:
                    if dog.detail_url and dog.breed:
                        breed_cache[dog.detail_url] = dog.breed

            new_urls = {
                d.detail_url
                for d in dogs
                if d.detail_url and d.detail_url not in seen_urls
            }
            if debug:
                logging.debug(
                    "ajax page=%s new_urls=%s total_seen=%s ajax_max_pages=%s",
                    p,
                    len(new_urls),
                    len(seen_urls),
                    ajax_max_pages,
                )
            if show_progress:
                _progress(min(p, max_pages), max_pages)

            if p > 1 and len(dogs) == 0:
                break
            if p > 1 and len(new_urls) == 0:
                zero_new_pages += 1
                if zero_new_pages >= max_zero_new_pages:
                    break
            else:
                zero_new_pages = 0

            seen_urls.update(new_urls)
            all_dogs.extend(dogs)
            if p >= ajax_max_pages:
                break
            time.sleep(sleep_s)
    except Exception as exc:
        logging.warning("ajax scrape failed, falling back to legacy parser: %s", exc)
        all_dogs = _scrape_all_dogs_legacy(
            max_pages=max_pages,
            sleep_s=sleep_s,
            debug=debug,
            max_zero_new_pages=max_zero_new_pages,
            show_progress=show_progress,
            max_retries=max_retries,
            backoff_s=backoff_s,
        )

    # Flatten dataclasses into rows
    df = (
        pd.DataFrame([asdict(d) for d in all_dogs])
        .drop_duplicates(subset=["detail_url"])
        .reset_index(drop=True)
    )
    if show_progress:
        print()

    # Optional: expand raw_stats keys into separate columns (keeps raw_stats too)
    # This makes it easier to analyze in CSV without parsing JSON-ish dict strings.
    if "raw_stats" in df.columns and not df.empty:
        stats_df = pd.json_normalize(df["raw_stats"]).add_prefix("stat_")
        df = pd.concat([df.drop(columns=["raw_stats"]), stats_df, df[["raw_stats"]]], axis=1)

    if "breed" in df.columns:
        scored = df["breed"].fillna("").apply(_score_hypoallergenic_proxy)
        df["hypo_score"] = scored.apply(lambda x: x["score"])
        df["hypo_confidence"] = scored.apply(lambda x: x["confidence"])
        df["hypo_reasons"] = scored.apply(lambda x: "; ".join(x["reasons"]))
        df["is_hypoallergenic"] = scored.apply(lambda x: x["is_candidate"])

    return df


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    is_na = pd.isna(value)
    if is_na is pd.NA:
        return ""
    if isinstance(is_na, bool) and is_na:
        return ""
    return _clean(str(value))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _prune_score_by_dog(
    score_by_dog: Dict[str, int],
    all_dogs_state: Dict[str, Dict[str, str]],
    max_items: int,
) -> Dict[str, int]:
    if len(score_by_dog) <= max_items:
        return score_by_dog

    ranked = sorted(
        score_by_dog.items(),
        key=lambda kv: _safe_text(all_dogs_state.get(kv[0], {}).get("last_seen", "")),
        reverse=True,
    )
    return dict(ranked[:max_items])


def _dog_key_from_row(row: pd.Series) -> str:
    detail_url = _safe_text(row.get("detail_url", ""))
    if detail_url:
        return detail_url
    return "|".join([
        _safe_text(row.get("name", "")).lower(),
        _safe_text(row.get("breed", "")).lower(),
        _safe_text(row.get("location", "")).lower(),
        _safe_text(row.get("gender", "")).lower(),
        _safe_text(row.get("age", "")).lower(),
    ])


def load_hypo_state(state_path: str) -> Dict[str, Any]:
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {"dogs": {}}
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("failed to load state file %s: %s", state_path, exc)
        return {"dogs": {}}

    dogs = data.get("dogs", {})
    if not isinstance(dogs, dict):
        dogs = {}
    all_dogs = data.get("all_dogs", {})
    if not isinstance(all_dogs, dict):
        all_dogs = {}
    model = data.get("model", {})
    if not isinstance(model, dict):
        model = {}
    return {
        "dogs": dogs,
        "all_dogs": all_dogs,
        "model": model,
    }


def save_hypo_state(state_path: str, state_payload: Dict[str, Any]) -> None:
    state_dir = os.path.dirname(state_path)
    if state_dir:
        os.makedirs(state_dir, exist_ok=True)

    payload = {
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "dogs": state_payload.get("dogs", {}),
        "all_dogs": state_payload.get("all_dogs", {}),
        "model": state_payload.get("model", {}),
    }
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def build_hypo_change_sets(
    df: pd.DataFrame,
    prior_state: Dict[str, Any],
) -> Dict[str, Any]:
    prior_dogs = prior_state.get("dogs", {}) if isinstance(prior_state, dict) else {}
    prior_all_dogs = prior_state.get("all_dogs", {}) if isinstance(prior_state, dict) else {}
    prior_model = prior_state.get("model", {}) if isinstance(prior_state, dict) else {}
    prior_keys = set(prior_dogs.keys())
    now_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    working_df = df.copy()
    if not working_df.empty:
        working_df["dog_key"] = working_df.apply(_dog_key_from_row, axis=1)
    else:
        working_df["dog_key"] = pd.Series(dtype=str)

    tracked_fields = [
        "name",
        "breed",
        "location",
        "gender",
        "age",
        "detail_url",
        "image_url",
        "hypo_score",
        "hypo_confidence",
        "hypo_reasons",
    ]
    diff_fields = ["name", "breed", "location", "gender", "age"]

    def row_to_report(row: pd.Series, status: str, changed_fields: str = "") -> Dict[str, str]:
        return {
            "status": status,
            "changed_fields": changed_fields,
            "name": _safe_text(row.get("name", "")),
            "breed": _safe_text(row.get("breed", "")),
            "location": _safe_text(row.get("location", "")),
            "gender": _safe_text(row.get("gender", "")),
            "age": _safe_text(row.get("age", "")),
            "detail_url": _safe_text(row.get("detail_url", "")),
            "image_url": _safe_text(row.get("image_url", "")),
            "hypo_score": _safe_text(row.get("hypo_score", "")),
            "hypo_confidence": _safe_text(row.get("hypo_confidence", "")),
            "hypo_reasons": _safe_text(row.get("hypo_reasons", "")),
            "first_seen": _safe_text(row.get("first_seen", "")),
            "last_seen": _safe_text(row.get("last_seen", "")),
        }

    if "is_hypoallergenic" not in working_df.columns:
        hypo_df = pd.DataFrame(columns=working_df.columns)
    else:
        hypo_df = working_df[working_df["is_hypoallergenic"] == 1].copy()

    new_rows: List[Dict[str, str]] = []
    existing_rows: List[Dict[str, str]] = []
    current_state_dogs: Dict[str, Dict[str, str]] = {}
    current_keys = set()

    for _, row in hypo_df.iterrows():
        dog_key = _safe_text(row.get("dog_key", ""))
        if not dog_key:
            continue
        current_keys.add(dog_key)

        dog_snapshot = {field: _safe_text(row.get(field, "")) for field in tracked_fields}
        prev_snapshot = prior_dogs.get(dog_key, {})

        changed_fields_list = []
        if prev_snapshot:
            for field in diff_fields:
                if dog_snapshot[field] != _safe_text(prev_snapshot.get(field, "")):
                    changed_fields_list.append(field)

        first_seen = _safe_text(prev_snapshot.get("first_seen", "")) or now_utc
        dog_snapshot["first_seen"] = first_seen
        dog_snapshot["last_seen"] = now_utc
        current_state_dogs[dog_key] = dog_snapshot

        report_row = {
            "status": "new" if not prev_snapshot else ("existing_updated" if changed_fields_list else "existing"),
            "changed_fields": ", ".join(changed_fields_list),
            **dog_snapshot,
        }
        if report_row["status"] == "new":
            new_rows.append(report_row)
        else:
            existing_rows.append(report_row)

    removed_rows: List[Dict[str, str]] = []
    for dog_key in sorted(prior_keys - current_keys):
        prev_snapshot = prior_dogs.get(dog_key, {})
        removed_rows.append({
            "status": "no_longer_listed",
            "changed_fields": "",
            "name": _safe_text(prev_snapshot.get("name", "")),
            "breed": _safe_text(prev_snapshot.get("breed", "")),
            "location": _safe_text(prev_snapshot.get("location", "")),
            "gender": _safe_text(prev_snapshot.get("gender", "")),
            "age": _safe_text(prev_snapshot.get("age", "")),
            "detail_url": _safe_text(prev_snapshot.get("detail_url", "")),
            "image_url": _safe_text(prev_snapshot.get("image_url", "")),
            "hypo_score": _safe_text(prev_snapshot.get("hypo_score", "")),
            "hypo_confidence": _safe_text(prev_snapshot.get("hypo_confidence", "")),
            "hypo_reasons": _safe_text(prev_snapshot.get("hypo_reasons", "")),
            "first_seen": _safe_text(prev_snapshot.get("first_seen", "")),
            "last_seen": _safe_text(prev_snapshot.get("last_seen", "")),
        })

    new_rows.sort(key=lambda r: (r["name"].lower(), r["detail_url"].lower()))
    existing_rows.sort(key=lambda r: (r["name"].lower(), r["detail_url"].lower()))
    removed_rows.sort(key=lambda r: (r["name"].lower(), r["detail_url"].lower()))

    current_non_hypo_score_by_dog: Dict[str, int] = {}
    if {"is_hypoallergenic", "hypo_score", "dog_key"}.issubset(working_df.columns):
        non_hypo_scores_df = working_df[working_df["is_hypoallergenic"] == 0][["dog_key", "hypo_score"]]
        for _, score_row in non_hypo_scores_df.iterrows():
            dog_key = _safe_text(score_row.get("dog_key", ""))
            if not dog_key:
                continue
            score = _safe_int(score_row.get("hypo_score", 0), 0)
            if 0 < score < HYPO_RULE_THRESHOLD:
                current_non_hypo_score_by_dog[dog_key] = score

    historical_score_by_dog_raw = prior_model.get("score_by_dog", {})
    if not isinstance(historical_score_by_dog_raw, dict):
        historical_score_by_dog_raw = {}
    historical_non_hypo_score_by_dog = {
        _safe_text(k): _safe_int(v, 0)
        for k, v in historical_score_by_dog_raw.items()
        if _safe_text(k)
    }
    # Backward-compat: if older state only has score_history, seed from that.
    if not historical_non_hypo_score_by_dog and isinstance(prior_model.get("score_history", []), list):
        for idx, score in enumerate(prior_model.get("score_history", [])):
            s = _safe_int(score, 0)
            if 0 < s < HYPO_RULE_THRESHOLD:
                historical_non_hypo_score_by_dog[f"legacy_{idx}"] = s

    near_rows: List[Dict[str, str]] = []
    if {"is_hypoallergenic", "hypo_score", "dog_key"}.issubset(working_df.columns):
        non_hypo_df = working_df[
            (working_df["is_hypoallergenic"] == 0)
            & (working_df["hypo_score"] > 0)
            & (working_df["hypo_score"] < HYPO_RULE_THRESHOLD)
        ].copy()
        non_hypo_df = non_hypo_df.sort_values(by=["hypo_score", "name"], ascending=[False, True])
        chosen_df = non_hypo_df.head(NEAR_HYPO_MAX_ITEMS)
        if not chosen_df.empty:
            chosen_df = chosen_df.sort_values(by=["hypo_score", "name"], ascending=[False, True]).head(NEAR_HYPO_MAX_ITEMS)
            for _, row in chosen_df.iterrows():
                near_rows.append(row_to_report(row, status="near_hypo"))

    all_dogs_state = dict(prior_all_dogs)
    for _, row in working_df.iterrows():
        dog_key = _safe_text(row.get("dog_key", ""))
        if not dog_key:
            continue
        prev = prior_all_dogs.get(dog_key, {})
        seen_count = _safe_int(prev.get("seen_count", 0), 0) + 1
        current_score = _safe_int(row.get("hypo_score", 0), 0)
        max_score = max(_safe_int(prev.get("max_score", 0), 0), current_score)
        first_seen = _safe_text(prev.get("first_seen", "")) or now_utc
        all_dogs_state[dog_key] = {
            "name": _safe_text(row.get("name", "")),
            "breed": _safe_text(row.get("breed", "")),
            "location": _safe_text(row.get("location", "")),
            "gender": _safe_text(row.get("gender", "")),
            "age": _safe_text(row.get("age", "")),
            "detail_url": _safe_text(row.get("detail_url", "")),
            "image_url": _safe_text(row.get("image_url", "")),
            "last_score": str(current_score),
            "max_score": str(max_score),
            "last_is_hypo": str(_safe_int(row.get("is_hypoallergenic", 0), 0)),
            "seen_count": str(seen_count),
            "first_seen": first_seen,
            "last_seen": now_utc,
        }

    merged_score_by_dog = dict(historical_non_hypo_score_by_dog)
    merged_score_by_dog.update(current_non_hypo_score_by_dog)
    if any(not k.startswith("legacy_") for k in merged_score_by_dog):
        merged_score_by_dog = {
            k: v for k, v in merged_score_by_dog.items() if not k.startswith("legacy_")
        }
    merged_score_by_dog = _prune_score_by_dog(
        merged_score_by_dog,
        all_dogs_state,
        MODEL_SCORE_HISTORY_MAX,
    )
    model_score_history = list(merged_score_by_dog.values())
    model_state = {
        "run_count": _safe_int(prior_model.get("run_count", 0), 0) + 1,
        "score_by_dog": merged_score_by_dog,
        "score_history": model_score_history,
        "unique_dogs_in_model": len(merged_score_by_dog),
        "last_near_count": len(near_rows),
    }

    return {
        "new_rows": new_rows,
        "existing_rows": existing_rows,
        "removed_rows": removed_rows,
        "near_rows": near_rows,
        "current_state_dogs": current_state_dogs,
        "state_payload": {
            "dogs": current_state_dogs,
            "all_dogs": all_dogs_state,
            "model": model_state,
        },
    }


def _truncate(value: str, max_len: int) -> str:
    text = _safe_text(value)
    if len(text) <= max_len:
        return text
    if max_len <= 1:
        return text[:max_len]
    return text[: max_len - 1] + "…"


def _row_cells(row: Dict[str, str], include_change_flag: bool = False) -> Dict[str, str]:
    updated = ""
    if include_change_flag and row.get("changed_fields"):
        updated = "yes"
    return {
        "name": _safe_text(row.get("name", "")) or "Unknown name",
        "breed": _safe_text(row.get("breed", "")) or "Unknown breed",
        "location": _safe_text(row.get("location", "")) or "Unknown location",
        "age": _safe_text(row.get("age", "")) or "Unknown age",
        "score": _safe_text(row.get("hypo_score", "")),
        "confidence": _safe_text(row.get("hypo_confidence", "")),
        "updated": updated,
        "url": _safe_text(row.get("detail_url", "")),
    }


def _render_text_table(rows: List[Dict[str, str]], include_change_flag: bool = False) -> List[str]:
    if not rows:
        return ["(none)"]

    columns = [
        ("Name", "name", 16),
        ("Breed", "breed", 24),
        ("Location", "location", 12),
        ("Age", "age", 10),
        ("Score", "score", 5),
        ("Conf", "confidence", 6),
        ("Upd", "updated", 3),
    ]
    header = " | ".join(title.ljust(width) for title, _, width in columns)
    divider = "-+-".join("-" * width for _, _, width in columns)
    lines = [header, divider]

    for row in rows:
        cells = _row_cells(row, include_change_flag=include_change_flag)
        line = " | ".join(
            _truncate(cells[key], width).ljust(width) for _, key, width in columns
        )
        lines.append(line)
        if cells["url"]:
            lines.append(f"  {cells['url']}")
    return lines


def _render_html_table(rows: List[Dict[str, str]], include_change_flag: bool = False) -> str:
    if not rows:
        return "<p><em>None</em></p>"

    header_cells = "".join(
        f"<th style='text-align:left;padding:8px;border-bottom:1px solid #ddd'>{label}</th>"
        for label in ("Name", "Breed", "Location", "Age", "Score", "Confidence", "Updated", "Link")
    )

    body_rows = []
    for row in rows:
        cells = _row_cells(row, include_change_flag=include_change_flag)
        link = ""
        if cells["url"]:
            safe_url = html.escape(cells["url"])
            link = f"<a href='{safe_url}'>View</a>"
        body_rows.append(
            "<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['name'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['breed'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['location'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['age'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['score'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['confidence'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['updated'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{link}</td>"
            "</tr>"
        )

    return (
        "<table style='border-collapse:collapse;width:100%;max-width:1100px;font-family:Arial,sans-serif;font-size:14px'>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def build_hypo_email_body(
    total_dogs: int,
    new_rows: List[Dict[str, str]],
    existing_rows: List[Dict[str, str]],
    removed_rows: List[Dict[str, str]],
    near_rows: List[Dict[str, str]],
) -> str:
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"MSPCA hypoallergenic update ({now_local})",
        "",
        f"Total dogs scraped: {total_dogs}",
        f"New hypo candidates: {len(new_rows)}",
        f"Existing hypo candidates: {len(existing_rows)}",
        f"Previously flagged, now no longer listed: {len(removed_rows)}",
        (
            "Nearly-hypo candidates: "
            f"{len(near_rows)} "
            f"(positive-score hit only; "
            f"score range 1-{HYPO_RULE_THRESHOLD - 1}, "
            f"cap={NEAR_HYPO_MAX_ITEMS})"
        ),
        f"Rule threshold: score >= {HYPO_RULE_THRESHOLD}",
        "",
        "=== NEW HYPO DOGS ===",
    ]
    lines.extend(_render_text_table(new_rows))

    lines.extend(["", "=== EXISTING HYPO DOGS (updates flagged) ==="])
    lines.extend(_render_text_table(existing_rows, include_change_flag=True))

    lines.extend(["", "=== PREVIOUSLY FLAGGED, NOW NO LONGER LISTED ==="])
    lines.extend(_render_text_table(removed_rows))

    lines.extend(["", f"=== NEARLY-HYPO DOGS (positive score hit, top {NEAR_HYPO_MAX_ITEMS}) ==="])
    lines.extend(_render_text_table(near_rows))

    return "\n".join(lines)


def build_hypo_email_html(
    total_dogs: int,
    new_rows: List[Dict[str, str]],
    existing_rows: List[Dict[str, str]],
    removed_rows: List[Dict[str, str]],
    near_rows: List[Dict[str, str]],
) -> str:
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        "<html><body style='font-family:Arial,sans-serif;color:#222'>"
        f"<h2>MSPCA Hypoallergenic Update ({html.escape(now_local)})</h2>"
        "<p>"
        f"Total dogs scraped: <strong>{total_dogs}</strong><br>"
        f"New hypo candidates: <strong>{len(new_rows)}</strong><br>"
        f"Existing hypo candidates: <strong>{len(existing_rows)}</strong><br>"
        f"Previously flagged, now no longer listed: <strong>{len(removed_rows)}</strong><br>"
        f"Nearly-hypo candidates: <strong>{len(near_rows)}</strong> "
        f"(positive-score hit only, score 1-{HYPO_RULE_THRESHOLD - 1}, cap={NEAR_HYPO_MAX_ITEMS})<br>"
        f"Rule threshold: score &gt;= {HYPO_RULE_THRESHOLD}"
        "</p>"
        "<h3>New Hypo Dogs</h3>"
        f"{_render_html_table(new_rows)}"
        "<h3>Existing Hypo Dogs (updates flagged)</h3>"
        f"{_render_html_table(existing_rows, include_change_flag=True)}"
        "<h3>Previously Flagged, Now No Longer Listed</h3>"
        f"{_render_html_table(removed_rows)}"
        f"<h3>Nearly-Hypo Dogs (positive score hit, top {NEAR_HYPO_MAX_ITEMS})</h3>"
        f"{_render_html_table(near_rows)}"
        "</body></html>"
    )


def send_email_report(
    body: str,
    subject_prefix: str = "MSPCA Hypo Update",
    attachment_path: str = "",
    html_body: str = "",
) -> bool:
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    email_to = os.environ.get("EMAIL_TO", "")

    if not smtp_user or not smtp_pass or not email_to:
        return False

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"{subject_prefix} {now}"

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="text",
                subtype="csv",
                filename=os.path.basename(attachment_path),
            )

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return True
    except Exception as exc:
        logging.error("email send failed: %s", exc)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MSPCA dog adoption scraper")
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--sleep-s", type=float, default=1.0)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--show-progress", action="store_true")
    parser.add_argument("--max-zero-new-pages", type=int, default=3)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff-s", type=float, default=1.0)
    parser.add_argument("--log-path", default="scrape.log")
    parser.add_argument("--state-path", default=".state/mspca_hypo_state.json")
    parser.add_argument("--save-full-csv", action="store_true")
    parser.add_argument("--save-hypo-csv", action="store_true")
    parser.add_argument("--output-prefix", default="")
    args = parser.parse_args()

    _setup_logging(args.log_path, verbose=args.debug)

    prior_state = load_hypo_state(args.state_path)
    breed_cache = build_breed_cache(prior_state)

    df = scrape_all_dogs(
        max_pages=args.max_pages,
        sleep_s=args.sleep_s,
        debug=args.debug,
        show_progress=args.show_progress,
        max_zero_new_pages=args.max_zero_new_pages,
        max_retries=args.max_retries,
        backoff_s=args.backoff_s,
        breed_cache=breed_cache,
    )
    logging.info("scraped %s dogs", len(df))

    change_sets = build_hypo_change_sets(df, prior_state)
    new_rows = change_sets["new_rows"]
    existing_rows = change_sets["existing_rows"]
    removed_rows = change_sets["removed_rows"]
    near_rows = change_sets["near_rows"]

    logging.info(
        "hypo summary: new=%s existing=%s no_longer_listed=%s near=%s",
        len(new_rows),
        len(existing_rows),
        len(removed_rows),
        len(near_rows),
    )

    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M")
    prefix = args.output_prefix

    if args.save_full_csv:
        output_path = f"{prefix}{timestamp}_mspca_dogs_final.csv"
        df.to_csv(output_path, index=False)
        logging.info("saved full dataset: %s", output_path)

    hypo_csv_path = ""
    if args.save_hypo_csv:
        hypo_rows = new_rows + existing_rows
        hypo_df = pd.DataFrame(hypo_rows)
        hypo_csv_path = f"{prefix}{timestamp}_mspca_hypo_dogs.csv"
        hypo_df.to_csv(hypo_csv_path, index=False)
        logging.info("saved hypo dataset: %s", hypo_csv_path)

    save_hypo_state(args.state_path, change_sets["state_payload"])
    logging.info("saved state to %s", args.state_path)

    subject_prefix = (
        "MSPCA Hypo Update "
        f"(new: {len(new_rows)}, existing: {len(existing_rows)}, removed: {len(removed_rows)}, near: {len(near_rows)})"
    )
    email_body = build_hypo_email_body(
        len(df),
        new_rows,
        existing_rows,
        removed_rows,
        near_rows,
    )
    email_html = build_hypo_email_html(
        len(df),
        new_rows,
        existing_rows,
        removed_rows,
        near_rows,
    )
    email_sent = send_email_report(
        body=email_body,
        subject_prefix=subject_prefix,
        attachment_path=hypo_csv_path,
        html_body=email_html,
    )
    if email_sent:
        logging.info("email sent")
    else:
        logging.info("email not sent (missing SMTP_* or EMAIL_TO env vars)")
