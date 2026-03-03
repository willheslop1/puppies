"""
MSPCA Dog Adoption Scraper (listing pages only)

What this version adds vs. your original:
- Parses the structured card DOM instead of regex over visible <a> text
- Extracts and stores:
  - detail_url (card link)
  - image_url (card image)
  - raw_stats (all petStats key/value pairs found on the card)
- Still captures name, breed, location, gender, age
- More resilient to MSPCA changing card text formatting

Output:
- mspca_dogs_final.csv

Notes:
- This intentionally does NOT visit individual dog detail pages.
"""

import argparse
import difflib
import logging
import os
import re
import smtplib
import time
from email.message import EmailMessage
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.mspca.org/adoption-search/"
DOGS_QUERY = "?type=dog"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; mspca-adoption-scraper/2.0; +https://example.com)"
}

HYPOALLERGENIC_PATTERNS = [
    r"\bhypoallergenic\b",
    r"\b[a-z]*doodle\b",
    r"\b[a-z]+poo\b",
    r"\bpoodle(?:s)?\b",
    r"\bbichon(?: frise)?\b",
    r"\bmaltese\b",
    r"\bschnauzer(?:s)?\b",
    r"\byork(?:shire)? terrier\b",
    r"\byorkie\b",
    r"\bshih ?tzu\b",
    r"\bshi ?htzu\b",
    r"\bshihtzu\b",
    r"\bshitzu\b",
    r"\bhavanese\b",
    r"\bportuguese water dog\b",
    r"\bspanish water dog\b",
    r"\blagotto(?: romagnolo)?\b",
    r"\bsoft coated wheaten\b",
    r"\bwheaten terrier\b",
    r"\bxolo(?:itzcuintli)?\b",
    r"\bchinese crested\b",
    r"\bbedlington terrier\b",
    r"\bcoton de tulear\b",
    r"\bbolognese\b",
    r"\blhasa apso\b",
    r"\btibetan terrier\b",
    r"\bwire fox terrier\b",
    r"\bairedale terrier\b",
    r"\bwelsh terrier\b",
    r"\blakeland terrier\b",
    r"\bnorwich terrier\b",
    r"\bnorfolk terrier\b",
    r"\bsealyham terrier\b",
    r"\bdandie dinmont terrier\b",
    r"\baustralian terrier\b",
    r"\birish water spaniel\b",
    r"\bpuli\b",
    r"\bkomondor\b",
    r"\baffenpinscher\b",
    r"\bbasenji\b",
    r"\bkerry blue terrier\b",
    r"\bscottish terrier\b",
    r"\bwest highland white terrier\b",
    r"\bwestie\b",
    r"\bcairn terrier\b",
    r"\bbarbet\b",
    r"\blowchen\b",
    r"\bperuvian (?:inca )?orchid\b",
    r"\bamerican hairless terrier\b",
    r"\bhairless\b",
]

HYPOALLERGENIC_RE = re.compile("|".join(HYPOALLERGENIC_PATTERNS), re.IGNORECASE)
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


def _is_hypoallergenic_proxy(breed: str) -> int:
    """
    Evidence-informed proxy for low-shedding "hypoallergenic-like" breeds.
    Studies show no breed is reliably hypoallergenic for Can f 1 exposure:
    - 10.2500/ajra.2011.25.3606
    - 10.1016/j.jaci.2012.05.013
    - 10.1111/j.1398-9995.2005.00824.x
    """
    normalized = _normalize_breed_text(breed)
    if not normalized:
        return 0
    if HYPOALLERGENIC_RE.search(normalized):
        return 1

    candidates = [_canonicalize_breed(normalized)] + [
        _canonicalize_breed(p) for p in _breed_segments(normalized)
    ]

    for part in candidates:
        if not part:
            continue
        if part in LOW_SHEDDING_BREED_SET:
            return 1
        if any(f" {breed_name} " in f" {part} " for breed_name in LOW_SHEDDING_BREED_SET):
            return 1
        if difflib.get_close_matches(part, LOW_SHEDDING_FUZZY_TERMS, n=1, cutoff=0.84):
            return 1
    return 0

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


def scrape_all_dogs(
    max_pages: int = 50,
    sleep_s: float = 1.0,
    debug: bool = False,
    max_zero_new_pages: int = 3,
    show_progress: bool = True,
    max_retries: int = 3,
    backoff_s: float = 1.0,
) -> pd.DataFrame:
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
                "page=%s new_urls=%s total_seen=%s", p, len(new_urls), len(seen_urls)
            )
        if show_progress:
            _progress(p, max_pages)

        # stop when pagination runs out / no cards found
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
        df["is_hypoallergenic"] = df["breed"].fillna("").apply(
            _is_hypoallergenic_proxy
        )

    return df


def send_email_with_csv(
    csv_path: str,
    row_count: int,
    hypo_count: int,
    subject_prefix: str = "MSCPA Dogs",
) -> None:
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
    msg.set_content(
        "Attached: "
        f"{row_count} dogs.\n"
        f"Total dogs: {row_count}\n"
        f"Hypoallergenic: {hypo_count}\n"
        f"Generated at {now}."
    )

    with open(csv_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(csv_path),
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
    parser.add_argument("--output-prefix", default="")
    args = parser.parse_args()

    _setup_logging(args.log_path, verbose=args.debug)

    df = scrape_all_dogs(
        max_pages=args.max_pages,
        sleep_s=args.sleep_s,
        debug=args.debug,
        show_progress=args.show_progress,
        max_zero_new_pages=args.max_zero_new_pages,
        max_retries=args.max_retries,
        backoff_s=args.backoff_s,
    )
    logging.info("scraped %s dogs", len(df))
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M")
    prefix = args.output_prefix
    output_path = f"{prefix}{timestamp}_mspca_dogs_final.csv"
    df.to_csv(output_path, index=False)
    logging.info("saved %s dogs to %s", len(df), output_path)
    hypo_count = int(df["is_hypoallergenic"].sum()) if "is_hypoallergenic" in df.columns else 0
    email_sent = send_email_with_csv(output_path, len(df), hypo_count)
    if email_sent:
        logging.info("email sent")
    else:
        logging.info("email not sent (missing SMTP_* or EMAIL_TO env vars)")
