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

import re
import time
import os
import smtplib
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
    r"\bdoodle\b",
    r"\b[a-z]+poo\b",
    r"\bpoodle\b",
    r"\bbichon\b",
    r"\bmaltese\b",
    r"\bschnauzer\b",
    r"\byork(?:shire)? terrier\b",
    r"\bshih tzu\b",
    r"\bhavanese\b",
    r"\bportuguese water dog\b",
    r"\bwater dog\b",
    r"\blagotto\b",
    r"\bsoft coated wheaten\b",
    r"\bwheaten terrier\b",
    r"\bxolo(?:itzcuintli)?\b",
    r"\bchinese crested\b",
    r"\bbasenji\b",
    r"\bafghan hound\b",
    r"\bkerry blue terrier\b",
    r"\bscottish terrier\b",
    r"\bwest highland white terrier\b",
    r"\bwestie\b",
    r"\bcairn terrier\b",
    r"\bbarbet\b",
    r"\blowchen\b",
    r"\bspanish water dog\b",
    r"\bperuvian (?:inca )?orchid\b",
    r"\bamerican hairless terrier\b",
    r"\bhairless\b",
]

HYPOALLERGENIC_RE = re.compile("|".join(HYPOALLERGENIC_PATTERNS), re.IGNORECASE)

def _clean(s: str) -> str:
    return " ".join((s or "").split()).strip()


def _progress(page: int, max_pages: int) -> None:
    bar_width = 24
    ratio = min(page / max_pages, 1.0)
    filled = int(bar_width * ratio)
    bar = "#" * filled + "-" * (bar_width - filled)
    print(f"\rProgress: [{bar}] page {page}/{max_pages}", end="", flush=True)


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


def fetch(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


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
) -> pd.DataFrame:
    all_dogs: List[Dog] = []
    seen_urls = set()
    zero_new_pages = 0

    for p in range(1, max_pages + 1):
        url = page_url(p)

        try:
            html = fetch(url)
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
            print(f"debug: page={p} new_urls={len(new_urls)} total_seen={len(seen_urls)}")
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
            lambda b: 1 if HYPOALLERGENIC_RE.search(b) else 0
        )

    return df


def _load_prev_hypo_ids(path: str) -> set:
    if not os.path.exists(path):
        return set()
    try:
        prev = pd.read_csv(path)
    except Exception:
        return set()
    if "detail_url" in prev.columns:
        prev_ids = prev["detail_url"].fillna("").astype(str)
        return set(v for v in prev_ids if v)
    return set()


def send_email_with_csv(
    csv_path: str,
    row_count: int,
    hypo_count: int,
    new_hypo_count: int,
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
        f"New hypoallergenic: {new_hypo_count}\n"
        f"Generated at {now}."
    )

    with open(csv_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(csv_path),
        )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
    return True


if __name__ == "__main__":
    df = scrape_all_dogs(max_pages=30, sleep_s=1.0, debug=False, show_progress=True)
    print(df.head(10))
    output_path = "mspca_dogs_final.csv"
    prev_hypo_ids = _load_prev_hypo_ids(output_path)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} dogs to {output_path}")
    hypo_count = int(df["is_hypoallergenic"].sum()) if "is_hypoallergenic" in df.columns else 0
    new_hypo_count = 0
    if "detail_url" in df.columns and "is_hypoallergenic" in df.columns:
        current_hypo_ids = set(
            df.loc[df["is_hypoallergenic"] == 1, "detail_url"].fillna("").astype(str)
        )
        current_hypo_ids = set(v for v in current_hypo_ids if v)
        new_hypo_count = len(current_hypo_ids - prev_hypo_ids)
    email_sent = send_email_with_csv(output_path, len(df), hypo_count, new_hypo_count)
    if email_sent:
        print("Email sent.")
    else:
        print("Email not sent (missing SMTP_* or EMAIL_TO env vars).")
