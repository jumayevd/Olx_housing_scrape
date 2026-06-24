"""
OLX scraper — Railway production version.
Scrapes apartment listings from olx.uz and upserts into Supabase.

Data sources, in priority order:
  1. <script type="application/ld+json">  — server-rendered, reliable title/price/currency/description.
  2. The on-page parameter list (Общая площадь, Этаж, …) parsed element-by-element
     so values never bleed across parameters.
  3. DOM selectors / description inference as a last resort.

Tuned for completeness, not speed — we have plenty of memory, so we load fully,
wait for render, and retry hard. Redirected/removed listings are skipped, never
saved as empty rows.
"""

import asyncio
import json
import math
import random
import re
import os
import time
from datetime import datetime

import pandas as pd
from playwright.async_api import async_playwright
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
BASE_URL   = os.getenv("OLX_BASE_URL", "https://www.olx.uz/nedvizhimost/kvartiry/prodazha/?currency=UZS")
MAX_PAGES  = int(os.getenv("MAX_PAGES", "25"))

DB_USER    = os.environ.get("DB_USER")
DB_PASS    = os.environ.get("DB_PASS")
DB_HOST    = os.environ.get("DB_HOST", "aws-0-ap-southeast-1.pooler.supabase.com")
DB_PORT    = os.environ.get("DB_PORT", "5432")
DB_NAME    = os.environ.get("DB_NAME", "postgres")
TABLE_NAME = os.getenv("TABLE_NAME", "olx_listings")

# How many of the first ads to dump full diagnostics for (set DIAG_DUMP>0 to enable).
DIAG_DUMP  = int(os.getenv("DIAG_DUMP", "0"))

# Resume: within a run, skip listings that already have TODAY's snapshot row, so a
# restarted/overlapping run finishes today's snapshot instead of starting over.
# Each new calendar day re-scrapes everything (a fresh daily snapshot).
RESUME_SKIP_DONE_TODAY = os.getenv("RESUME_SKIP_DONE_TODAY", "true").lower() == "true"

# Hard wall-clock budget for a single run. We stop cleanly when reached so a slow
# run can never bleed past the next daily cron (which APScheduler would then skip
# with "maximum number of running instances reached"). The next run resumes.
MAX_RUNTIME_HOURS = float(os.getenv("MAX_RUNTIME_HOURS", "22"))

# ─────────────────────────────────────────────────────────────────
# TIMING  — balanced for completeness without tripping OLX blocks.
# All knobs below are deliberately conservative; trim further only if blocks stay
# rare. The big time win is NOT shorter sleeps but skipping the no-price retry
# trap and resuming today's already-scraped listings (see RESUME_SKIP_DONE_TODAY).
# ─────────────────────────────────────────────────────────────────
AD_WAIT_MS            = (3500, 6000)   # buffer after page load
BETWEEN_ADS           = (2.5, 5.0)
BETWEEN_LIST          = (4.5, 9.0)
LONG_BREAK_EVERY      = 22
LONG_BREAK_SECS       = (13, 22)
SCROLL_PASSES         = (2, 3)
SCROLL_DIST_PX        = (500, 1200)
SCROLL_PAUSE          = (0.4, 0.8)
BROWSER_RESTART_EVERY = 40            # plenty of memory — restart rarely
AD_ATTEMPTS           = int(os.getenv("AD_ATTEMPTS", "4"))   # was 5; no-price no longer retries
BATCH_SIZE            = 1             # save every listing immediately

CHROMIUM_ARGS = [
    # Required for headless on Linux/Railway
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    # GPU not needed in headless
    "--disable-gpu",
    "--disable-accelerated-2d-canvas",
    # Misc hardening / noise reduction
    "--disable-web-security",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--hide-scrollbars",
    "--metrics-recording-only",
    "--mute-audio",
    "--no-first-run",
    "--safebrowsing-disable-auto-update",
    # NOTE: heap-capping flags (--single-process, --no-zygote,
    # --js-flags=--max-old-space-size) are intentionally absent — they break
    # React rendering on heavier pages and cause null fields.
]


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def clean(text):
    if not text:
        return None
    return re.sub(r"\s+", " ", str(text)).strip() or None


def extract_number(text):
    if not text:
        return None
    text = str(text).replace(" ", "").replace(" ", "")
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def listing_id_from_url(url):
    """OLX listing id embedded in the URL, e.g. '…-IDabc123.html' → 'IDabc123'."""
    m = re.search(r"-(ID[A-Za-z0-9]+)\.html", url or "")
    return m.group(1) if m else None


def normalize_area(val):
    """Normalize an area value to '<number> м²' (handles '76', '76 м²', '50,43 m²')."""
    if not val:
        return None
    m = re.search(r"(\d+[.,]?\d*)", str(val))
    if not m:
        return clean(val)
    return f"{m.group(1).replace(',', '.')} м²"


def clean_location(raw):
    """Turn the raw map-block text into a clean 'City, District' (drops widget junk).

    Raw looks like: 'Ташкент, Яшнабадский район Ташкентская область Изображений
    нет. Картографические данные Условия Посмотреть расположение на карте'
    → 'Ташкент, Яшнабадский район'
    """
    if not raw:
        return None
    t = clean(raw)
    if not t:
        return None
    # Cut everything from the map-widget boilerplate onward.
    t = re.split(
        r"\s*(?:Изображени|Картограф|Посмотрет|Услови|Show|Map data|Terms|©|http)",
        t, maxsplit=1, flags=re.I,
    )[0]
    # Strip category prefixes that leak in from breadcrumb items.
    t = re.sub(r"\b(Продажа|Аренда|Sotuv|Ijara)\s*[-–]\s*", "", t, flags=re.I)
    t = clean(t)
    if not t:
        return None
    # If a district is present, keep through it and drop the region tail.
    m = re.search(r"^(.*?\bрайон)\b", t, flags=re.I)
    if m:
        return clean(m.group(1))
    # No district: keep city + region but cap length to avoid stray text.
    return clean(t[:60])


def location_from_title(page_title):
    """OLX titles end with '… - Продажа <City> на Olx' — pull the city out.

    Anchored to the trailing 'Продажа … на Olx' suffix (noun form, end of string)
    so a listing title that itself starts with 'Продаётся …' isn't mistaken for it.
    """
    if not page_title:
        return None
    # Greedy prefix locks onto the LAST 'Продажа … на Olx' (the suffix), so a
    # listing title that itself starts with 'Продажа …' isn't captured by mistake.
    m = re.search(r".*Продажа\s+(.+?)\s+на\s+Olx\s*$", page_title, re.I)
    return clean(m.group(1)) if m else None


async def short_delay(a, b):
    await asyncio.sleep(random.uniform(a, b))


async def human_scroll(page, fast=False):
    passes = 1 if fast else random.randint(*SCROLL_PASSES)
    for _ in range(passes):
        dist = random.randint(*SCROLL_DIST_PX)
        await page.mouse.wheel(0, dist)
        await asyncio.sleep(random.uniform(*SCROLL_PAUSE))
        if not fast and random.random() < 0.3:
            await asyncio.sleep(random.uniform(0.5, 1.2))


# ─────────────────────────────────────────────────────────────────
# DATABASE  — upsert on listing_id
# ─────────────────────────────────────────────────────────────────

# The columns we store, in order. first_seen / last_seen intentionally removed.
# snapshot_date is part of the PK: one row per listing per day (daily snapshots).
COLUMNS = [
    "listing_id", "snapshot_date", "olx_id", "title", "price", "currency", "area",
    "num_rooms", "market_type", "views", "stair", "total_floors", "posted_date",
    "scraped_date", "negotiation", "seller", "location", "seller_joined",
    "description", "url",
]

# Columns refreshed on a same-day re-scrape (everything except the composite key).
UPDATE_COLUMNS = [c for c in COLUMNS if c not in ("listing_id", "snapshot_date")]


def get_engine():
    if not DB_USER or not DB_PASS:
        raise RuntimeError("DB_USER and DB_PASS environment variables are required.")
    from sqlalchemy.engine import URL
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_table(engine):
    """Create the listings table if needed and migrate the schema in place.

    Primary key is (listing_id, snapshot_date) so each daily run APPENDS a fresh
    snapshot per listing — one row per listing per day — rather than overwriting.
    A same-day re-scrape updates that day's row (idempotent resume).
    """
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                listing_id    TEXT NOT NULL,
                snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
                olx_id        BIGINT,
                title         TEXT,
                price         NUMERIC,
                currency      TEXT,
                area          TEXT,
                num_rooms     INT,
                market_type   TEXT,
                views         INT,
                stair         TEXT,
                total_floors  TEXT,
                posted_date   TEXT,
                scraped_date  TEXT,
                negotiation   BOOLEAN,
                seller        TEXT,
                location      TEXT,
                seller_joined TEXT,
                description   TEXT,
                url           TEXT,
                PRIMARY KEY (listing_id, snapshot_date)
            )
        """))
        # Migrate existing tables: add olx_id/total_floors, drop first_seen/last_seen.
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS olx_id BIGINT"))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS total_floors TEXT"))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} DROP COLUMN IF EXISTS first_seen"))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} DROP COLUMN IF EXISTS last_seen"))
        # Migrate a legacy single-column PK (listing_id) → (listing_id, snapshot_date).
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS snapshot_date DATE"))
        conn.execute(text(
            f"UPDATE {TABLE_NAME} SET snapshot_date = "
            f"COALESCE(NULLIF(left(scraped_date, 10), '')::date, CURRENT_DATE) "
            f"WHERE snapshot_date IS NULL"
        ))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN snapshot_date SET NOT NULL"))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN snapshot_date SET DEFAULT CURRENT_DATE"))
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_attribute a
                      ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                    WHERE c.conrelid = '{TABLE_NAME}'::regclass
                      AND c.contype = 'p' AND a.attname = 'snapshot_date'
                ) THEN
                    ALTER TABLE {TABLE_NAME} DROP CONSTRAINT IF EXISTS {TABLE_NAME}_pkey;
                    ALTER TABLE {TABLE_NAME} ADD CONSTRAINT {TABLE_NAME}_pkey
                        PRIMARY KEY (listing_id, snapshot_date);
                END IF;
            END $$;
        """))
    print(f"  [DB] Table '{TABLE_NAME}' ready (daily-snapshot mode: PK = listing_id + snapshot_date).")


def load_done_today(engine):
    """listing_ids that already have a snapshot row for today — skipped on resume."""
    if not RESUME_SKIP_DONE_TODAY:
        return set()
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(f"SELECT listing_id FROM {TABLE_NAME} WHERE snapshot_date = CURRENT_DATE")
            ).fetchall()
    except Exception as e:
        print(f"  [resume] could not load today's snapshot ids (continuing full scrape): {e}")
        return set()
    return {r[0] for r in rows if r[0]}


def tidy_existing_location(loc):
    """Re-clean a stored location value (handles old map-junk and 3-part forms)."""
    c = clean_location(loc)
    if not c:
        return None
    parts = [p.strip() for p in c.split(",")]
    # Drop a leading 'X область' when a city + district follow it.
    if len(parts) >= 3 and "область" in parts[0].lower():
        c = ", ".join(parts[1:])
    return c


def backfill_locations(engine):
    """One-time pass to re-clean existing location values. Gated by env var."""
    if os.getenv("BACKFILL_LOCATIONS", "false").lower() != "true":
        return
    print("  [backfill] cleaning existing location values...")
    with engine.begin() as conn:
        rows = conn.execute(
            text(f"SELECT listing_id, location FROM {TABLE_NAME} WHERE location IS NOT NULL")
        ).fetchall()
    fixed = 0
    for lid, loc in rows:
        new = tidy_existing_location(loc)
        if new and new != loc:
            with engine.begin() as conn:
                conn.execute(
                    text(f"UPDATE {TABLE_NAME} SET location = :l WHERE listing_id = :id"),
                    {"l": new, "id": lid},
                )
            fixed += 1
    print(f"  [backfill] updated {fixed}/{len(rows)} location values.")


def save_batch_to_db(data_list, engine):
    """Upsert a batch — insert new listings, refresh ALL fields on conflict."""
    if not data_list:
        return

    df = pd.DataFrame(data_list)
    df = df[[c for c in COLUMNS if c in df.columns]]
    records = df.to_dict(orient="records")
    # pandas turns None into NaN for numeric columns — restore None explicitly
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and math.isnan(v):
                rec[k] = None

    saved = 0
    failed = 0
    for rec in records:
        if not rec.get("listing_id"):
            continue
        cols    = [c for c in rec.keys()]
        values  = [f":{c}" for c in cols]
        updates = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS if c in cols
        )
        sql = text(f"""
            INSERT INTO {TABLE_NAME} ({", ".join(cols)})
            VALUES ({", ".join(values)})
            ON CONFLICT (listing_id, snapshot_date) DO UPDATE SET
                {updates}
        """)
        try:
            with engine.begin() as conn:
                conn.execute(sql, rec)
            saved += 1
        except Exception as e:
            failed += 1
            print(f"  [DB] ✗ Failed to save listing {rec.get('listing_id')}: {e}")

    print(f"  [DB] ✓ {saved} saved, {failed} failed.")


# ─────────────────────────────────────────────────────────────────
# BROWSER FACTORY
# ─────────────────────────────────────────────────────────────────

# Block only images/media — they hold no listing data. CSS/JS load normally so
# React renders fully (where the parameters live).
BLOCKED_RESOURCES = {"image", "media"}

async def make_browser_page(p):
    browser = await p.chromium.launch(headless=True, slow_mo=60, args=CHROMIUM_ARGS)
    context = await browser.new_context(
        viewport={"width": 1400, "height": 900},
        locale="ru-RU",
        timezone_id="Asia/Tashkent",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    async def block_resources(route):
        if route.request.resource_type in BLOCKED_RESOURCES:
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", block_resources)

    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins',   { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU','ru','en-US'] });
    """)
    return browser, page


# ─────────────────────────────────────────────────────────────────
# LINK COLLECTION
# ─────────────────────────────────────────────────────────────────

def page_url(base, page_num):
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}page={page_num}" if page_num > 1 else base


async def get_all_links(p, base_url, max_pages):
    """Collect listing URLs across ALL pages until the results run out.

    max_pages is just a safety ceiling; we stop early when pagination ends —
    detected as consecutive pages that are empty (after retries) or add no new
    listings. This way we capture every page, however many OLX has.
    """
    all_links = set()
    browser, page = await make_browser_page(p)

    empty_streak = 0   # consecutive pages with NO listing links at all
    nogain_streak = 0  # consecutive pages adding 0 new (duplicates/end)

    for pg in range(1, max_pages + 1):
        url = page_url(base_url, pg)
        print(f"── LIST PAGE {pg}/{max_pages}: {url}")

        page_raw = 0      # listing links seen on this page (after retries)
        page_gained = 0   # NEW links this page added

        for attempt in range(3):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(random.randint(3000, 5000))
                try:
                    await page.wait_for_selector("a[href*='/d/obyavlenie/']", timeout=15000)
                except Exception:
                    pass
                await human_scroll(page)
            except Exception as e:
                print(f"   ✗ Error (attempt {attempt+1}/3): {e}")
                try:
                    await browser.close()
                except Exception:
                    pass
                await asyncio.sleep(10)
                browser, page = await make_browser_page(p)
                continue

            page_title = (await page.title()).lower()
            body_text  = (await page.text_content("body") or "").lower()

            if "403" in page_title or "access denied" in body_text or "captcha" in body_text:
                wait_secs = (attempt + 1) * 15
                print(f"   ✗ 403 — waiting {wait_secs}s (attempt {attempt+1}/3)")
                await asyncio.sleep(wait_secs)
                continue

            hrefs = await page.locator("a").evaluate_all("els => els.map(e => e.href)")
            raw_links = [h.split("?")[0] for h in hrefs if h and "/d/obyavlenie/" in h]
            before = len(all_links)
            all_links.update(raw_links)
            gained = len(all_links) - before
            page_raw = len(raw_links)
            page_gained = gained
            print(f"   +{gained} new  (total {len(all_links)})")

            if gained == 0 and pg > 1:
                if raw_links:
                    # Page rendered fine — every ad was already collected. The feed
                    # is sorted newest-first and shifts as new ads post, so later
                    # pages overlap earlier ones. Not a block; just move on.
                    print(f"   ~ all {len(raw_links)} links already seen (feed shifted) — moving on")
                    break
                # Truly empty page → likely a block/empty render → retry.
                wait_secs = (attempt + 1) * 20
                print(f"   ✗ Empty page — possible block, waiting {wait_secs}s before retry")
                await asyncio.sleep(wait_secs)
                continue

            break
        else:
            print(f"   ✗ Skipping page {pg} after 3 failed attempts")

        # ── End-of-pagination detection ───────────────────────────
        empty_streak  = empty_streak + 1  if page_raw == 0    else 0
        nogain_streak = nogain_streak + 1 if page_gained == 0 else 0

        if pg > 1 and empty_streak >= 2:
            print(f"\n  ✓ Reached end of results — {empty_streak} empty pages. Stopping at page {pg}.")
            break
        if pg > 1 and nogain_streak >= 4:
            print(f"\n  ✓ No new listings for {nogain_streak} pages — assuming end. Stopping at page {pg}.")
            break

        if pg < max_pages:
            await short_delay(*BETWEEN_LIST)

    try:
        await browser.close()
    except Exception:
        pass

    print(f"  Link collection finished: {len(all_links)} unique listings.")
    return list(all_links)


# ─────────────────────────────────────────────────────────────────
# JSON-LD EXTRACTOR  — primary source (server-rendered schema.org data)
# ─────────────────────────────────────────────────────────────────

def _iter_jsonld_objects(parsed):
    """Yield every dict inside a parsed JSON-LD blob (handles @graph and lists)."""
    stack = [parsed]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            yield node
            if "@graph" in node and isinstance(node["@graph"], list):
                stack.extend(node["@graph"])
        elif isinstance(node, list):
            stack.extend(node)


async def extract_from_jsonld(page):
    """Parse all <script type="application/ld+json"> blocks into our fields."""
    result = {}
    try:
        blobs = await page.evaluate(
            """() => Array.from(
                document.querySelectorAll('script[type="application/ld+json"]')
            ).map(s => s.textContent || '')"""
        )
    except Exception as e:
        print(f"  [json-ld] eval error: {e}")
        return result

    for raw in blobs:
        raw = (raw or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        for obj in _iter_jsonld_objects(parsed):
            if not result.get("title") and obj.get("name"):
                result["title"] = clean(obj.get("name"))
            if not result.get("description") and obj.get("description"):
                result["description"] = clean(obj.get("description"))

            # Offers → price / currency
            offers = obj.get("offers")
            if isinstance(offers, list):
                offers = offers[0] if offers else None
            if isinstance(offers, dict):
                if not result.get("price"):
                    p = offers.get("price") or offers.get("lowPrice")
                    if p not in (None, "", 0, "0"):
                        result["price"] = extract_number(p)
                if not result.get("currency"):
                    cur = (offers.get("priceCurrency") or "").upper()
                    if cur:
                        result["currency"] = (
                            "USD" if cur in ("USD", "U.E.", "UE") else
                            "UZS" if cur in ("UZS", "SUM", "СУМ") else cur
                        )
                avail = str(offers.get("availability") or "").lower()
                if "soldout" in avail or "discontinued" in avail:
                    result["_sold"] = True

            # Address / location
            addr = obj.get("address")
            if not result.get("location") and addr:
                if isinstance(addr, dict):
                    parts = [clean(addr.get(k)) for k in
                             ("streetAddress", "addressLocality", "addressRegion")]
                    parts = [x for x in parts if x]
                    if parts:
                        result["location"] = ", ".join(dict.fromkeys(parts))
                elif isinstance(addr, str):
                    result["location"] = clean(addr)

            # Posted date
            for dk in ("datePosted", "datePublished", "validFrom", "uploadDate"):
                if not result.get("posted_date") and obj.get(dk):
                    result["posted_date"] = clean(str(obj.get(dk)))

            # Area sometimes appears as floorSize on RealEstate types
            if not result.get("area"):
                fs = obj.get("floorSize")
                if isinstance(fs, dict) and fs.get("value"):
                    unit = fs.get("unitText") or "м²"
                    result["area"] = f"{clean(fs.get('value'))} {unit}".strip()

    return {k: v for k, v in result.items() if v is not None}


# ─────────────────────────────────────────────────────────────────
# PARAMETER LIST EXTRACTOR  — clean, element-wise (no value bleed)
# ─────────────────────────────────────────────────────────────────

# Russian labels we care about. Order matters only for diagnostics; matching is
# exact-prefix with a non-letter boundary so "Этаж" never matches "Этажность дома".
PARAM_LABELS = [
    "Общая площадь",
    "Жилая площадь",
    "Площадь кухни",
    "Количество комнат",
    "Этажность дома",
    "Этажей в доме",
    "Этаж",
    "Тип жилья",
    "Тип дома",
    "Ремонт",
    "Меблирована",
    "Комиссия",
    "Новостройка",
]

_PARAM_JS = """
(labels) => {
    const out = {};
    const els = Array.from(document.querySelectorAll('p, li, span, div, dd'));
    for (const lbl of labels) {
        let best = null;
        for (const el of els) {
            if (el.children.length > 3) continue;          // prefer leaf-ish nodes
            const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
            if (!t.startsWith(lbl)) continue;
            const rest = t.slice(lbl.length);
            // reject if the next char is a Cyrillic letter — means a longer label
            if (/^[А-Яа-яЁё]/.test(rest)) continue;
            if (best === null || t.length < best.length) best = t;
        }
        if (best !== null) out[lbl] = best;
    }
    return out;
}
"""


async def get_params(page):
    """Return {label: 'value'} for each known parameter, parsed per-element."""
    try:
        raw = await page.evaluate(_PARAM_JS, PARAM_LABELS)
    except Exception as e:
        print(f"  [params] eval error: {e}")
        return {}
    params = {}
    for lbl, full in (raw or {}).items():
        # strip the label prefix and any leading ":" / whitespace
        val = clean(str(full)[len(lbl):].lstrip(" : \t"))
        if val:
            params[lbl] = val
    return params


# ─────────────────────────────────────────────────────────────────
# LOCATION EXTRACTOR  — full "City, District/region" from the map block
# ─────────────────────────────────────────────────────────────────

_LOCATION_JS = r"""
() => {
    const norm = s => (s || '').replace(/\s+/g, ' ').trim();

    // 1) Known map/location testids — read the address text directly.
    const sels = [
        '[data-testid="map-aside-section"]',
        '[data-testid="qa-static-ad-map"]',
        '[data-testid="ad-location-link"]',
        '[data-cy="ad-location"]',
        '[data-testid="location-link"]',
    ];
    for (const s of sels) {
        const el = document.querySelector(s);
        if (el) {
            const t = norm(el.innerText);
            if (t && t.length < 160) return {src: s, text: t};
        }
    }

    // 2) Find the LOCATION/Карта/Манзил heading, take the text right after it.
    const heads = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,p,span,div'));
    for (const h of heads) {
        const ht = norm(h.textContent);
        if (/^(Карта|Местоположение|Манзил|Joylashuv|Location)$/i.test(ht)) {
            let n = h.nextElementSibling, depth = 0;
            while (n && depth < 5) {
                const t = norm(n.innerText);
                if (t && t.length < 160) return {src: 'heading:' + ht, text: t};
                n = n.nextElementSibling; depth++;
            }
        }
    }
    return null;
}
"""


_BREADCRUMB_JS = r"""
() => {
    const norm = s => (s || '').replace(/\s+/g, ' ').trim();
    // Breadcrumb items — the geographic tail is what we want.
    let items = [];
    const bc = document.querySelector('[data-testid="breadcrumbs"], nav[aria-label*="readcrumb" i], ol');
    if (bc) items = Array.from(bc.querySelectorAll('li, a')).map(e => norm(e.textContent)).filter(Boolean);
    return items;
}
"""


async def get_location(page):
    """Return (raw_text, src): full location text from the ad's map block.

    Scrolls the map block into view first so React renders it (otherwise some
    ads fall through to the title's city only).
    """
    # Scroll the whole page so the lazily-rendered map/location section mounts.
    # (olx.uz exposes no map testid, so we can't target it directly — a full
    # pass guarantees the "Местоположение" block renders for every ad.)
    try:
        await page.evaluate("""async () => {
            const step = 700;
            for (let y = 0; y <= document.body.scrollHeight; y += step) {
                window.scrollTo(0, y);
                await new Promise(r => setTimeout(r, 90));
            }
            window.scrollTo(0, document.body.scrollHeight);
        }""")
        await page.wait_for_timeout(900)
    except Exception:
        pass

    # 1) PRIMARY: breadcrumb — reliable, server-rendered 'City, District'.
    try:
        crumbs = await page.evaluate(_BREADCRUMB_JS)
    except Exception:
        crumbs = None
    bc = _parse_breadcrumb_geo(crumbs)
    if bc:
        return bc, "breadcrumb"

    # 2) FALLBACK: the map block ('Местоположение' section).
    try:
        res = await page.evaluate(_LOCATION_JS)
    except Exception as e:
        print(f"  [location] eval error: {e}")
        res = None
    if res and res.get("text"):
        return clean(res.get("text")), res.get("src")

    return None, None


_BC_CATEGORY = {"главная", "недвижимость", "квартиры", "дома", "коммерческая",
                "продажа", "аренда", "olx", "olx.uz", "uy-joy", "kvartiralar"}


def _parse_breadcrumb_geo(crumbs):
    """From breadcrumb items, build 'City, District' (requires a район)."""
    if not crumbs:
        return None
    items = [re.sub(r"^(Продажа|Аренда|Sotuv|Ijara)\s*[-–]\s*", "", c, flags=re.I).strip()
             for c in crumbs]
    # Collapse consecutive duplicates (breadcrumb yields both <li> and <a>).
    deduped = []
    for c in items:
        if c and (not deduped or deduped[-1] != c):
            deduped.append(c)
    items = deduped
    district = next((c for c in items if "район" in c.lower()), None)
    if not district:
        return None  # no district → let the map fallback try
    region = next((c for c in items if "область" in c.lower()), None)
    # City = the item immediately before the district, if it's a real place name.
    city = None
    di = items.index(district)
    if di > 0:
        prev = items[di - 1]
        if "область" not in prev.lower() and prev.lower() not in _BC_CATEGORY:
            city = prev
    head = city or region
    return ", ".join(p for p in (head, district) if p)


async def get_olx_id(page):
    """OLX's pure-numeric ad id, shown on the page as 'ID: 779712345' / '№ …'."""
    try:
        raw = await page.evaluate(r"""() => {
            // Prefer an explicit footer element if present.
            for (const el of document.querySelectorAll('[data-cy="ad-footer-bar-section"], [data-testid="ad-footer-bar-section"]')) {
                const m = (el.innerText || '').match(/(\d{6,12})/);
                if (m) return m[1];
            }
            // Otherwise scan body text for an 'ID:'/'№' label followed by digits.
            const bt = document.body ? document.body.innerText : '';
            const m = bt.match(/(?:ID|№)\s*[:№.]?\s*(\d{6,12})/);
            return m ? m[1] : null;
        }""")
        return int(raw) if raw and str(raw).isdigit() else None
    except Exception as e:
        print(f"  [olx_id] eval error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
# SCRAPE ONE AD
# ─────────────────────────────────────────────────────────────────

class NotAListing(Exception):
    """Raised when a URL redirects away from an ad (removed/expired) — skip it."""


async def scrape_ad(page, url, diag=False):
    views_holder = {"value": None}

    async def handle_response(response):
        try:
            if "statistics" in response.url and response.status == 200:
                data = await response.json()
                v = (data.get("data", {}).get("statistics", {})
                         .get("page_views", {}).get("sum"))
                if v is not None:
                    views_holder["value"] = int(v)
        except Exception:
            pass

    page.on("response", handle_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # Wait for h1 — signals React rendered. networkidle never settles on OLX.
        try:
            await page.wait_for_selector("h1", timeout=18000)
        except Exception:
            pass
        await page.wait_for_timeout(random.randint(*AD_WAIT_MS))
        await human_scroll(page)
        await asyncio.sleep(1.3)
    finally:
        page.remove_listener("response", handle_response)

    page_title = await page.title()
    body_snippet = (await page.text_content("body") or "")[:600].lower()
    if any(x in page_title.lower() for x in ["403", "access denied", "captcha", "just a moment"]) \
            or "403 error" in body_snippet or "access denied" in body_snippet:
        raise Exception("BLOCKED_403")

    # ── Listing ID (always from the URL) ──────────────────────────
    listing_id = listing_id_from_url(url)

    # ── Detect redirect to a non-listing page (removed/expired ad) ─
    final_url = page.url
    if "/d/obyavlenie/" not in final_url:
        raise NotAListing(f"redirected to {final_url}")

    # ── PRIMARY: JSON-LD ──────────────────────────────────────────
    jd = await extract_from_jsonld(page)
    if jd.get("_sold"):
        raise NotAListing("offer marked sold/discontinued")

    # ── PRIMARY: parameter list (clean, element-wise) ─────────────
    params = await get_params(page)

    # ── OLX's pure-numeric ad id (shown as 'ID: …' on the page) ────
    olx_id = await get_olx_id(page)

    if diag:
        print(f"  [DIAG] final_url   : {final_url}")
        print(f"  [DIAG] page_title  : {page_title[:120]}")
        print(f"  [DIAG] listing_id  : {listing_id}   olx_id: {olx_id}")
        print(f"  [DIAG] json-ld keys: {sorted(jd.keys())}")
        print(f"  [DIAG] json-ld     : "
              f"title={str(jd.get('title'))[:50]!r} price={jd.get('price')} "
              f"cur={jd.get('currency')} loc={str(jd.get('location'))[:40]!r}")
        print(f"  [DIAG] params      : {params}")

    title       = jd.get("title")
    price       = jd.get("price")
    currency    = jd.get("currency")
    description = jd.get("description")
    location    = jd.get("location")
    posted_date = jd.get("posted_date")

    area         = params.get("Общая площадь") or jd.get("area")
    num_rooms    = extract_number(params.get("Количество комнат"))
    stair        = params.get("Этаж")
    total_floors = params.get("Этажность дома") or params.get("Этажей в доме")
    market_type  = params.get("Тип жилья") or params.get("Тип дома")
    negotiation  = False
    seller       = None
    seller_joined = None
    views        = jd.get("views") or views_holder["value"]

    # ── FALLBACK: DOM selectors for anything still missing ────────
    if not title:
        for sel in ["h1", '[data-cy="ad_title"]', '[data-testid="ad-title"]']:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    title = clean(await el.first.inner_text())
                    if title:
                        break
            except Exception:
                pass
        if not title:
            title = clean(page_title.split(" - ")[0].split(" | ")[0])

    if not price or not currency:
        try:
            price_loc = page.locator('[data-testid="ad-price-container"]')
            if await price_loc.count() > 0:
                price_text = clean(await price_loc.first.inner_text()) or ""
                if not price:
                    price = extract_number(price_text)
                low = price_text.lower()
                if not currency:
                    if "$" in price_text or "у.е" in low or "usd" in low:
                        currency = "USD"
                    elif "сум" in low or "uzs" in low or "sum" in low:
                        currency = "UZS"
                if "договорная" in low:
                    negotiation = True
        except Exception:
            pass

    if not seller:
        try:
            sl = page.locator('[data-testid="user-profile-user-name"]')
            if await sl.count() > 0:
                seller = clean(await sl.first.inner_text())
        except Exception:
            pass

    if not seller_joined:
        try:
            ms = page.locator('[data-testid="member-since"]')
            if await ms.count() > 0:
                seller_joined = clean(await ms.first.inner_text())
        except Exception:
            pass

    # PRIMARY for location: the map block, which carries city + district/region.
    if not location:
        loc_raw, loc_src = await get_location(page)
        loc_clean = clean_location(loc_raw)
        if diag:
            print(f"  [DIAG] location    : src={loc_src}")
            print(f"  [DIAG]   raw       : {loc_raw!r}")
            print(f"  [DIAG]   cleaned   : {loc_clean!r}")
        if loc_clean:
            location = loc_clean
    # Fallback: the city embedded in the page title (city only, no district).
    if not location:
        location = location_from_title(page_title)

    if not description:
        try:
            dl = page.locator('[data-testid="ad_description"], [data-cy="ad_description"]')
            if await dl.count() > 0:
                description = clean(await dl.first.inner_text())
        except Exception:
            pass

    if not posted_date:
        try:
            date_loc = page.locator('[data-cy="ad-posted-at"], [data-testid="ad-posted-at"]')
            if await date_loc.count() > 0:
                posted_date = clean(await date_loc.first.inner_text())
        except Exception:
            pass

    if not views:
        for sel in ['[data-testid="page-view-counter"]', '[data-cy="view-count"]']:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    views = extract_number(await el.first.inner_text())
                    if views:
                        break
            except Exception:
                pass

    # ── LAST RESORT: infer from description (clean, scoped regex) ──
    if description:
        desc_low = description.lower()
        if not area:
            mm = re.search(r"(\d+[.,]?\d*)\s*м[²2]", description, re.I)
            if mm:
                area = f"{mm.group(1)} м²"
        if not num_rooms:
            mm = re.search(r"(\d+)\s*[\s-]*комнат", description, re.I)
            if mm:
                num_rooms = int(mm.group(1))
        if not market_type:
            if any(x in desc_low for x in ["вторичн"]):
                market_type = "Вторичный рынок"
            elif any(x in desc_low for x in ["новостройка", "первичн"]):
                market_type = "Новостройка"
        if not negotiation and ("договорная" in desc_low or "negotiable" in desc_low):
            negotiation = True

    area = normalize_area(area)

    return {
        "listing_id":    listing_id,
        "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
        "olx_id":        olx_id,
        "title":         title,
        "price":         price,
        "currency":      currency,
        "area":          area,
        "num_rooms":     num_rooms,
        "market_type":   market_type,
        "views":         views,
        "stair":         stair,
        "total_floors":  total_floors,
        "posted_date":   posted_date,
        "scraped_date":  now_str(),
        "negotiation":   negotiation,
        "seller":        seller,
        "location":      location,
        "seller_joined": seller_joined,
        "description":   description,
        "url":           url,
    }


# ─────────────────────────────────────────────────────────────────
# BROWSER WARMUP  — establish cookies/session before hitting ad pages
# ─────────────────────────────────────────────────────────────────

async def warmup_browser(page):
    try:
        print("  [browser] warming up session on OLX homepage...")
        await page.goto("https://www.olx.uz/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(random.randint(3000, 5000))
        await human_scroll(page)
        print("  [browser] session ready.")
    except Exception as e:
        print(f"  [browser] warmup failed (continuing anyway): {e}")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

_scrape_lock = asyncio.Lock()


async def run_scrape():
    # Guard against overlap: RUN_ON_START calls this directly while the daily
    # cron may also fire it. Without this, two scrapes run concurrently in the
    # same process — doubling OLX load, worsening blocks, and losing data.
    if _scrape_lock.locked():
        print(f"⏭  scrape already in progress — skipping this trigger ({now_str()})")
        return 0

    async with _scrape_lock:
        return await _run_scrape_inner()


async def _run_scrape_inner():
    print(f"\n{'='*55}")
    print(f"  SCRAPE STARTED  —  {now_str()}")
    print(f"{'='*55}\n")

    try:
        engine = get_engine()
        ensure_table(engine)
        backfill_locations(engine)
        print("✓ Database connected.")
    except Exception as e:
        print(f"FATAL DB ERROR: {e}")
        return 0

    batch_data = []
    ad_counter = 0
    skipped_removed = 0
    skipped_recent = 0
    budget_hit = False
    start_ts = time.monotonic()
    budget_secs = MAX_RUNTIME_HOURS * 3600 if MAX_RUNTIME_HOURS > 0 else None

    async with async_playwright() as p:
        print("Collecting listing links...")
        all_links = list(set(await get_all_links(p, BASE_URL, MAX_PAGES)))
        print(f"\nTotal unique links: {len(all_links)}")

        # ── Resume: drop listings already snapshotted today so we finish today's
        #    snapshot rather than start over (after a restart, OOM, or budget cut).
        done_today = load_done_today(engine)
        if done_today:
            before = len(all_links)
            all_links = [l for l in all_links
                         if listing_id_from_url(l) not in done_today]
            skipped_recent = before - len(all_links)
            print(f"  Resume: {skipped_recent} listings already snapshotted today "
                  f"skipped — {len(all_links)} to scrape this run.")

        browser, page = await make_browser_page(p)
        await warmup_browser(page)

        for idx, link in enumerate(all_links, start=1):
            ad_counter += 1

            # ── Wall-clock budget: stop cleanly before bleeding into the next
            #    daily cron. The next run resumes via load_done_today().
            if budget_secs and (time.monotonic() - start_ts) > budget_secs:
                budget_hit = True
                print(f"\n  ⏲  Reached {MAX_RUNTIME_HOURS:g}h runtime budget at "
                      f"listing {idx}/{len(all_links)} — stopping cleanly; "
                      f"next run resumes the rest.\n")
                break

            if idx > 1 and (idx - 1) % BROWSER_RESTART_EVERY == 0:
                print(f"\n  ── restarting browser at listing {idx} ──\n")
                try:
                    await browser.close()
                except Exception:
                    pass
                await asyncio.sleep(5)
                browser, page = await make_browser_page(p)
                await warmup_browser(page)

            print(f"[{idx}/{len(all_links)}] {link.split('/')[-1][:60]}")
            diag = ad_counter <= DIAG_DUMP
            data = None
            skip_permanently = False

            for attempt in range(AD_ATTEMPTS):
                try:
                    data = await scrape_ad(page, link, diag=diag)
                    # A real render yields a title AND at least one structured
                    # detail (price / area / rooms). Price alone is NOT required:
                    # many listings are price-on-request / договорная, and gating
                    # on price made those retry 5× and get discarded every run —
                    # the main reason a run never finished. An empty React shell
                    # (title from <title> but no params) still retries.
                    has_detail = data and (
                        data.get("price") or data.get("area") or data.get("num_rooms")
                    )
                    if data and not (data.get("title") and has_detail):
                        raise Exception("RENDER_FAILED")
                    break
                except NotAListing as e:
                    print(f"  ⏭  skipped (not a listing): {e}")
                    skip_permanently = True
                    data = None
                    break
                except Exception as e:
                    err = str(e)
                    if "BLOCKED_403" in err:
                        wait = (attempt + 1) * 30
                        print(f"  BLOCKED — retry {attempt+1}/{AD_ATTEMPTS} in {wait}s")
                        await asyncio.sleep(wait)
                    elif "RENDER_FAILED" in err:
                        wait = (attempt + 1) * 10
                        print(f"  Page rendered incomplete — retry {attempt+1}/{AD_ATTEMPTS} in {wait}s")
                        await asyncio.sleep(wait)
                        data = None
                    elif any(x in err for x in [
                        "Target page", "browser has been closed",
                        "page has been closed", "Browser closed",
                        "context or browser", "Target closed",
                        "Timeout", "timeout", "Page crashed", "crashed",
                    ]):
                        print(f"  Browser crashed/timeout — restarting (attempt {attempt+1}/{AD_ATTEMPTS})")
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        await asyncio.sleep(5)
                        browser, page = await make_browser_page(p)
                        await warmup_browser(page)
                    else:
                        print(f"  FAILED: {e}")
                        break

            if skip_permanently:
                skipped_removed += 1
            elif data:
                batch_data.append(data)
                print(
                    f"  ✓  rooms={data['num_rooms']}  area={data['area']}  "
                    f"floor={data['stair']}/{data['total_floors']}  "
                    f"price={data['price']} {data['currency']}  "
                    f"type={str(data['market_type'])[:18]}  loc={str(data['location'])[:30]}"
                )
            else:
                print("  ✗ skipped (incomplete after retries)")

            if len(batch_data) >= BATCH_SIZE:
                save_batch_to_db(batch_data, engine)
                batch_data.clear()

            await short_delay(*BETWEEN_ADS)

            if ad_counter % LONG_BREAK_EVERY == 0:
                secs = random.uniform(*LONG_BREAK_SECS)
                print(f"\n  ── pause {secs:.0f}s ──\n")
                await asyncio.sleep(secs)

        try:
            await browser.close()
        except Exception:
            pass

    if batch_data:
        save_batch_to_db(batch_data, engine)

    status = "STOPPED (budget)" if budget_hit else "DONE"
    elapsed_h = (time.monotonic() - start_ts) / 3600
    print(f"\n{'='*55}")
    print(f"  {status}  —  {ad_counter} ads processed, {skipped_removed} removed/skipped, "
          f"{skipped_recent} resumed-skip  —  {elapsed_h:.1f}h  —  {now_str()}")
    print(f"{'='*55}\n")
    return ad_counter


if __name__ == "__main__":
    asyncio.run(run_scrape())
