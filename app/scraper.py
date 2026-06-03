"""
OLX scraper — Railway production version.
Scrapes apartment listings from olx.uz and upserts into Supabase.
"""

import asyncio
import math
import random
import re
import os
from datetime import datetime

import pandas as pd
from playwright.async_api import async_playwright
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.olx.uz/nedvizhimost/kvartiry/prodazha/?currency=UZS"
MAX_PAGES  = 25

DB_USER    = os.environ.get("DB_USER")
DB_PASS    = os.environ.get("DB_PASS")
DB_HOST    = os.environ.get("DB_HOST", "aws-0-ap-southeast-1.pooler.supabase.com")
DB_PORT    = os.environ.get("DB_PORT", "5432")
DB_NAME    = os.environ.get("DB_NAME", "postgres")
TABLE_NAME = "olx_listings"

# ─────────────────────────────────────────────────────────────────
# TIMING  — tuned for completeness, not speed
# ─────────────────────────────────────────────────────────────────
AD_WAIT_MS            = (3000, 6000)   # buffer after page load
BETWEEN_ADS           = (3.0, 6.0)
BETWEEN_LIST          = (5.0, 10.0)
LONG_BREAK_EVERY      = 15
LONG_BREAK_SECS       = (15, 25)
SCROLL_PASSES         = (2, 3)
SCROLL_DIST_PX        = (500, 1200)
SCROLL_PAUSE          = (0.4, 0.9)
BROWSER_RESTART_EVERY = 20
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
    # NOTE: --single-process, --no-zygote, and --js-flags=--max-old-space-size
    # were removed — they cap V8 heap at 256 MB which silently breaks React
    # rendering on heavier listing pages, causing null fields.
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
    text = str(text).replace("\u00a0", "").replace(" ", "")
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    """Create the listings table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                listing_id    TEXT PRIMARY KEY,
                title         TEXT,
                price         NUMERIC,
                currency      TEXT,
                area          TEXT,
                num_rooms     INT,
                market_type   TEXT,
                views         INT,
                stair         TEXT,
                posted_date   TEXT,
                scraped_date  TEXT,
                negotiation   BOOLEAN,
                seller        TEXT,
                location      TEXT,
                seller_joined TEXT,
                description   TEXT,
                url           TEXT,
                first_seen    TEXT,
                last_seen     TEXT
            )
        """))
    print(f"  [DB] Table '{TABLE_NAME}' ready.")


def save_batch_to_db(data_list, engine):
    """Upsert a batch — insert new listings, update price/views/last_seen on conflict."""
    if not data_list:
        return

    col_order = [
        "listing_id", "title", "price", "currency", "area", "num_rooms",
        "market_type", "views", "stair", "posted_date", "scraped_date",
        "negotiation", "seller", "location", "seller_joined", "description", "url",
    ]
    df = pd.DataFrame(data_list)
    df = df[[c for c in col_order if c in df.columns]]
    records = df.to_dict(orient="records")
    # pandas converts None back to NaN for numeric columns — fix each value explicitly
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and math.isnan(v):
                rec[k] = None

    saved = 0
    failed = 0
    for rec in records:
        # Skip records with no listing_id or 403 pages
        if not rec.get("listing_id"):
            continue
        if rec.get("title") and "403" in str(rec["title"]):
            continue
        cols    = [str(c) for c in rec.keys()]
        values  = [f":{c}" for c in cols]
        updates = ", ".join(
            f"{c} = EXCLUDED.{c}"
            for c in ["price", "currency", "views", "scraped_date", "negotiation", "location"]
            if c in cols
        )
        sql = text(f"""
            INSERT INTO {TABLE_NAME} ({", ".join(cols)}, first_seen, last_seen)
            VALUES ({", ".join(values)}, :scraped_date, :scraped_date)
            ON CONFLICT (listing_id) DO UPDATE SET
                {updates},
                last_seen = EXCLUDED.scraped_date
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

BLOCKED_RESOURCES = {"image", "media"}

async def make_browser_page(p):
    browser = await p.chromium.launch(headless=True, slow_mo=80, args=CHROMIUM_ARGS)
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

    # Block images, fonts, CSS — not needed for scraping, saves ~40% memory
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
    """Collect listing URLs across all pages. Restarts browser if it crashes."""
    all_links = set()
    browser, page = await make_browser_page(p)

    for pg in range(1, max_pages + 1):
        url = page_url(base_url, pg)
        print(f"── LIST PAGE {pg}/{max_pages}: {url}")

        for attempt in range(3):
            try:
                try:
                    await page.goto(url, wait_until="networkidle", timeout=45000)
                except Exception:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(random.randint(3000, 5000))
                # Wait for ad cards to actually render
                try:
                    await page.wait_for_selector("a[href*='/d/obyavlenie/']", timeout=15000)
                except Exception:
                    pass
                await human_scroll(page)
            except Exception as e:
                print(f"   ✗ Error (attempt {attempt+1}/3): {e}")
                # Browser may have crashed — restart it
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
            before = len(all_links)
            for href in hrefs:
                if href and "/d/obyavlenie/" in href:
                    all_links.add(href.split("?")[0])
            gained = len(all_links) - before
            print(f"   +{gained} new  (total {len(all_links)})")

            # If 0 links found, OLX likely served an empty page — retry
            if gained == 0 and pg > 1:
                wait_secs = (attempt + 1) * 20
                print(f"   ✗ No links found — OLX may have blocked this page, waiting {wait_secs}s before retry")
                await asyncio.sleep(wait_secs)
                continue

            break
        else:
            print(f"   ✗ Skipping page {pg} after 3 failed attempts")

        if pg < max_pages:
            await short_delay(*BETWEEN_LIST)

    try:
        await browser.close()
    except Exception:
        pass

    return list(all_links)


# ─────────────────────────────────────────────────────────────────
# STRUCTURED ATTRIBUTES
# ─────────────────────────────────────────────────────────────────

async def get_attrs(page):
    attrs = {}
    try:
        # Try known container selectors in order
        container = None
        for sel in [
            '[data-nx-name="ListContainer"]',
            '[data-testid="ad-parameters"]',
            '[data-cy="ad-parameters"]',
            'ul[class*="parameter"]',
            'ul[class*="Parameter"]',
        ]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                container = loc.first
                break

        if container is None:
            return attrs

        rows  = container.locator("li, p")
        count = await rows.count()
        for i in range(count):
            row_text = clean(await rows.nth(i).inner_text())
            if not row_text:
                continue
            if ":" in row_text:
                k, _, v = row_text.partition(":")
                attrs[clean(k)] = clean(v)
            else:
                parts = row_text.split(None, 1)
                if len(parts) == 2:
                    attrs[clean(parts[0])] = clean(parts[1])
    except Exception as e:
        print(f"  [attrs] {e}")
    return attrs


# ─────────────────────────────────────────────────────────────────
# SCRAPE ONE AD
# ─────────────────────────────────────────────────────────────────

async def scrape_ad(page, url):
    try:
        views_holder = {"value": None}

        async def handle_response(response):
            try:
                if "statistics" in response.url and response.status == 200:
                    data = await response.json()
                    v = (
                        data.get("data", {})
                            .get("statistics", {})
                            .get("page_views", {})
                            .get("sum")
                    )
                    if v is not None:
                        views_holder["value"] = int(v)
            except Exception:
                pass

        page.on("response", handle_response)
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # Let the network settle so OLX's API calls finish loading listing data
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        # Confirm React has rendered — h1 visible means content is in the DOM
        try:
            await page.wait_for_selector("h1", timeout=15000)
        except Exception:
            pass
        await page.wait_for_timeout(random.randint(*AD_WAIT_MS))
        await human_scroll(page)
        await asyncio.sleep(2.0)
        page.remove_listener("response", handle_response)

        page_title = await page.title()
        body_snippet = (await page.text_content("body") or "")[:500].lower()
        if any(x in page_title.lower() for x in ["403", "access denied", "captcha", "just a moment"]) \
                or "403 error" in body_snippet or "access denied" in body_snippet:
            raise Exception("BLOCKED_403")

        # Listing ID
        listing_id = None
        try:
            label2 = page.locator('[data-nx-name="Label2"]')
            if await label2.count() > 0:
                raw = clean(await label2.first.inner_text()) or ""
                m = re.search(r"(\d{5,})", raw)
                if m:
                    listing_id = m.group(1)
        except Exception:
            pass
        if not listing_id:
            m = re.search(r"-(ID[A-Za-z0-9]+)\.html", url)
            if m:
                listing_id = m.group(1)

        # Title
        title = None
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

        # Price & currency
        price = currency = None
        try:
            price_loc  = page.locator('[data-testid="ad-price-container"]')
            price_text = ""
            if await price_loc.count() > 0:
                price_text = clean(await price_loc.first.inner_text()) or ""
            price = extract_number(price_text)
            low = price_text.lower()
            if "$" in price_text or "у.е" in low or "usd" in low:
                currency = "USD"
            elif "сум" in low or "uzs" in low or "sum" in low:
                currency = "UZS"
        except Exception:
            pass

        # Structured attrs
        attrs = await get_attrs(page)

        def pick(keys):
            for k in keys:
                if attrs.get(k):
                    return attrs[k]
            return None

        area        = clean(pick(["Общая площадь", "Umumiy maydoni", "Умумий майдони"]))
        num_rooms   = extract_number(pick(["Количество комнат", "Xonalar soni", "Xona soni"]))
        market_type = clean(pick(["Тип жилья", "Uy-joy turi"]))
        stair       = clean(pick(["Этаж", "Qavat"]))

        # Body text fallback
        bt = ""
        if not all([area, num_rooms, stair, market_type]):
            try:
                bt = clean(await page.text_content("body")) or ""
                if not area:
                    m = re.search(r"Общая площадь[:\s]*([^\n\r]{1,30}?)(?=\s*(?:Этаж|Количество|Тип|$))", bt, re.I)
                    if m:
                        val = clean(m.group(1))
                        if val and re.search(r"\d", val) and len(val) < 30:
                            area = val
                if not num_rooms:
                    m = re.search(r"Количество комнат[:\s]*(\d+)", bt, re.I)
                    if m:
                        num_rooms = int(m.group(1))
                if not market_type:
                    m = re.search(r"Тип жилья[:\s]*([^\n\r]{1,60}?)(?=\s*(?:Этаж|Количество|Общая|$))", bt, re.I)
                    if m:
                        val = clean(m.group(1))
                        if val and len(val) < 60:
                            market_type = val
                if not stair:
                    m = re.search(r"\bЭтаж[:\s]*([^\n\r]{1,30}?)(?=\s*(?:Количество|Общая|Тип|$))", bt, re.I)
                    if m:
                        val = clean(m.group(1))
                        if val and len(val) < 30:
                            stair = val
            except Exception:
                pass

        views = views_holder["value"]
        # Fallback: try page view counter element
        if views is None:
            for sel in ['[data-testid="page-view-counter"]', '[data-cy="view-count"]', '[class*="view-count"]']:
                try:
                    el = page.locator(sel)
                    if await el.count() > 0:
                        views = extract_number(await el.first.inner_text())
                        if views:
                            break
                except Exception:
                    pass
        # Fallback: regex on body
        if views is None and bt:
            m = re.search(r"(\d+)\s*(?:просмотр|view)", bt, re.I)
            if m:
                views = int(m.group(1))

        # Posted date
        posted_date = None
        try:
            date_loc = page.locator('[data-cy="ad-posted-at"], [data-testid="ad-posted-at"]')
            if await date_loc.count() > 0:
                posted_date = clean(await date_loc.first.inner_text())
            elif bt:
                m = re.search(r"Опубликовано[:\s]*([^\n\r]{3,40})", bt, re.I)
                if m:
                    posted_date = clean(m.group(1))
        except Exception:
            pass

        # Negotiation
        negotiation = False
        try:
            p4 = page.locator('[data-nx-name="P4"]')
            if await p4.count() > 0:
                p4_text = (clean(await p4.first.inner_text()) or "").lower()
                if any(x in p4_text for x in ["договорная", "negotiable", "kelishiladi"]):
                    negotiation = True
            if not negotiation and bt:
                if "договорная" in bt.lower() or "negotiable" in bt.lower():
                    negotiation = True
        except Exception:
            pass

        # Seller
        seller = None
        try:
            sl = page.locator('[data-testid="user-profile-user-name"]')
            if await sl.count() > 0:
                seller = clean(await sl.first.inner_text())
        except Exception:
            pass

        # Location
        location = None
        try:
            texts = await page.locator("p, span").all_inner_texts()
            for t in texts:
                t = clean(t)
                if not t:
                    continue
                if any(x in t.lower() for x in ["район", "ташкент", "toshkent", "область"]):
                    if len(t) < 120:
                        location = t
                        break
        except Exception:
            pass

        # Seller joined
        seller_joined = None
        try:
            ms = page.locator('[data-testid="member-since"]')
            if await ms.count() > 0:
                seller_joined = clean(await ms.first.inner_text())
        except Exception:
            pass

        # Description
        description = None
        try:
            dl = page.locator('[data-testid="ad_description"]')
            if await dl.count() > 0:
                description = clean(await dl.first.inner_text())
        except Exception:
            pass

        # Description fallbacks
        if description:
            desc_low = description.lower()
            if not area:
                m = re.search(r"(\d+[.,]?\d*)\s*м[²2]", description, re.I)
                if m:
                    area = m.group(1) + " м²"
            if not num_rooms:
                m = re.search(r"(\d+)[\s-]*комнат|комнат[:\s]*(\d+)", description, re.I)
                if m:
                    num_rooms = int(m.group(1) or m.group(2))
            if not market_type:
                if any(x in desc_low for x in ["вторичный", "вторичка"]):
                    market_type = "Вторичный рынок"
                elif any(x in desc_low for x in ["новостройка", "первичный"]):
                    market_type = "Новостройка"

        return {
            "listing_id":    listing_id,
            "title":         title,
            "price":         price,
            "currency":      currency,
            "area":          area,
            "num_rooms":     num_rooms,
            "market_type":   market_type,
            "views":         views,
            "stair":         stair,
            "posted_date":   posted_date,
            "scraped_date":  now_str(),
            "negotiation":   negotiation,
            "seller":        seller,
            "location":      location,
            "seller_joined": seller_joined,
            "description":   description,
            "url":           url,
        }

    except Exception as e:
        if "BLOCKED_403" in str(e):
            raise  # let caller handle retry
        print(f"  FAILED: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
# BROWSER WARMUP  — establish cookies/session before hitting ad pages
# ─────────────────────────────────────────────────────────────────

async def warmup_browser(page):
    """Visit the OLX homepage so the browser has a valid session before ad pages."""
    try:
        print("  [browser] warming up session on OLX homepage...")
        await page.goto("https://www.olx.uz/", wait_until="domcontentloaded", timeout=30000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        await page.wait_for_timeout(random.randint(3000, 5000))
        await human_scroll(page)
        print("  [browser] session ready.")
    except Exception as e:
        print(f"  [browser] warmup failed (continuing anyway): {e}")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

async def run_scrape():
    print(f"\n{'='*55}")
    print(f"  SCRAPE STARTED  —  {now_str()}")
    print(f"{'='*55}\n")

    try:
        engine = get_engine()
        ensure_table(engine)
        print("✓ Database connected.")
    except Exception as e:
        print(f"FATAL DB ERROR: {e}")
        return 0

    batch_data = []
    ad_counter = 0

    async with async_playwright() as p:
        # Step 1: collect links (browser managed inside get_all_links)
        print("Collecting listing links...")
        all_links = list(set(await get_all_links(p, BASE_URL, MAX_PAGES)))
        print(f"\nTotal unique links: {len(all_links)}")

        # Step 2: scrape each ad
        browser, page = await make_browser_page(p)
        await warmup_browser(page)

        for idx, link in enumerate(all_links, start=1):
            ad_counter += 1

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
            data = None
            for attempt in range(3):
                try:
                    data = await scrape_ad(page, link)
                    # If page loaded but rendered nothing, treat as a failed render
                    if data and not data.get("title") and not data.get("price"):
                        raise Exception("RENDER_FAILED")
                    break
                except Exception as e:
                    err = str(e)
                    if "BLOCKED_403" in err:
                        wait = (attempt + 1) * 30
                        print(f"  BLOCKED — retry {attempt+1}/3 in {wait}s")
                        await asyncio.sleep(wait)
                    elif "RENDER_FAILED" in err:
                        wait = (attempt + 1) * 10
                        print(f"  Page rendered empty — retry {attempt+1}/3 in {wait}s")
                        await asyncio.sleep(wait)
                        # don't reset data — let next attempt overwrite
                        data = None
                    elif any(x in err for x in [
                        "Target page", "browser has been closed",
                        "page has been closed", "Browser closed",
                        "context or browser", "Target closed",
                    ]):
                        print(f"  Browser crashed — restarting (attempt {attempt+1}/3)")
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

            if data:
                batch_data.append(data)
                print(
                    f"  ✓  rooms={data['num_rooms']}  "
                    f"price={data['price']} {data['currency']}  "
                    f"loc={str(data['location'])[:40]}"
                )
            else:
                print("  ✗ skipped")

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

    print(f"\n{'='*55}")
    print(f"  DONE  —  {ad_counter} ads processed  —  {now_str()}")
    print(f"{'='*55}\n")
    return ad_counter


if __name__ == "__main__":
    asyncio.run(run_scrape())
