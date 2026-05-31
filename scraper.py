#!/usr/bin/env python3
"""
OLX.uz Housing Scraper — Railway-ready
olx.uz/nedvizhimost/kvartiry/prodazha/

Features:
  • Checkpoint / resume  — survives crashes and restarts
  • Auto-pagination      — detects last page, stops when no new links
  • All 25 columns       — living area, kitchen area, floor+total, wall material,
                           condition, transaction type, seller type, phone, etc.
  • Anti-bot             — stealth patches, UA rotation, human-like timing
  • Incremental saves    — flushes CSV every SAVE_EVERY ads
  • transfer.sh upload   — prints a download URL in the logs when done (free, no account)
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, Page

# ──────────────────────────────────────────────────────────────────
# CONFIG  (all overridable via Railway environment variables)
# ──────────────────────────────────────────────────────────────────

BASE_URL  = os.getenv(
    "OLX_BASE_URL",
    "https://www.olx.uz/nedvizhimost/kvartiry/prodazha/?currency=UZS",
)
MAX_PAGES = int(os.getenv("MAX_PAGES", "999"))       # hard ceiling; auto-stops earlier
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
OUTPUT_CSV      = OUTPUT_DIR / "olx_housing.csv"
OUTPUT_XLSX     = OUTPUT_DIR / "olx_housing.xlsx"
LOG_FILE        = OUTPUT_DIR / "scraper.log"

# Timing
AD_WAIT_MS       = (1_800, 3_500)    # ms  — wait after loading each ad
BETWEEN_ADS      = (2.5, 5.0)        # s   — gap between ads
BETWEEN_LIST     = (5.0, 9.0)        # s   — gap between list pages
LONG_BREAK_EVERY = 15                # ads — cadence for a longer rest
LONG_BREAK_SECS  = (15, 25)          # s   — length of that rest
SCROLL_PASSES    = (2, 3)
SCROLL_DIST_PX   = (500, 1_200)
SCROLL_PAUSE     = (0.4, 0.9)
SAVE_EVERY       = 10                # ads — checkpoint/CSV flush interval

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# navigator.webdriver and other bot signals patched away
STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver',           { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',             { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages',           { get: () => ['ru-RU', 'ru', 'uz-UZ', 'en-US'] });
    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
    Object.defineProperty(navigator, 'deviceMemory',        { get: () => 8 });
    window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
    const _origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origQuery(p);
"""

# ──────────────────────────────────────────────────────────────────
# COLUMN ORDER  (the final DataFrame follows this exactly)
# ──────────────────────────────────────────────────────────────────

COLUMNS = [
    "Listing ID",
    "Title",
    "Price",
    "Currency",
    "Negotiation",
    "Area",             # Общая площадь
    "Living Area",      # Жилая площадь
    "Kitchen Area",     # Площадь кухни
    "Num Rooms",
    "Floor",            # floor number only  (from "3 из 9" → 3)
    "Total Floors",     # floors in building (from "3 из 9" → 9)
    "Market Type",      # Тип жилья (вторичный / новостройка)
    "Wall Material",    # Материал стен
    "Condition",        # Состояние / ремонт
    "Transaction Type", # Тип сделки
    "Views",
    "Posted Date",
    "Scraped Date",
    "Seller",
    "Seller Type",      # Частное лицо / Агентство
    "Seller Joined",
    "Phone",
    "Location",
    "Description",
    "URL",
]

# Attribute keys in Russian and Uzbek (Latin / Cyrillic variants)
ATTR_MAP = {
    "Area":             ["Общая площадь", "Umumiy maydoni", "Умумий майдони", "Площадь"],
    "Living Area":      ["Жилая площадь", "Yashash maydoni", "Яшаш майдони"],
    "Kitchen Area":     ["Площадь кухни", "Oshxona maydoni", "Ошхона майдони"],
    "Num Rooms":        ["Количество комнат", "Xonalar soni", "Xona soni", "Комнат"],
    "Floor_raw":        ["Этаж", "Qavat", "Qavati"],
    "Market Type":      ["Тип жилья", "Uy-joy turi", "Uyjoy turi"],
    "Wall Material":    ["Материал стен", "Devor materiali", "Тип дома", "Uy turi"],
    "Condition":        ["Состояние", "Holati", "Ремонт", "Tamir holati", "Tamir"],
    "Transaction Type": ["Тип сделки", "Bitim turi", "Sotish turi"],
}


# ──────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────

def clean(text):
    if not text:
        return None
    t = re.sub(r"\s+", " ", str(text)).strip()
    return t or None


def extract_number(text):
    if not text:
        return None
    text = str(text).replace("\u00a0", "").replace(" ", "")
    m = re.search(r"(\d[\d,\.]*)", text)
    if not m:
        return None
    try:
        return int(float(m.group(1).replace(",", ".")))
    except ValueError:
        return None


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def short_delay(a, b):
    await asyncio.sleep(random.uniform(a, b))


async def human_scroll(page: Page, fast=False):
    passes = 1 if fast else random.randint(*SCROLL_PASSES)
    for _ in range(passes):
        dist = random.randint(*SCROLL_DIST_PX)
        await page.mouse.wheel(0, dist)
        await asyncio.sleep(random.uniform(*SCROLL_PAUSE))
        if not fast and random.random() < 0.3:
            await asyncio.sleep(random.uniform(0.6, 1.4))
    if not fast and random.random() < 0.2:
        await page.mouse.wheel(0, -random.randint(100, 300))
        await asyncio.sleep(random.uniform(0.3, 0.7))


async def random_mouse_move(page: Page):
    try:
        vp = page.viewport_size or {"width": 1440, "height": 900}
        for _ in range(random.randint(2, 4)):
            x = random.randint(100, vp["width"] - 100)
            y = random.randint(100, vp["height"] - 100)
            await page.mouse.move(x, y, steps=random.randint(5, 12))
            await asyncio.sleep(random.uniform(0.05, 0.25))
    except Exception:
        pass


def attr_pick(attrs: dict, keys: list):
    """Return the first matching value from attrs, case-insensitive."""
    for k in keys:
        if attrs.get(k):
            return attrs[k]
        for ak, av in attrs.items():
            if ak and ak.strip().lower() == k.lower():
                return av
    return None


# ──────────────────────────────────────────────────────────────────
# CHECKPOINT  (resume on crash / restart)
# ──────────────────────────────────────────────────────────────────

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, encoding="utf-8") as f:
                cp = json.load(f)
            cp["scraped_urls"] = set(cp.get("scraped_urls", []))
            log.info(
                f"Checkpoint loaded — "
                f"{len(cp['scraped_urls'])} done, "
                f"{len(cp.get('all_links', []))} total links"
            )
            return cp
        except Exception as e:
            log.warning(f"Checkpoint corrupt ({e}), starting fresh")
    return {"all_links": [], "scraped_urls": set(), "data": []}


def save_checkpoint(all_links, scraped_urls, data):
    cp = {
        "all_links":    list(all_links),
        "scraped_urls": list(scraped_urls),
        "data":         data,
    }
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    tmp.replace(CHECKPOINT_FILE)   # atomic replace — avoids corrupt checkpoint on kill


def flush_csv(data):
    if not data:
        return
    df = pd.DataFrame(data)
    df = df[[c for c in COLUMNS if c in df.columns]]
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")


# ──────────────────────────────────────────────────────────────────
# COLLECT ALL LISTING LINKS  (auto-paginate)
# ──────────────────────────────────────────────────────────────────

def page_url(base: str, n: int) -> str:
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}page={n}" if n > 1 else base


async def detect_last_page(page: Page) -> int:
    try:
        # OLX.uz "last page" link
        for sel in [
            'a[data-cy="page-link-last"]',
            'a[data-testid="pagination-last"]',
            'a[aria-label*="last"]',
        ]:
            el = page.locator(sel)
            if await el.count() > 0:
                n = extract_number(await el.first.get_attribute("href") or "")
                if n:
                    return n

        # Fallback: highest number in pagination links
        hrefs = await page.locator('[class*="pagination"] a, [data-testid*="pagination"] a').evaluate_all(
            "els => els.map(e => e.href)"
        )
        nums = []
        for href in hrefs:
            m = re.search(r"[?&]page=(\d+)", href or "")
            if m:
                nums.append(int(m.group(1)))
        if nums:
            return max(nums)
    except Exception:
        pass
    return MAX_PAGES


async def collect_links(page: Page, existing_urls: set) -> list:
    all_links = set(existing_urls)
    last_page = MAX_PAGES

    for pg in range(1, last_page + 1):
        url = page_url(BASE_URL, pg)
        log.info(f"LIST PAGE {pg}/{last_page}  →  {url}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
        except Exception:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        await page.wait_for_timeout(random.randint(2_000, 4_000))
        await human_scroll(page)

        # Auto-detect last page on first iteration
        if pg == 1:
            detected = await detect_last_page(page)
            if detected and detected < last_page:
                last_page = min(detected, MAX_PAGES)
                log.info(f"Auto-detected last page: {last_page}")

        hrefs = await page.locator("a").evaluate_all("els => els.map(e => e.href)")
        before = len(all_links)
        for href in hrefs:
            if href and "/d/obyavlenie/" in href:
                all_links.add(href.split("?")[0])

        gained = len(all_links) - before
        log.info(f"  +{gained} new  (total {len(all_links)})")

        # No new links means we've gone past the real last page
        if gained == 0 and pg > 1:
            log.info("No new links found — stopping pagination early.")
            break

        if pg < last_page:
            await short_delay(*BETWEEN_LIST)

    return list(all_links)


# ──────────────────────────────────────────────────────────────────
# EXTRACT STRUCTURED ATTRIBUTES FROM AD PAGE
# ──────────────────────────────────────────────────────────────────

async def extract_attrs(page: Page) -> dict:
    attrs = {}
    selectors = [
        '[data-nx-name="ListContainer"] li',
        '[data-nx-name="ListContainer"] p',
        '[data-testid="ad-parameters-container"] li',
        '[class*="params"] li',
        '[class*="Params"] li',
        '[class*="attr"] li',
    ]
    for sel in selectors:
        try:
            items = page.locator(sel)
            count = await items.count()
            for i in range(count):
                text = clean(await items.nth(i).inner_text())
                if not text:
                    continue
                if ":" in text:
                    k, _, v = text.partition(":")
                    k, v = clean(k), clean(v)
                    if k and v:
                        attrs[k] = v
        except Exception:
            pass
    return attrs


# ──────────────────────────────────────────────────────────────────
# SCRAPE ONE AD  —  returns dict or None
# ──────────────────────────────────────────────────────────────────

async def scrape_ad(page: Page, url: str) -> dict | None:
    try:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except Exception:
            await asyncio.sleep(6)
            await page.goto(url, wait_until="domcontentloaded", timeout=90_000)

        await page.wait_for_timeout(random.randint(*AD_WAIT_MS))
        await human_scroll(page)
        await random_mouse_move(page)

        # ── Block / CAPTCHA detection ─────────────────────────────
        page_title = await page.title()
        if any(x in page_title.lower() for x in
               ["403", "access denied", "captcha", "just a moment", "blocked"]):
            log.warning(f"BLOCKED — long pause then skipping: {url}")
            await asyncio.sleep(random.uniform(45, 90))
            return None

        body_text = clean(await page.text_content("body")) or ""

        # ── 1. LISTING ID ─────────────────────────────────────────
        listing_id = None
        for sel in ['[data-nx-name="Label2"]', '[data-testid="ad-id"]']:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    raw = clean(await el.first.inner_text()) or ""
                    m = re.search(r"(\d{5,})", raw)
                    if m:
                        listing_id = m.group(1)
                        break
            except Exception:
                pass
        if not listing_id:
            m = re.search(r"\bID[:\s#]*(\d{5,})", body_text, re.I)
            if m:
                listing_id = m.group(1)
        if not listing_id:
            m = re.search(r"-(ID[A-Za-z0-9]+)\.html", url)
            if m:
                listing_id = m.group(1)

        # ── 2. TITLE ──────────────────────────────────────────────
        title = None
        try:
            h1 = page.locator("h1")
            if await h1.count() > 0:
                title = clean(await h1.first.inner_text())
        except Exception:
            pass
        if not title:
            title = clean(page_title.split(":")[0])

        # ── 3. PRICE & CURRENCY ───────────────────────────────────
        price = None
        currency = None
        try:
            pl = page.locator('[data-testid="ad-price-container"]')
            price_text = ""
            if await pl.count() > 0:
                price_text = clean(await pl.first.inner_text()) or ""
            if not price_text:
                m = re.search(r"([\d\s]{4,})\s*(сум|uzs|sum|\$|usd)", body_text, re.I)
                if m:
                    price_text = m.group(0)
            price = extract_number(price_text)
            low = price_text.lower()
            if "$" in price_text or "у.е" in low or "usd" in low:
                currency = "USD"
            elif "сум" in low or "uzs" in low or "sum" in low:
                currency = "UZS"
        except Exception:
            pass

        # ── 4. STRUCTURED ATTRS ───────────────────────────────────
        attrs = await extract_attrs(page)

        # ── 5. AREA (total) ───────────────────────────────────────
        area = clean(attr_pick(attrs, ATTR_MAP["Area"]))
        if not area:
            m = re.search(r"Общая площадь[:\s]*([^\n\r]{1,25})", body_text, re.I)
            if not m:
                m = re.search(r"(\d+[\.,]?\d*)\s*м[²2]", body_text, re.I)
            area = clean(m.group(1)) if m else None

        # ── 6. LIVING AREA ────────────────────────────────────────
        living_area = clean(attr_pick(attrs, ATTR_MAP["Living Area"]))
        if not living_area:
            m = re.search(r"Жилая площадь[:\s]*([^\n\r]{1,25})", body_text, re.I)
            living_area = clean(m.group(1)) if m else None

        # ── 7. KITCHEN AREA ───────────────────────────────────────
        kitchen_area = clean(attr_pick(attrs, ATTR_MAP["Kitchen Area"]))
        if not kitchen_area:
            m = re.search(r"Площадь кухни[:\s]*([^\n\r]{1,25})", body_text, re.I)
            kitchen_area = clean(m.group(1)) if m else None

        # ── 8. ROOMS ──────────────────────────────────────────────
        num_rooms = extract_number(attr_pick(attrs, ATTR_MAP["Num Rooms"]))
        if not num_rooms:
            m = re.search(r"Количество комнат[:\s]*(\d+)", body_text, re.I)
            if not m:
                m = re.search(r"(\d+)[\s-]*комнат", body_text, re.I)
            num_rooms = int(m.group(1)) if m else None

        # ── 9. FLOOR & TOTAL FLOORS ───────────────────────────────
        floor_raw = clean(attr_pick(attrs, ATTR_MAP["Floor_raw"]))
        floor = None
        total_floors = None
        if floor_raw:
            fm = re.match(r"(\d+)\s*(?:из|of|/)\s*(\d+)", floor_raw)
            if fm:
                floor       = int(fm.group(1))
                total_floors = int(fm.group(2))
            else:
                floor = extract_number(floor_raw)
        if floor is None:
            m = re.search(r"Этаж[:\s]*(\d+)\s*(?:из|/)\s*(\d+)", body_text, re.I)
            if m:
                floor        = int(m.group(1))
                total_floors = int(m.group(2))
            else:
                m = re.search(r"Этаж[:\s]*(\d+)", body_text, re.I)
                if m:
                    floor = int(m.group(1))

        # ── 10. MARKET TYPE ───────────────────────────────────────
        market_type = clean(attr_pick(attrs, ATTR_MAP["Market Type"]))
        if not market_type:
            m = re.search(r"Тип жилья[:\s]*([^\n\r]{1,60})", body_text, re.I)
            market_type = clean(m.group(1)) if m else None

        # ── 11. WALL MATERIAL ─────────────────────────────────────
        wall_material = clean(attr_pick(attrs, ATTR_MAP["Wall Material"]))
        if not wall_material:
            m = re.search(r"Материал стен[:\s]*([^\n\r]{1,60})", body_text, re.I)
            wall_material = clean(m.group(1)) if m else None

        # ── 12. CONDITION ─────────────────────────────────────────
        condition = clean(attr_pick(attrs, ATTR_MAP["Condition"]))
        if not condition:
            m = re.search(r"Состояние[:\s]*([^\n\r]{1,60})", body_text, re.I)
            condition = clean(m.group(1)) if m else None

        # ── 13. TRANSACTION TYPE ──────────────────────────────────
        transaction_type = clean(attr_pick(attrs, ATTR_MAP["Transaction Type"]))
        if not transaction_type:
            m = re.search(r"Тип сделки[:\s]*([^\n\r]{1,60})", body_text, re.I)
            transaction_type = clean(m.group(1)) if m else None

        # ── 14. VIEWS ─────────────────────────────────────────────
        views = None
        try:
            vl = page.locator('[data-testid="page-view-counter"]')
            if await vl.count() > 0:
                views = extract_number(await vl.first.inner_text())
        except Exception:
            pass
        if not views:
            m = re.search(r"(\d+)\s*(?:просмотр|view)", body_text, re.I)
            if m:
                views = int(m.group(1))

        # ── 15. POSTED DATE ───────────────────────────────────────
        posted_date = None
        for sel in ['[data-cy="ad-posted-at"]', '[data-testid="ad-posted-at"]']:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    posted_date = clean(await el.first.inner_text())
                    break
            except Exception:
                pass
        if not posted_date:
            for pat in [
                r"Опубликовано[:\s]*([^\n\r]{3,40})",
                r"Дата публикации[:\s]*([^\n\r]{3,40})",
                r"E'lon sanasi[:\s]*([^\n\r]{3,40})",
            ]:
                m = re.search(pat, body_text, re.I)
                if m:
                    posted_date = clean(m.group(1))
                    break

        # ── 16. NEGOTIATION ───────────────────────────────────────
        negotiation = False
        try:
            for sel in ['[data-nx-name="P4"]', '[data-testid="ad-price-container"]']:
                el = page.locator(sel)
                if await el.count() > 0:
                    t = (clean(await el.first.inner_text()) or "").lower()
                    if any(x in t for x in ["договорная", "negotiable", "kelishiladi"]):
                        negotiation = True
                        break
        except Exception:
            pass
        if not negotiation and any(
            x in body_text.lower() for x in ["договорная", "negotiable", "kelishiladi"]
        ):
            negotiation = True

        # ── 17. SELLER NAME ───────────────────────────────────────
        seller = None
        try:
            sl = page.locator('[data-testid="user-profile-user-name"]')
            if await sl.count() > 0:
                seller = clean(await sl.first.inner_text())
        except Exception:
            pass

        # ── 18. SELLER TYPE ───────────────────────────────────────
        seller_type = None
        for sel in [
            '[data-testid="user-profile-seller-type"]',
            '[class*="SellerType"]',
            '[class*="sellerType"]',
        ]:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    seller_type = clean(await el.first.inner_text())
                    if seller_type:
                        break
            except Exception:
                pass
        if not seller_type:
            bt_low = body_text.lower()
            if any(x in bt_low for x in ["агентство", "риелтор", "риэлтор", "агент"]):
                seller_type = "Agency"
            elif any(x in bt_low for x in ["частное лицо", "хусусий шахс", "jismoniy shaxs"]):
                seller_type = "Private"

        # ── 19. SELLER JOINED ─────────────────────────────────────
        seller_joined = None
        try:
            ms = page.locator('[data-testid="member-since"]')
            if await ms.count() > 0:
                seller_joined = clean(await ms.first.inner_text())
        except Exception:
            pass
        if not seller_joined:
            try:
                texts = await page.locator("span, p").all_inner_texts()
                for t in texts:
                    t = clean(t)
                    if t and "на olx с" in t.lower():
                        seller_joined = t
                        break
            except Exception:
                pass

        # ── 20. PHONE  (try clicking "show phone" first) ──────────
        phone = None
        try:
            btn = page.locator(
                '[data-testid="show-phone"], '
                '[class*="show-phone"], '
                'button:has-text("Показать номер"), '
                "button:has-text(\"Raqamni ko\u02BBrsatish\")"
            )
            if await btn.count() > 0:
                await btn.first.click()
                await asyncio.sleep(random.uniform(1.5, 2.5))
                body_text = clean(await page.text_content("body")) or ""
        except Exception:
            pass
        m = re.search(r"(\+?998[\s\-]?\d{2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})", body_text)
        if m:
            phone = re.sub(r"[\s\-]", "", m.group(1))

        # ── 21. LOCATION ──────────────────────────────────────────
        location = None
        for sel in [
            '[data-testid="ad-location-link"]',
            '[data-cy="ad-location"]',
            '[class*="location-text"]',
            '[class*="LocationText"]',
        ]:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    location = clean(await el.first.inner_text())
                    if location:
                        break
            except Exception:
                pass
        if not location:
            try:
                texts = await page.locator("p, span").all_inner_texts()
                for t in texts:
                    t = clean(t)
                    if not t:
                        continue
                    low = t.lower()
                    if any(x in low for x in ["район", "ташкент", "toshkent", "область", "viloyat"]):
                        if len(t) < 120:
                            location = t
                            break
            except Exception:
                pass

        # ── 22. DESCRIPTION ───────────────────────────────────────
        description = None
        try:
            dl = page.locator('[data-testid="ad_description"]')
            if await dl.count() > 0:
                description = clean(await dl.first.inner_text())
        except Exception:
            pass

        # ── Description-based fallbacks for sparse listings ───────
        src = description or body_text
        if not area and src:
            m = re.search(r"(\d+[\.,]?\d*)\s*м[²2]", src, re.I)
            if m:
                area = m.group(1) + " м²"
        if not num_rooms and src:
            m = re.search(r"(\d+)[\s-]*комнат|комнат[:\s]*(\d+)", src, re.I)
            if m:
                num_rooms = int(m.group(1) or m.group(2))
        if not market_type and src:
            sl = src.lower()
            if any(x in sl for x in ["вторичный", "вторичка"]):
                market_type = "Вторичный рынок"
            elif any(x in sl for x in ["новостройка", "первичный"]):
                market_type = "Новостройка"

        return {
            "Listing ID":       listing_id,
            "Title":            title,
            "Price":            price,
            "Currency":         currency,
            "Negotiation":      negotiation,
            "Area":             area,
            "Living Area":      living_area,
            "Kitchen Area":     kitchen_area,
            "Num Rooms":        num_rooms,
            "Floor":            floor,
            "Total Floors":     total_floors,
            "Market Type":      market_type,
            "Wall Material":    wall_material,
            "Condition":        condition,
            "Transaction Type": transaction_type,
            "Views":            views,
            "Posted Date":      posted_date,
            "Scraped Date":     now(),
            "Seller":           seller,
            "Seller Type":      seller_type,
            "Seller Joined":    seller_joined,
            "Phone":            phone,
            "Location":         location,
            "Description":      description,
            "URL":              url,
        }

    except Exception as e:
        log.error(f"FAILED {url}: {e}")
        return None


# ──────────────────────────────────────────────────────────────────
# FILE UPLOAD  (transfer.sh — free, no account, 14-day link)
# ──────────────────────────────────────────────────────────────────

async def upload_result(path: Path):
    """Upload the CSV to transfer.sh and print the download URL to logs."""
    try:
        import httpx
        log.info(f"Uploading {path.name} to transfer.sh …")
        async with httpx.AsyncClient(timeout=300) as client:
            with open(path, "rb") as f:
                r = await client.put(
                    f"https://transfer.sh/{path.name}",
                    content=f.read(),
                    headers={"Max-Days": "14"},
                )
        if r.status_code == 200:
            url = r.text.strip()
            log.info("=" * 60)
            log.info("DOWNLOAD YOUR FILE (link valid 14 days):")
            log.info(f"  {url}")
            log.info("=" * 60)
        else:
            log.warning(f"Upload returned HTTP {r.status_code} — file only saved locally.")
    except Exception as e:
        log.warning(f"Upload failed ({e}) — file only saved locally.")


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("OLX.uz Housing Scraper")
    log.info(f"Base URL   : {BASE_URL}")
    log.info(f"Max pages  : {MAX_PAGES}")
    log.info(f"Output dir : {OUTPUT_DIR.resolve()}")
    log.info("=" * 60)

    # Load checkpoint
    cp = load_checkpoint()
    all_links    = cp.get("all_links", [])
    scraped_urls = cp.get("scraped_urls", set())
    all_data     = cp.get("data", [])

    ua = random.choice(USER_AGENTS)
    log.info(f"User-agent : {ua[:60]}…")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            slow_mo=80,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1440,900",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="ru-RU",
            timezone_id="Asia/Tashkent",
            user_agent=ua,
            extra_http_headers={
                "Accept-Language": "ru-RU,ru;q=0.9,uz;q=0.8,en-US;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "Sec-Ch-Ua-Platform": '"Windows"',
            },
        )
        page = await context.new_page()
        await page.add_init_script(STEALTH_SCRIPT)

        # Warm-up visit to the OLX homepage
        log.info("Warming session on olx.uz...")
        await page.goto("https://www.olx.uz/", wait_until="domcontentloaded", timeout=60_000)
        await short_delay(3, 6)
        await human_scroll(page, fast=True)
        await random_mouse_move(page)

        # Collect links (skip if already in checkpoint)
        if not all_links:
            log.info("Collecting listing links from all pages...")
            all_links = await collect_links(page, scraped_urls)
            save_checkpoint(all_links, scraped_urls, all_data)
        else:
            log.info(f"Reusing {len(all_links)} links from checkpoint")

        pending = [u for u in all_links if u not in scraped_urls]
        log.info(f"Total links: {len(all_links)}  |  Pending: {len(pending)}")

        # Scrape each ad
        for idx, link in enumerate(pending, start=1):
            short_name = (link.rstrip("/").split("/")[-1] or link.rstrip("/").split("/")[-2])[:60]
            log.info(f"[{idx}/{len(pending)}]  {short_name}")

            data = await scrape_ad(page, link)
            if data:
                all_data.append(data)
                scraped_urls.add(link)
                log.info(
                    f"  ✓  rooms={data['Num Rooms']}  "
                    f"area={data['Area']}  "
                    f"floor={data['Floor']}/{data['Total Floors']}  "
                    f"price={data['Price']} {data['Currency']}  "
                    f"loc={str(data['Location'])[:40]}"
                )
            else:
                log.warning("  ✗  skipped")

            await short_delay(*BETWEEN_ADS)

            if idx % SAVE_EVERY == 0:
                save_checkpoint(all_links, scraped_urls, all_data)
                flush_csv(all_data)
                log.info(f"  [saved checkpoint — {len(all_data)} records so far]")

            if idx % LONG_BREAK_EVERY == 0:
                secs = random.uniform(*LONG_BREAK_SECS)
                log.info(f"  ── long pause {secs:.0f}s ──")
                await asyncio.sleep(secs)

        await browser.close()

    # Final save
    if all_data:
        df = pd.DataFrame(all_data)
        df = df[[c for c in COLUMNS if c in df.columns]]
        df.to_csv(OUTPUT_CSV,  index=False, encoding="utf-8-sig")
        df.to_excel(OUTPUT_XLSX, index=False)

        log.info("=" * 60)
        log.info(f"DONE  —  {len(df)} listings saved")
        log.info(f"  → {OUTPUT_CSV}")
        log.info(f"  → {OUTPUT_XLSX}")
        log.info("=" * 60)

        await upload_result(OUTPUT_CSV)
    else:
        log.warning("No data collected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted — checkpoint was last saved at the previous flush.")
