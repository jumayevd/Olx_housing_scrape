"""
Uybor.uz scraper — pulls sale listings from the public JSON API and upserts
into Supabase. No browser needed: the API returns every field inline, including
geocoordinates (lat/lng). Region/district names are resolved via the reverse-
geocode endpoint (cached per district, so it's cheap).

API: https://api.uybor.uz/api/v1/listings?operationType__eq=sale&limit=50&page=N
Categories: /api/v1/listings/categories  (id → name)
Geocode:    /api/v1/listings/geocode/by-coordinates?lat=..&lng=..
"""

import os
import time
import math
import logging

import requests
from sqlalchemy import create_engine, text

log = logging.getLogger("uybor")

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
API_BASE   = "https://api.uybor.uz/api/v1"
API_URL    = f"{API_BASE}/listings"
CATS_URL   = f"{API_BASE}/listings/categories"
GEO_URL    = f"{API_BASE}/listings/geocode/by-coordinates"
OPERATION  = os.getenv("UYBOR_OPERATION", "sale")
PAGE_SIZE  = int(os.getenv("UYBOR_PAGE_SIZE", "50"))
MAX_PAGES  = int(os.getenv("UYBOR_MAX_PAGES", "400"))
LISTING_URL = "https://uybor.uz/listings/{id}"
MEDIA_URL   = "https://api.uybor.uz/api/v1/media/n/{f}"

DB_USER    = os.environ.get("DB_USER")
DB_PASS    = os.environ.get("DB_PASS")
DB_HOST    = os.environ.get("DB_HOST", "aws-0-ap-southeast-1.pooler.supabase.com")
DB_PORT    = os.environ.get("DB_PORT", "5432")
DB_NAME    = os.environ.get("DB_NAME", "postgres")
TABLE_NAME = os.getenv("TABLE_NAME", "uybor_listings")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://uybor.uz/",
}

CATEGORY_FALLBACK = {
    7: "Квартира", 8: "Дом", 9: "Коттедж", 10: "Для бизнеса",
    11: "Земельный участок", 12: "Офис", 17: "Производство",
    18: "Готовый бизнес", 19: "Здание", 21: "Склад", 23: "Жилой",
    24: "Нежилой", 26: "Комната", 27: "Дача", 28: "Частный дом",
}

# Clean, self-describing schema. (column → SQL type)
# PK is (listing_id, snapshot_date): one row per listing per day (daily snapshots).
SCHEMA = {
    "listing_id":        "BIGINT",
    "snapshot_date":     "DATE",
    "operation_type":    "TEXT",
    "category":          "TEXT",
    "sub_category":      "TEXT",
    "description":       "TEXT",
    "rooms":             "TEXT",
    "area_m2":           "NUMERIC",
    "floor":             "INT",
    "total_floors":      "INT",
    "is_new_building":   "BOOLEAN",
    "renovation":        "TEXT",
    "building_material": "TEXT",
    "price_usd":         "NUMERIC",
    "price_uzs":         "NUMERIC",
    "currency":          "TEXT",
    "region":            "TEXT",
    "city":              "TEXT",
    "district":          "TEXT",
    "address":           "TEXT",
    "latitude":          "DOUBLE PRECISION",
    "longitude":         "DOUBLE PRECISION",
    "views":             "INT",
    "clicks":            "INT",
    "favorites":         "INT",
    "is_vip":            "BOOLEAN",
    "is_premium":        "BOOLEAN",
    "is_urgently":       "BOOLEAN",
    "seller_id":         "BIGINT",
    "photos_count":      "INT",
    "main_photo":        "TEXT",
    "posted_at":         "TEXT",
    "bumped_at":         "TEXT",
    "expires_at":        "TEXT",
    "url":               "TEXT",
    "scraped_at":        "TEXT",
}
COLUMNS = list(SCHEMA.keys())
UPDATE_COLUMNS = [c for c in COLUMNS if c not in ("listing_id", "snapshot_date")]


# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────

def get_engine():
    if not DB_USER or not DB_PASS:
        raise RuntimeError("DB_USER and DB_PASS environment variables are required.")
    from sqlalchemy.engine import URL
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER, password=DB_PASS,
        host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_table(engine):
    """Create the table if needed; PK = (listing_id, snapshot_date) so each daily
    run APPENDS a fresh snapshot per listing instead of overwriting. A same-day
    re-run updates that day's row (idempotent)."""
    cols_sql = ",\n                ".join(f"{c} {t}" for c, t in SCHEMA.items())
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n                {cols_sql},\n"
            f"                PRIMARY KEY (listing_id, snapshot_date)\n            )"
        ))
        for c, t in SCHEMA.items():
            if c == "listing_id":
                continue
            # snapshot_date needs a default so ADD COLUMN backfills legacy rows.
            coldef = f"{c} {t} DEFAULT CURRENT_DATE" if c == "snapshot_date" else f"{c} {t}"
            conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {coldef}"))
        # Migrate a legacy single-column PK (listing_id) → (listing_id, snapshot_date).
        conn.execute(text(
            f"UPDATE {TABLE_NAME} SET snapshot_date = "
            f"COALESCE(NULLIF(left(scraped_at, 10), '')::date, CURRENT_DATE) "
            f"WHERE snapshot_date IS NULL"
        ))
        conn.execute(text(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN snapshot_date SET NOT NULL"))
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
    log.info(f"Table '{TABLE_NAME}' ready (daily-snapshot mode: PK = listing_id + snapshot_date).")


def save_batch(rows, engine):
    if not rows:
        return 0
    cols = ", ".join(COLUMNS)
    vals = ", ".join(f":{c}" for c in COLUMNS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLUMNS)
    sql = text(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({vals}) "
               f"ON CONFLICT (listing_id, snapshot_date) DO UPDATE SET {updates}")
    saved = 0
    with engine.begin() as conn:
        for r in rows:
            try:
                conn.execute(sql, r)
                saved += 1
            except Exception as e:
                log.warning(f"save failed for {r.get('listing_id')}: {e}")
    return saved


# ─────────────────────────────────────────────────────────────────
# REFERENCE DATA: categories + reverse-geocoded location names
# ─────────────────────────────────────────────────────────────────

def fetch_categories(session):
    try:
        r = session.get(CATS_URL, params={"limit": 200}, headers=HEADERS, timeout=20)
        items = (r.json() or {}).get("results", [])
        cmap = {}
        for c in items:
            nm = c.get("name")
            cmap[c.get("id")] = (nm.get("ru") if isinstance(nm, dict) else nm) or None
        if cmap:
            log.info(f"loaded {len(cmap)} categories")
            return cmap
    except Exception as e:
        log.warning(f"category fetch failed, using fallback: {e}")
    return dict(CATEGORY_FALLBACK)


def _geocode(session, lat, lng):
    """Reverse-geocode → (region, city, district) names.

    region  = the oblast ('… область'), district = '… район',
    city     = the locality (e.g. Ташкент / Самарканд).
    """
    try:
        r = session.get(GEO_URL, params={"lat": lat, "lng": lng}, headers=HEADERS, timeout=20)
        results = (r.json() or {}).get("results", [])
    except Exception:
        return None, None, None
    region = city = district = None
    province_fallback = None
    for it in results:
        kind = it.get("kind")
        name = (it.get("name") or "").strip()
        if not name:
            continue
        low = name.lower()
        if kind == "district" and low.endswith("район") and district is None:
            district = name
        elif kind == "locality" and city is None:
            city = name
        elif kind == "province":
            if "область" in low and region is None:
                region = name
            elif province_fallback is None:
                province_fallback = name      # e.g. 'Ташкент' (city as province)
    # Fall back so region/city are never both empty.
    if region is None:
        region = province_fallback or city
    if city is None:
        city = province_fallback
    return region, city, district


def resolve_location(session, item, cache):
    """region/city/district for a listing, cached by districtId (stable per district)."""
    lat, lng = item.get("lat"), item.get("lng")
    if lat is None or lng is None:
        return None, None, None
    did = item.get("districtId")
    key = did if did is not None else (round(lat, 5), round(lng, 5))
    if key in cache:
        return cache[key]
    loc = _geocode(session, lat, lng)
    cache[key] = loc
    time.sleep(0.15)   # polite, only on cache miss
    return loc


# ─────────────────────────────────────────────────────────────────
# MAPPING
# ─────────────────────────────────────────────────────────────────

def _num(v):
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _int(v):
    try:
        return int(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def map_listing(item, cmap, region, city, district, now_str):
    prices = item.get("prices") or {}
    media  = item.get("media") or []
    main_photo = None
    if media:
        m0 = media[0]
        main_photo = m0.get("url") or (MEDIA_URL.format(f=m0.get("fileName")) if m0.get("fileName") else None)
    return {
        "listing_id":        item.get("id"),
        "snapshot_date":     now_str[:10],   # 'YYYY-MM-DD' — part of the daily-snapshot PK
        "operation_type":    item.get("operationType"),
        "category":          cmap.get(item.get("categoryId")),
        "sub_category":      cmap.get(item.get("subCategoryId")),
        "description":       item.get("description"),
        "rooms":             str(item.get("room")) if item.get("room") is not None else None,
        "area_m2":           _num(item.get("square")),
        "floor":             _int(item.get("floor")),
        "total_floors":      _int(item.get("floorTotal")),
        "is_new_building":   bool(item.get("isNewBuilding")),
        "renovation":        item.get("repair"),
        "building_material": item.get("foundation"),
        "price_usd":         _num(prices.get("usd")) or _num(item.get("price") if (item.get("priceCurrency") or "").lower() == "usd" else None),
        "price_uzs":         _num(prices.get("uzs")),
        "currency":          (item.get("priceCurrency") or "").upper() or None,
        "region":            region,
        "city":              city,
        "district":          district,
        "address":           item.get("address"),
        "latitude":          _num(item.get("lat")),
        "longitude":         _num(item.get("lng")),
        "views":             _int(item.get("views")),
        "clicks":            _int(item.get("clicks")),
        "favorites":         _int(item.get("favorites")),
        "is_vip":            bool(item.get("isVip")),
        "is_premium":        bool(item.get("isPremium")),
        "is_urgently":       bool(item.get("isUrgently")),
        "seller_id":         _int(item.get("userId")),
        "photos_count":      len(media),
        "main_photo":        main_photo,
        "posted_at":         item.get("createdAt"),
        "bumped_at":         item.get("upAt"),
        "expires_at":        item.get("expiredAt"),
        "url":               LISTING_URL.format(id=item.get("id")),
        "scraped_at":        now_str,
    }


# ─────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────

def fetch_page(session, page):
    params = {"operationType__eq": OPERATION, "limit": PAGE_SIZE, "page": page}
    for attempt in range(4):
        try:
            resp = session.get(API_URL, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            log.warning(f"page {page}: HTTP {resp.status_code} (attempt {attempt+1}/4)")
        except Exception as e:
            log.warning(f"page {page}: {e} (attempt {attempt+1}/4)")
        time.sleep((attempt + 1) * 3)
    return None


def run_scrape():
    from datetime import datetime
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*55}\n  UYBOR SCRAPE STARTED — {now_str}\n{'='*55}")

    try:
        engine = get_engine()
        ensure_table(engine)
    except Exception as e:
        print(f"FATAL DB ERROR: {e}")
        return 0

    session = requests.Session()
    cmap = fetch_categories(session)
    geo_cache = {}
    total_saved = total_seen = 0
    expected = None

    for page in range(1, MAX_PAGES + 1):
        data = fetch_page(session, page)
        if data is None:
            print(f"  page {page}: failed after retries — stopping.")
            break
        results = data.get("results") or []
        if expected is None:
            expected = data.get("total")
            pages = math.ceil(expected / PAGE_SIZE) if expected else "?"
            print(f"  total listings: {expected}  (~{pages} pages of {PAGE_SIZE})")
        if not results:
            print(f"  page {page}: empty — reached the end.")
            break

        rows = []
        for it in results:
            if not it.get("id"):
                continue
            region, city, district = resolve_location(session, it, geo_cache)
            rows.append(map_listing(it, cmap, region, city, district, now_str))
        saved = save_batch(rows, engine)
        total_saved += saved
        total_seen += len(results)
        print(f"  page {page}: {len(results)} fetched, {saved} upserted "
              f"(total {total_seen}/{expected}, geo-cache {len(geo_cache)})")

        if expected and total_seen >= expected:
            print("  collected all expected listings — stopping.")
            break
        time.sleep(0.5)

    print(f"\n{'='*55}\n  UYBOR DONE — {total_saved} upserted, {total_seen} seen, "
          f"{len(geo_cache)} districts geocoded — "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*55}\n")
    return total_saved


# ─────────────────────────────────────────────────────────────────
# SCHEDULER ENTRY POINT
# ─────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    hour   = int(os.getenv("SCRAPE_HOUR", "12"))
    minute = int(os.getenv("SCRAPE_MINUTE", "0"))
    run_on_start = os.getenv("RUN_ON_START", "true").lower() == "true"

    if run_on_start:
        log.info("RUN_ON_START=true — running first uybor scrape now...")
        try:
            run_scrape()
        except Exception as e:
            log.error(f"initial scrape failed: {e}")

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_scrape,
        CronTrigger(hour=hour, minute=minute, timezone="UTC"),
        id="daily_uybor_scrape",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    log.info(f"Uybor scheduler running — daily scrape at {hour:02d}:{minute:02d} UTC")
    scheduler.start()


if __name__ == "__main__":
    main()
