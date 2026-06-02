"""
scheduler.py — runs run_scrape() daily at a fixed time (UTC).
Deploy on Railway and leave it running for 30 days.
"""

import asyncio
import os
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.scraper import run_scrape

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Daily run time — override via env vars if needed
SCRAPE_HOUR   = int(os.environ.get("SCRAPE_HOUR",   "3"))   # 03:00 UTC = 08:00 Tashkent
SCRAPE_MINUTE = int(os.environ.get("SCRAPE_MINUTE", "0"))
RUN_ON_START  = os.environ.get("RUN_ON_START", "true").lower() == "true"


async def main():
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        run_scrape,
        CronTrigger(hour=SCRAPE_HOUR, minute=SCRAPE_MINUTE, timezone="UTC"),
        id="daily_scrape",
        max_instances=1,   # never run two scrapes at the same time
        coalesce=True,     # if missed, run once not multiple times
        misfire_grace_time=3600,
    )

    scheduler.start()
    log.info(f"Scheduler running — daily scrape at {SCRAPE_HOUR:02d}:{SCRAPE_MINUTE:02d} UTC")

    if RUN_ON_START:
        log.info("RUN_ON_START=true — starting first scrape now...")
        await run_scrape()

    # Keep the process alive
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
