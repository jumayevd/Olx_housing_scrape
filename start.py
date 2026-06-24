#!/usr/bin/env python3
"""Railway entry point.

Runs one of two scrapers depending on the SCRAPER env var, so a single image
can back two Railway services:
  SCRAPER=uybor  → uybor.uz API scraper (app/uybor_scraper.py)
  SCRAPER=olx (default) → OLX Playwright scraper (scheduler.py)
"""
import os

if os.getenv("SCRAPER", "olx").lower() == "uybor":
    from app.uybor_scraper import main
    main()
else:
    import asyncio
    from scheduler import main
    asyncio.run(main())
