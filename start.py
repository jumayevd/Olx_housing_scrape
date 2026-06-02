#!/usr/bin/env python3
"""Railway entry point — starts the daily scheduler."""
import asyncio
from scheduler import main

if __name__ == "__main__":
    asyncio.run(main())
