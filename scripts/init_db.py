#!/usr/bin/env python3
"""Initialize the database and create tables."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.storage.database import init_database


async def main():
    print("Initializing database...")
    db = await init_database()
    print("Database initialized successfully!")
    print("Tables created: klines, aggtrades, signals")
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
