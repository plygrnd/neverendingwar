"""
Simple scheduler for running collectors on cadence.

For a shitty VPS, this is more reliable than trying to manage
systemd timers or crontab entries. Just run this as a background
process: `python -m new.scheduler &`

For production: replace with proper task scheduling (APScheduler,
Celery, or just cron entries calling each collector).
"""

import time
import random
import logging
import threading
from config.settings import settings
from collectors.uhaul import UHaulCollector
from collectors.zillow import ZillowCollector
from collectors.reddit import RedditCollector

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("new.scheduler")

COLLECTORS = [
    (UHaulCollector, settings.uhaul_interval),
    (ZillowCollector, settings.zillow_interval),
    (RedditCollector, settings.reddit_interval),
]

MAX_BACKOFF_MULTIPLIER = 4


def run_collector(collector_cls, interval_minutes):
    """Run a collector in a loop with drift correction, jitter, and backoff."""
    name = collector_cls.source_name if hasattr(collector_cls, 'source_name') else collector_cls.__name__
    consecutive_failures = 0

    while True:
        start = time.monotonic()
        try:
            logger.info(f"Starting collection: {name}")
            collector = collector_cls()
            collector.run()
            logger.info(f"Completed collection: {name}")
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            logger.error(f"Collector {name} failed: {e}", exc_info=True)

        elapsed = time.monotonic() - start
        base_interval = interval_minutes * 60

        # Exponential backoff on consecutive failures, capped at 4x
        if consecutive_failures > 0:
            backoff = min(2 ** consecutive_failures, MAX_BACKOFF_MULTIPLIER)
            effective_interval = base_interval * backoff
        else:
            effective_interval = base_interval

        # Jitter: +/- 5% of interval to spread load
        jitter = random.uniform(-0.05, 0.05) * effective_interval

        # Subtract elapsed time to prevent drift
        sleep_time = max(0, effective_interval - elapsed + jitter)

        logger.info(f"Next {name} run in {sleep_time / 60:.1f} minutes")
        time.sleep(sleep_time)


def main():
    logger.info("NEW scheduler starting")
    logger.info(f"Collectors: {len(COLLECTORS)}")

    threads = []
    for collector_cls, interval in COLLECTORS:
        t = threading.Thread(
            target=run_collector,
            args=(collector_cls, interval),
            daemon=True,
        )
        t.start()
        threads.append(t)
        # Stagger collector starts to avoid hammering everything at once
        time.sleep(10)

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler shutting down")


if __name__ == "__main__":
    main()
