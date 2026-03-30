"""Environment-driven configuration for NEW."""

import os
from pathlib import Path
from dataclasses import dataclass, field

BASE_DIR = Path(__file__).parent.parent


@dataclass
class Settings:
    # Database
    db_path: str = os.getenv("NEW_DB_PATH", str(BASE_DIR / "data" / "new.duckdb"))

    # Collection cadence (minutes)
    uhaul_interval: int = int(os.getenv("NEW_UHAUL_INTERVAL", "360"))  # 6 hours
    zillow_interval: int = int(os.getenv("NEW_ZILLOW_INTERVAL", "1440"))  # daily
    google_interval: int = int(os.getenv("NEW_GOOGLE_INTERVAL", "360"))  # 6 hours
    reddit_interval: int = int(os.getenv("NEW_REDDIT_INTERVAL", "60"))  # hourly
    news_interval: int = int(os.getenv("NEW_NEWS_INTERVAL", "120"))  # 2 hours

    # API keys (optional, enables richer collection)
    google_api_key: str = os.getenv("NEW_GOOGLE_API_KEY", "")
    zillow_api_key: str = os.getenv("NEW_ZILLOW_API_KEY", "")  # bridge interactive / ZWSID
    reddit_client_id: str = os.getenv("NEW_REDDIT_CLIENT_ID", "")
    reddit_client_secret: str = os.getenv("NEW_REDDIT_CLIENT_SECRET", "")

    # Analysis
    baseline_window_days: int = int(os.getenv("NEW_BASELINE_WINDOW", "90"))
    anomaly_zscore_threshold: float = float(os.getenv("NEW_ANOMALY_THRESHOLD", "2.0"))

    # Installations config
    installations_path: str = os.getenv(
        "NEW_INSTALLATIONS_PATH",
        str(BASE_DIR / "config" / "installations.yaml"),
    )

    # Scraping
    user_agent: str = os.getenv(
        "NEW_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    )
    request_delay_seconds: float = float(os.getenv("NEW_REQUEST_DELAY", "2.0"))

    # Logging
    log_level: str = os.getenv("NEW_LOG_LEVEL", "INFO")


settings = Settings()
