"""Abstract base for all collectors."""

import time
import logging
import yaml
from abc import ABC, abstractmethod
from pathlib import Path
from config.settings import settings
from db.store import (
    batch_store_observations,
    batch_store_text_observations,
    log_collection_run,
    complete_collection_run,
)
from models import Installation, CollectorResult

logger = logging.getLogger("new.collectors")


def load_installations() -> list[Installation]:
    """Load and validate installation definitions from YAML."""
    config_path = Path(settings.installations_path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Installations config not found at {config_path}. "
            f"Copy installations.yaml.example to installations.yaml and customize."
        )
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return [Installation(**inst) for inst in data.get("installations", [])]


class BaseCollector(ABC):
    """All collectors inherit from this. Handles run logging, rate limiting, and error capture."""

    source_name: str = "unknown"

    def __init__(self):
        self.installations = load_installations()

    def run(self, installation_ids: list[str] | None = None):
        """Execute collection for specified installations (or all)."""
        targets = self.installations
        if installation_ids:
            targets = [i for i in targets if i.id in installation_ids]

        for installation in targets:
            run_id = log_collection_run(self.source_name, installation.id)
            try:
                result = self.collect(installation)
                stored = batch_store_observations(result.observations)
                stored_text = batch_store_text_observations(result.text_observations)
                complete_collection_run(
                    run_id,
                    records=stored + stored_text,
                    records_failed=result.errors,
                )
                logger.info(
                    f"[{self.source_name}] {installation.id}: "
                    f"collected {stored + stored_text} records "
                    f"({result.errors} errors)"
                )
            except Exception as e:
                complete_collection_run(run_id, error=str(e))
                logger.error(
                    f"[{self.source_name}] {installation.id}: {e}", exc_info=True
                )

            # rate limiting between installations
            time.sleep(settings.request_delay_seconds)

    @abstractmethod
    def collect(self, installation: Installation) -> CollectorResult:
        """Collect data for a single installation. Returns structured results."""
        ...
