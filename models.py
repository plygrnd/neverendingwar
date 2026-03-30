"""Typed data models for NEW observations, text records, and installation configs."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pydantic import BaseModel, Field, field_validator


class Installation(BaseModel):
    """Military installation configuration, validated from YAML."""

    id: str
    name: str
    branch: str
    tier: int = Field(ge=1, le=3)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    radius_km: float = Field(default=25, gt=0)
    nearby_city: str
    county: str = ""
    uhaul_locations: list[str] = Field(default_factory=list)
    zillow_region: str = ""
    subreddits: list[str] = Field(default_factory=list)
    news_feeds: list[str] = Field(default_factory=list)


class Observation(BaseModel):
    """A single numeric observation from a collector."""

    installation_id: str
    source: str
    observed_date: date
    metric_name: str
    metric_value: float
    metadata: dict | None = None
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def metadata_hash(self) -> str:
        """Deterministic hash of metadata for deduplication."""
        if not self.metadata:
            return ""
        raw = json.dumps(self.metadata, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def to_row_tuple(self) -> tuple:
        """Column-ordered tuple for INSERT INTO observations."""
        return (
            self.installation_id,
            self.source,
            self.collected_at,
            self.observed_date,
            self.metric_name,
            self.metric_value,
            json.dumps(self.metadata) if self.metadata else None,
            self.metadata_hash,
        )


class TextObservation(BaseModel):
    """A text observation (reddit post, news article)."""

    installation_id: str
    source: str
    observed_date: date
    title: str | None = None
    body: str | None = None
    url: str | None = None
    relevance_score: float | None = None
    metadata: dict | None = None
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_row_tuple(self) -> tuple:
        """Column-ordered tuple for INSERT INTO text_observations."""
        return (
            self.installation_id,
            self.source,
            self.collected_at,
            self.observed_date,
            self.title,
            self.body,
            self.url,
            self.relevance_score,
            json.dumps(self.metadata) if self.metadata else None,
        )


@dataclass
class CollectorResult:
    """Container returned by collect() — separates data from persistence."""

    observations: list[Observation] = field(default_factory=list)
    text_observations: list[TextObservation] = field(default_factory=list)
    errors: int = 0
