"""Tests for Pydantic models and validation."""

import pytest
from datetime import date
from pydantic import ValidationError
from models import Installation, Observation, TextObservation, CollectorResult


class TestInstallation:
    def test_valid_installation(self, sample_installation):
        assert sample_installation.id == "ft_liberty"
        assert sample_installation.tier == 1
        assert sample_installation.lat == pytest.approx(35.138)

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            Installation(id="test", name="Test")  # missing branch, tier, lat, lon, nearby_city

    def test_invalid_lat(self):
        with pytest.raises(ValidationError):
            Installation(
                id="test", name="Test", branch="army", tier=1,
                lat=100.0, lon=0.0, nearby_city="Nowhere",
            )

    def test_invalid_tier(self):
        with pytest.raises(ValidationError):
            Installation(
                id="test", name="Test", branch="army", tier=5,
                lat=35.0, lon=-79.0, nearby_city="Nowhere",
            )

    def test_defaults(self):
        inst = Installation(
            id="test", name="Test", branch="navy", tier=2,
            lat=36.0, lon=-76.0, nearby_city="Norfolk, VA",
        )
        assert inst.radius_km == 25
        assert inst.uhaul_locations == []
        assert inst.subreddits == []


class TestObservation:
    def test_metadata_hash_deterministic(self):
        """Same metadata in different insertion order should produce same hash."""
        obs1 = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 1, 15), metric_name="price",
            metric_value=100.0, metadata={"a": 1, "b": 2},
        )
        obs2 = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 1, 15), metric_name="price",
            metric_value=100.0, metadata={"b": 2, "a": 1},
        )
        assert obs1.metadata_hash == obs2.metadata_hash

    def test_metadata_hash_empty(self):
        obs = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 1, 15), metric_name="price",
            metric_value=100.0,
        )
        assert obs.metadata_hash == ""

    def test_to_row_tuple(self):
        obs = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 1, 15), metric_name="price",
            metric_value=99.50, metadata={"origin": "test"},
        )
        row = obs.to_row_tuple()
        assert len(row) == 8
        assert row[0] == "ft_liberty"
        assert row[4] == "price"
        assert row[5] == 99.50


class TestCollectorResult:
    def test_defaults(self):
        result = CollectorResult()
        assert result.observations == []
        assert result.text_observations == []
        assert result.errors == 0
