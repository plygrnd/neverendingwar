"""Tests for observation deduplication."""

from datetime import date
from models import Observation, TextObservation
from db.store import batch_store_observations, batch_store_text_observations


class TestObservationDedup:
    def test_duplicate_observation_ignored(self, fresh_db):
        """Inserting the same observation twice should result in one row."""
        obs = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 6, 1), metric_name="outbound_price_10ft_truck",
            metric_value=1200.0,
            metadata={"origin": "Fayetteville, NC", "destination": "Atlanta, GA"},
        )
        batch_store_observations([obs])
        batch_store_observations([obs])

        count = fresh_db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        assert count == 1

    def test_different_metadata_not_deduped(self, fresh_db):
        """Same metric but different metadata should create separate rows."""
        obs1 = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 6, 1), metric_name="outbound_price_10ft_truck",
            metric_value=1200.0,
            metadata={"origin": "Fayetteville, NC", "destination": "Atlanta, GA"},
        )
        obs2 = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 6, 1), metric_name="outbound_price_10ft_truck",
            metric_value=1500.0,
            metadata={"origin": "Fayetteville, NC", "destination": "Phoenix, AZ"},
        )
        batch_store_observations([obs1, obs2])

        count = fresh_db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        assert count == 2

    def test_batch_returns_count(self, fresh_db):
        """batch_store should return the number of records passed."""
        obs_list = [
            Observation(
                installation_id="ft_liberty", source="uhaul",
                observed_date=date(2024, 6, 1), metric_name=f"metric_{i}",
                metric_value=float(i),
            )
            for i in range(5)
        ]
        result = batch_store_observations(obs_list)
        assert result == 5

    def test_empty_batch(self, fresh_db):
        assert batch_store_observations([]) == 0


class TestTextObservationDedup:
    def test_duplicate_url_ignored(self, fresh_db):
        """Same URL for same installation+source should be deduped."""
        text = TextObservation(
            installation_id="ft_liberty", source="reddit",
            observed_date=date(2024, 6, 1),
            title="PCS orders came through",
            url="https://reddit.com/r/army/abc123",
            relevance_score=2.5,
        )
        batch_store_text_observations([text])
        batch_store_text_observations([text])

        count = fresh_db.execute("SELECT COUNT(*) FROM text_observations").fetchone()[0]
        assert count == 1
