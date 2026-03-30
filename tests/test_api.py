"""Tests for FastAPI endpoints."""

from datetime import date
from fastapi.testclient import TestClient
from models import Observation
from db.store import batch_store_observations
from api.main import app

client = TestClient(app)


class TestAPI:
    def test_root(self):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "NEW"
        assert data["status"] == "operational"

    def test_installations_empty(self, fresh_db):
        resp = client.get("/installations")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_installations_with_data(self, fresh_db):
        obs = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date(2024, 6, 1), metric_name="price",
            metric_value=100.0,
        )
        batch_store_observations([obs])

        resp = client.get("/installations")
        data = resp.json()
        assert "ft_liberty" in data
        assert "uhaul" in data["ft_liberty"]["sources"]

    def test_observations_endpoint(self, fresh_db):
        obs = Observation(
            installation_id="ft_liberty", source="uhaul",
            observed_date=date.today(), metric_name="price",
            metric_value=100.0,
        )
        batch_store_observations([obs])

        resp = client.get("/observations/ft_liberty")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["metric"] == "price"
        assert data[0]["value"] == 100.0

    def test_anomalies_empty(self, fresh_db):
        resp = client.get("/anomalies")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_health_empty(self, fresh_db):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_analyze_endpoint(self, fresh_db):
        resp = client.post("/analyze")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "complete"
