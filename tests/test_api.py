"""Tests for FastAPI endpoints (health, basic routing)."""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root():
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert "app" in data
    assert "version" in data


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_fb_import_empty_file():
    resp = client.post(
        "/api/v1/fb/import-report",
        files={"file": ("empty.csv", b"", "text/csv")},
    )
    assert resp.status_code == 400


def test_fb_import_no_file():
    resp = client.post("/api/v1/fb/import-report")
    assert resp.status_code == 422
