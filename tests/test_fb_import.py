"""Tests for FB import service: parsing, aliases, idempotency."""

import io
import csv
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from app.services.fb_import_service import (
    FIELD_ALIASES,
    _map_headers,
    _norm,
    import_fb_report,
    is_plausible_fb_campaign_id,
    normalize_campaign_id,
    parse_date_value,
    parse_file,
)


class TestAliases:
    def test_amount_spent_usd(self):
        assert FIELD_ALIASES[_norm("Amount spent (USD)")] == "spend"

    def test_amount_spent_eur(self):
        assert FIELD_ALIASES[_norm("Amount spent (EUR)")] == "spend"

    def test_clicks_all_ukr(self):
        assert FIELD_ALIASES[_norm("Кліки (усі)")] == "clicks"

    def test_link_clicks_en(self):
        assert FIELD_ALIASES[_norm("Link clicks")] == "link_clicks"

    def test_link_clicks_ukr(self):
        assert FIELD_ALIASES[_norm("кліки по посиланню")] == "link_clicks"

    def test_app_installs(self):
        assert FIELD_ALIASES[_norm("App Installs")] == "installs"

    def test_vitrachena_suma(self):
        assert FIELD_ALIASES[_norm("Витрачена сума")] == "spend"

    def test_leads_ukr(self):
        assert FIELD_ALIASES[_norm("Ліди")] == "leads"

    def test_country_ukr(self):
        assert FIELD_ALIASES[_norm("Країна")] == "country"


class TestNormalization:
    def test_campaign_id_float(self):
        assert normalize_campaign_id(52516677550841.0) == "52516677550841"

    def test_campaign_id_string(self):
        assert normalize_campaign_id("  12345678901  ") == "12345678901"

    def test_campaign_id_none(self):
        assert normalize_campaign_id(None) is None

    def test_plausible_id_ok(self):
        assert is_plausible_fb_campaign_id("52516677550841") is True

    def test_plausible_id_short(self):
        assert is_plausible_fb_campaign_id("12345") is False

    def test_plausible_id_all(self):
        assert is_plausible_fb_campaign_id("All") is False


class TestDateParsing:
    def test_iso(self):
        assert parse_date_value("2026-03-25") == date(2026, 3, 25)

    def test_dot_format(self):
        assert parse_date_value("25.03.2026") == date(2026, 3, 25)

    def test_none_with_default(self):
        d = date(2026, 1, 1)
        assert parse_date_value(None, d) == d


class TestCSVParsing:
    def test_parse_csv_basic(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Campaign ID", "Day", "Amount spent", "Impressions", "Leads"])
        w.writerow(["52516677550841", "2026-03-20", "50.5", "1000", "10"])
        w.writerow(["52516677550841", "2026-03-21", "30.0", "500", "5"])
        data = buf.getvalue().encode("utf-8")

        header, rows = parse_file(data, "test.csv")
        assert len(rows) == 2
        col_map = _map_headers(header)
        assert "campaign_id" in col_map
        assert "spend" in col_map


class TestImportService:
    def test_dry_run_csv(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Campaign ID", "Day", "Country", "Amount spent (USD)", "Impressions", "Link clicks", "Leads"])
        w.writerow(["52516677550841", "2026-03-20", "EC", "50.5", "1000", "200", "10"])
        data = buf.getvalue().encode("utf-8")

        db = MagicMock()
        result = import_fb_report(db, data, "test.csv", dry_run=True)

        assert result.success is True
        assert result.campaigns_upserted == 1
        assert result.insights_upserted == 1
        assert "dry_run" in result.message

    def test_missing_header(self):
        db = MagicMock()
        result = import_fb_report(db, b"just,some,data\n1,2,3", "test.csv")
        assert result.success is False
        assert "campaign_id" in result.message.lower() or "header" in result.message.lower()

    def test_empty_file(self):
        db = MagicMock()
        result = import_fb_report(db, b"", "test.csv")
        assert result.success is False

    def test_link_clicks_fallback(self):
        """When no Link clicks column, clicks value should fill link_clicks."""
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Campaign ID", "Day", "Кліки (усі)", "Витрачена сума", "Leads"])
        w.writerow(["52516677550841", "2026-03-20", "500", "10.0", "5"])
        data = buf.getvalue().encode("utf-8")

        db = MagicMock()
        result = import_fb_report(db, data, "test.csv", dry_run=True)
        assert result.success is True
        assert result.insights_upserted == 1
