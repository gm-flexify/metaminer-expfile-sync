"""HTTP client for Keitaro Admin API."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class KeitaroAPIService:
    def __init__(
        self, api_url: Optional[str] = None, api_key: Optional[str] = None
    ):
        self.api_url = api_url or settings.keitaro_api_url
        self.api_key = api_key or settings.keitaro_api_key
        self.timeout = settings.keitaro_timeout

        if not self.api_url:
            raise ValueError("KEITARO_API_URL must be set")
        if not self.api_key:
            raise ValueError("KEITARO_API_KEY must be set")

        if not self.api_url.endswith("/admin_api/v1"):
            self.api_url = self.api_url.rstrip("/") + "/admin_api/v1"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Any] = None,
    ) -> Tuple[bool, Optional[Any], Optional[str]]:
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        headers = {"Api-Key": self.api_key, "Content-Type": "application/json"}
        t0 = time.time()

        try:
            with httpx.Client(timeout=self.timeout) as client:
                if method.upper() == "GET":
                    resp = client.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    resp = client.post(url, headers=headers, json=json_data)
                else:
                    return False, None, f"Unsupported method: {method}"

                resp.raise_for_status()
                data = resp.json()
                elapsed = time.time() - t0
                logger.info("KT API %s %s -> %d (%.2fs)", method, endpoint, resp.status_code, elapsed)
                return True, data, None

        except httpx.HTTPStatusError as e:
            msg = f"HTTP {e.response.status_code}"
            try:
                msg += f": {e.response.json()}"
            except Exception:
                pass
            logger.warning("KT API error: %s %s -> %s", method, endpoint, msg)
            return False, None, msg
        except httpx.TimeoutException:
            logger.warning("KT API timeout: %s %s", method, endpoint)
            return False, None, f"Timeout after {self.timeout}s"
        except Exception as e:
            logger.error("KT API unexpected: %s %s -> %s", method, endpoint, e, exc_info=True)
            return False, None, str(e)

    # -- Reference data endpoints --

    def get_groups(self, group_type: str = "campaigns") -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/groups", params={"type": group_type})
        return (ok, data if isinstance(data, list) else [], err)

    def get_affiliate_networks(self) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/affiliate_networks")
        return (ok, data if isinstance(data, list) else [], err)

    def get_traffic_sources(self) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/traffic_sources")
        return (ok, data if isinstance(data, list) else [], err)

    def get_offers(self) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/offers")
        return (ok, data if isinstance(data, list) else [], err)

    def get_landings(self) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/landing_pages")
        return (ok, data if isinstance(data, list) else [], err)

    def get_campaigns(self) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", "/campaigns")
        return (ok, data if isinstance(data, list) else [], err)

    def get_campaign_streams(self, campaign_id: int) -> Tuple[bool, List[Dict], Optional[str]]:
        ok, data, err = self._request("GET", f"/campaigns/{campaign_id}/streams")
        return (ok, data if isinstance(data, list) else [], err)

    # -- Log endpoints --

    def get_clicks_log(
        self,
        date_from: str,
        date_to: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        default_columns = [
            "click_id", "campaign_id", "offer_id", "landing_id", "stream_id",
            "affiliate_network_id", "ts_id", "datetime",
            "ad_campaign_id", "external_id", "creative_id",
            "sub_id", "sub_id_1", "sub_id_2", "sub_id_3", "sub_id_4", "sub_id_5",
            "country", "country_code", "region", "city",
            "os", "browser", "device_type", "device_model", "language",
            "connection_type", "operator", "isp", "ip",
            "referrer", "domain", "destination",
            "is_bot", "is_unique_campaign", "is_unique_stream", "is_unique_global",
            "is_lead", "is_sale", "is_rejected",
            "cost", "revenue", "profit",
        ]
        body: Dict[str, Any] = {
            "range": {"from": f"{date_from} 00:00:00", "to": f"{date_to} 23:59:59", "timezone": "UTC"},
            "columns": columns or default_columns,
        }
        if filters:
            body["filters"] = filters
        return self._request("POST", "/clicks/log", json_data=body)

    def get_conversions_log(
        self,
        date_from: str,
        date_to: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        default_columns = [
            "conversion_id", "campaign_id", "offer_id", "landing_id", "stream_id",
            "affiliate_network_id", "ts_id",
            "status", "revenue", "conversion_type",
            "postback_datetime", "click_datetime",
            "ad_campaign_id", "external_id", "creative_id",
            "sub_id", "sub_id_1", "sub_id_2", "sub_id_3", "sub_id_4", "sub_id_5",
            "country", "country_code", "region", "city",
            "os", "browser", "device_type", "device_model", "language", "ip",
        ]
        body: Dict[str, Any] = {
            "range": {"from": f"{date_from} 00:00:00", "to": f"{date_to} 23:59:59", "timezone": "UTC"},
            "columns": columns or default_columns,
        }
        if filters:
            body["filters"] = filters
        return self._request("POST", "/conversions/log", json_data=body)

    # -- Report endpoint --

    def get_report(
        self,
        date_from: str,
        date_to: str,
        columns: Optional[List[str]] = None,
        grouping: Optional[List[str]] = None,
        filters: Optional[List[Dict]] = None,
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        default_columns = [
            "ad_campaign_id",
            "clicks", "campaign_unique_clicks",
            "leads", "sales", "rejected",
            "revenue", "cost", "profit",
        ]
        body: Dict[str, Any] = {
            "range": {"from": f"{date_from} 00:00:00", "to": f"{date_to} 23:59:59", "timezone": "UTC"},
            "columns": columns or default_columns,
            "grouping": grouping or ["ad_campaign_id"],
        }
        if filters:
            body["filters"] = filters
        return self._request("POST", "/report/build", json_data=body)
