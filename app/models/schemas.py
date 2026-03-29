"""Pydantic request/response schemas."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# FB Import
# ---------------------------------------------------------------------------

class FbImportSkippedRow(BaseModel):
    row_index: int
    reason: str
    campaign_id: Optional[str] = None


class FbImportResponse(BaseModel):
    success: bool
    message: str
    campaigns_upserted: int = 0
    insights_upserted: int = 0
    skipped: List[FbImportSkippedRow] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class FbCampaignOut(BaseModel):
    campaign_id: str
    name: Optional[str] = None
    account_id: Optional[str] = None
    status: Optional[str] = None

    class Config:
        from_attributes = True


class FbDailyInsightOut(BaseModel):
    campaign_id: str
    data_date: date
    country: Optional[str] = None
    spend: Optional[Decimal] = None
    impressions: Optional[int] = None
    reach: Optional[int] = None
    clicks: Optional[int] = None
    link_clicks: Optional[int] = None
    leads: Optional[int] = None
    installs: Optional[int] = None

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Keitaro Sync
# ---------------------------------------------------------------------------

class KeitaroSyncRequest(BaseModel):
    date_from: str = Field(..., description="YYYY-MM-DD")
    date_to: str = Field(..., description="YYYY-MM-DD")


class SyncResultResponse(BaseModel):
    success: bool
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)


class KCampaignOut(BaseModel):
    campaign_id: int
    name: Optional[str] = None
    alias: Optional[str] = None
    source_id: Optional[int] = None
    state: Optional[str] = None

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

class AnalyticsRequest(BaseModel):
    date_from: date
    date_to: date
    group_by: str = Field("campaign", description="campaign | country | offer | stream")
    country: Optional[str] = None
    ad_campaign_id: Optional[str] = None


class AnalyticsRow(BaseModel):
    class Config:
        from_attributes = True
        extra = "allow"


class AnalyticsResponse(BaseModel):
    rows: List[Dict[str, Any]]
    count: int


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
