"""Keitaro sync and query endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.db.models import KCampaign, KDailyStat
from app.models.schemas import (
    KCampaignOut,
    KeitaroSyncRequest,
    KtLiveStatsResponse,
    SyncResultResponse,
)
from app.services.keitaro_api_service import KeitaroAPIService
from app.services.keitaro_sync_service import (
    rebuild_daily_stats,
    sync_clicks_log,
    sync_clicks_log_chunked,
    sync_conversions_log,
    sync_reference_tables,
)

router = APIRouter(prefix="/api/v1/keitaro", tags=["keitaro"])


def _get_api() -> KeitaroAPIService:
    try:
        return KeitaroAPIService()
    except ValueError as e:
        raise HTTPException(500, str(e))


@router.post("/sync-reference", response_model=SyncResultResponse)
def sync_reference(db: Session = Depends(get_db)):
    """Sync all Keitaro reference tables (campaigns, offers, streams, etc.)."""
    api = _get_api()
    result = sync_reference_tables(db, api)
    return SyncResultResponse(
        success=result.success,
        message=result.message,
        details=result.details,
        errors=result.errors,
    )


@router.post("/sync-logs", response_model=SyncResultResponse)
def sync_logs(body: KeitaroSyncRequest, db: Session = Depends(get_db)):
    """Sync clicks + conversions logs for date range, then rebuild k_daily_stats."""
    api = _get_api()
    errors = []
    details = {}

    # Chunk clicks day-by-day to avoid Keitaro 504 on large date ranges
    clicks_res = sync_clicks_log_chunked(db, api, body.date_from, body.date_to)
    details["clicks"] = clicks_res.details
    if not clicks_res.success:
        errors.extend(clicks_res.errors)

    conv_res = sync_conversions_log(db, api, body.date_from, body.date_to)
    details["conversions"] = conv_res.details
    if not conv_res.success:
        errors.extend(conv_res.errors)

    stats_res = rebuild_daily_stats(db, body.date_from, body.date_to)
    details["daily_stats"] = stats_res.details
    if not stats_res.success:
        errors.extend(stats_res.errors)

    return SyncResultResponse(
        success=len(errors) == 0,
        message="Sync complete" if not errors else f"Partial ({len(errors)} errors)",
        details=details,
        errors=errors,
    )


@router.get("/live-stats", response_model=KtLiveStatsResponse)
def live_stats(
    date_from: date = Query(...),
    date_to: date = Query(...),
    group_by: str = Query("campaign", description="campaign | day | country"),
):
    """Fetch aggregated stats directly from Keitaro report API (no DB write).

    Returns clicks, unique_clicks, leads, sales, rejected, revenue, cost, profit
    grouped by the selected dimension. Called every 15 min from n8n for live monitoring.
    """
    grouping_map: Dict[str, List[str]] = {
        "campaign": ["ad_campaign_id"],
        "day":      ["date", "ad_campaign_id"],
        "country":  ["ad_campaign_id", "country"],
    }
    api = _get_api()
    ok, data, err = api.get_report(
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        grouping=grouping_map.get(group_by, ["ad_campaign_id"]),
    )
    if not ok:
        raise HTTPException(502, f"Keitaro API error: {err}")

    rows: List[Dict[str, Any]] = (
        data.get("rows", []) if isinstance(data, dict) else (data or [])
    )
    return KtLiveStatsResponse(
        date_from=date_from.isoformat(),
        date_to=date_to.isoformat(),
        group_by=group_by,
        rows=rows,
        count=len(rows),
    )


@router.get("/campaigns", response_model=List[KCampaignOut])
def list_campaigns(
    q: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(KCampaign)
    if q:
        query = query.filter(KCampaign.name.ilike(f"%{q}%"))
    return query.order_by(KCampaign.campaign_id.desc()).limit(500).all()


@router.get("/stats")
def query_stats(
    date_from: str = Query(...),
    date_to: str = Query(...),
    ad_campaign_id: Optional[str] = Query(None),
    country_code: Optional[str] = Query(None),
    limit: int = Query(500, le=5000),
    db: Session = Depends(get_db),
):
    query = db.query(KDailyStat).filter(
        KDailyStat.data_date >= date_from,
        KDailyStat.data_date <= date_to,
    )
    if ad_campaign_id:
        query = query.filter(KDailyStat.ad_campaign_id == ad_campaign_id)
    if country_code:
        query = query.filter(KDailyStat.country_code == country_code.upper())
    rows = query.order_by(KDailyStat.data_date.desc()).limit(limit).all()
    return [
        {
            "data_date": r.data_date.isoformat(),
            "ad_campaign_id": r.ad_campaign_id,
            "campaign_id": r.campaign_id,
            "offer_id": r.offer_id,
            "stream_id": r.stream_id,
            "country_code": r.country_code,
            "clicks": r.clicks,
            "unique_clicks": r.unique_clicks,
            "leads": r.leads,
            "sales": r.sales,
            "revenue": float(r.revenue),
            "cost": float(r.cost),
        }
        for r in rows
    ]
