"""Deep analytics endpoint: FB + Keitaro joined."""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.schemas import AnalyticsResponse
from app.services.analytics_service import deep_analytics

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])


@router.get("/deep", response_model=AnalyticsResponse)
def analytics_deep(
    date_from: date = Query(...),
    date_to: date = Query(...),
    group_by: str = Query("campaign", description="campaign | country | offer | stream"),
    country: Optional[str] = Query(None),
    ad_campaign_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Deep multi-level analytics joining FB spend with Keitaro conversions.

    Grouping chain: geo -> affiliate_network -> traffic_source -> offer -> k_campaign -> stream.
    """
    rows = deep_analytics(
        db,
        date_from=date_from,
        date_to=date_to,
        group_by=group_by,
        country=country,
        ad_campaign_id=ad_campaign_id,
    )
    # Convert Decimal to float for JSON serialization
    cleaned = []
    for row in rows:
        cleaned.append({
            k: float(v) if hasattr(v, "as_tuple") else v
            for k, v in row.items()
        })
    return AnalyticsResponse(rows=cleaned, count=len(cleaned))
