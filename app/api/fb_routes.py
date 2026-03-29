"""FB file import and query endpoints."""

from __future__ import annotations

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Query, UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.db.models import FbCampaign, FbDailyInsight
from app.models.schemas import (
    FbCampaignOut,
    FbDailyInsightOut,
    FbImportResponse,
    FbImportSkippedRow,
)
from app.services.fb_import_service import import_fb_report

router = APIRouter(prefix="/api/v1/fb", tags=["facebook"])


@router.post("/import-report", response_model=FbImportResponse)
async def import_report(
    file: UploadFile = File(...),
    dry_run: bool = Query(False),
    date_override: Optional[str] = Query(None, description="YYYY-MM-DD fallback date"),
    db: Session = Depends(get_db),
):
    """Upload XLSX/CSV with FB campaign data. Idempotent: same (campaign_id, date, country) overwrites."""
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")

    default_date = None
    if date_override:
        try:
            default_date = date.fromisoformat(date_override)
        except ValueError:
            raise HTTPException(400, f"Invalid date_override format: {date_override}")

    result = import_fb_report(db, data, file.filename or "upload.xlsx", default_date, dry_run)

    return FbImportResponse(
        success=result.success,
        message=result.message,
        campaigns_upserted=result.campaigns_upserted,
        insights_upserted=result.insights_upserted,
        skipped=[FbImportSkippedRow(**s.__dict__) for s in result.skipped],
        errors=result.errors,
    )


@router.get("/campaigns", response_model=List[FbCampaignOut])
def list_campaigns(
    q: Optional[str] = Query(None, description="Search by name"),
    db: Session = Depends(get_db),
):
    query = db.query(FbCampaign)
    if q:
        query = query.filter(FbCampaign.name.ilike(f"%{q}%"))
    return query.order_by(FbCampaign.updated_at.desc()).limit(500).all()


@router.get("/daily-insights", response_model=List[FbDailyInsightOut])
def list_insights(
    campaign_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    limit: int = Query(500, le=5000),
    db: Session = Depends(get_db),
):
    query = db.query(FbDailyInsight)
    if campaign_id:
        query = query.filter(FbDailyInsight.campaign_id == campaign_id)
    if date_from:
        query = query.filter(FbDailyInsight.data_date >= date_from)
    if date_to:
        query = query.filter(FbDailyInsight.data_date <= date_to)
    if country:
        query = query.filter(FbDailyInsight.country == country.upper())
    return query.order_by(FbDailyInsight.data_date.desc()).limit(limit).all()
