"""Orchestrate Keitaro data synchronization: reference tables, logs, and stats rebuild."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import text, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import (
    KAffiliateNetwork,
    KCampaign,
    KClickLog,
    KConversionLog,
    KDailyStat,
    KGroup,
    KLanding,
    KOffer,
    KStream,
    KStreamOffer,
    KTrafficSource,
)
from app.services.keitaro_api_service import KeitaroAPIService

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    success: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Reference table sync helpers
# ---------------------------------------------------------------------------

def _upsert_ref(db: Session, model, pk_field: str, rows: List[Dict], field_map: Dict[str, str]) -> int:
    """Generic upsert for reference tables. Returns count."""
    if not rows:
        return 0
    count = 0
    now = datetime.utcnow()
    for row in rows:
        pk_val = row.get(pk_field)
        if pk_val is None:
            continue
        values = {db_col: row.get(api_col) for db_col, api_col in field_map.items()}
        values["updated_at"] = now
        pk_col = list(model.__table__.primary_key.columns)[0].name
        values[pk_col] = pk_val

        stmt = insert(model).values(**values)
        update_cols = {k: getattr(stmt.excluded, k) for k in values if k != pk_col}
        stmt = stmt.on_conflict_do_update(index_elements=[pk_col], set_=update_cols)
        db.execute(stmt)
        count += 1
    db.commit()
    return count


def sync_reference_tables(db: Session, api: KeitaroAPIService) -> SyncResult:
    details: Dict[str, Any] = {}
    errors: List[str] = []

    # Groups (all types)
    for gtype in ("campaigns", "offers", "landings"):
        ok, data, err = api.get_groups(gtype)
        if ok:
            cnt = _upsert_ref(db, KGroup, "id", data, {"name": "name", "type": "type"})
            details[f"groups_{gtype}"] = cnt
        elif err:
            errors.append(f"groups/{gtype}: {err}")

    # Affiliate networks
    ok, data, err = api.get_affiliate_networks()
    if ok:
        cnt = _upsert_ref(db, KAffiliateNetwork, "id", data, {
            "name": "name", "postback_url": "postback_url",
            "offer_param": "offer_param", "state": "state",
        })
        details["affiliate_networks"] = cnt
    elif err:
        errors.append(f"affiliate_networks: {err}")

    # Traffic sources
    ok, data, err = api.get_traffic_sources()
    if ok:
        cnt = _upsert_ref(db, KTrafficSource, "id", data, {
            "name": "name", "postback_url": "postback_url", "state": "state",
        })
        details["traffic_sources"] = cnt
    elif err:
        errors.append(f"traffic_sources: {err}")

    # Offers
    ok, data, err = api.get_offers()
    if ok:
        cnt = _upsert_ref(db, KOffer, "id", data, {
            "name": "name", "group_id": "group_id", "network_id": "affiliate_network_id",
            "country": "country", "payout_type": "payout_type",
            "payout_value": "payout_value", "payout_currency": "payout_currency", "state": "state",
        })
        details["offers"] = cnt
    elif err:
        errors.append(f"offers: {err}")

    # Landings
    ok, data, err = api.get_landings()
    if ok:
        cnt = _upsert_ref(db, KLanding, "id", data, {
            "name": "name", "group_id": "group_id", "type": "type",
            "url": "action_payload", "state": "state",
        })
        details["landings"] = cnt
    elif err:
        errors.append(f"landings: {err}")

    # Campaigns
    ok, data, err = api.get_campaigns()
    campaign_ids: List[int] = []
    if ok:
        cnt = _upsert_ref(db, KCampaign, "id", data, {
            "alias": "alias", "name": "name", "group_id": "group_id",
            "source_id": "traffic_source_id", "cost_type": "cost_type", "state": "state",
        })
        campaign_ids = [c["id"] for c in data if c.get("id")]
        details["campaigns"] = cnt
    elif err:
        errors.append(f"campaigns: {err}")

    # Streams (per campaign)
    stream_count = 0
    offer_link_count = 0
    for cid in campaign_ids:
        ok, streams, err = api.get_campaign_streams(cid)
        if not ok:
            continue
        now = datetime.utcnow()
        for s in streams:
            sid = s.get("id")
            if sid is None:
                continue
            stmt = insert(KStream).values(
                stream_id=sid,
                campaign_id=cid,
                name=s.get("name"),
                type=s.get("type"),
                schema_=s.get("schema"),
                position=s.get("position"),
                updated_at=now,
            ).on_conflict_do_update(
                index_elements=["stream_id"],
                set_={
                    "campaign_id": cid,
                    "name": insert(KStream).excluded.name,
                    "type": insert(KStream).excluded.type,
                    "schema_": insert(KStream).excluded.schema_,
                    "position": insert(KStream).excluded.position,
                    "updated_at": now,
                },
            )
            db.execute(stmt)
            stream_count += 1

            # Stream-offer links
            for offer_link in s.get("offers", []):
                oid = offer_link.get("offer_id") or offer_link.get("id")
                if oid is None:
                    continue
                so_stmt = insert(KStreamOffer).values(
                    stream_id=sid,
                    offer_id=oid,
                    share=offer_link.get("share"),
                    state=offer_link.get("state"),
                    updated_at=now,
                ).on_conflict_do_update(
                    constraint="uq_stream_offer",
                    set_={
                        "share": insert(KStreamOffer).excluded.share,
                        "state": insert(KStreamOffer).excluded.state,
                        "updated_at": now,
                    },
                )
                db.execute(so_stmt)
                offer_link_count += 1

    db.commit()
    details["streams"] = stream_count
    details["stream_offers"] = offer_link_count

    return SyncResult(
        success=len(errors) == 0,
        message="Reference sync complete" if not errors else f"Partial sync ({len(errors)} errors)",
        details=details,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Log sync (clicks + conversions)
# ---------------------------------------------------------------------------

def _safe_int(v: Any) -> Optional[int]:
    if v is None or v == "" or v == "null":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_decimal(v: Any) -> Optional[Decimal]:
    if v is None or v == "" or v == "null":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _safe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).lower()
    return s in ("1", "true", "yes")


def _safe_ts(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00").replace(" ", "T"))
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(v), fmt)
        except ValueError:
            continue
    return None


def sync_clicks_log(db: Session, api: KeitaroAPIService, date_from: str, date_to: str) -> SyncResult:
    ok, data, err = api.get_clicks_log(date_from, date_to)
    if not ok:
        return SyncResult(success=False, message=f"API error: {err}", errors=[err or ""])

    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
        return SyncResult(success=True, message="No clicks for range", details={"clicks": 0})

    # Delete existing for date range, then insert
    db.execute(
        text("DELETE FROM k_clicks_log WHERE click_datetime::date BETWEEN :d1 AND :d2"),
        {"d1": date_from, "d2": date_to},
    )

    now = datetime.utcnow()
    count = 0
    for r in rows:
        cid = r.get("click_id") or r.get("id")
        if not cid:
            continue
        db.execute(
            insert(KClickLog).values(
                click_id=str(cid),
                campaign_id=_safe_int(r.get("campaign_id")),
                offer_id=_safe_int(r.get("offer_id")),
                landing_id=_safe_int(r.get("landing_id")),
                stream_id=_safe_int(r.get("stream_id")),
                affiliate_network_id=_safe_int(r.get("affiliate_network_id")),
                ts_id=_safe_int(r.get("ts_id")),
                click_datetime=_safe_ts(r.get("datetime") or r.get("click_datetime")),
                ad_campaign_id=r.get("ad_campaign_id"),
                external_id=r.get("external_id"),
                creative_id=r.get("creative_id"),
                sub_id=r.get("sub_id"),
                sub_id_1=r.get("sub_id_1"),
                sub_id_2=r.get("sub_id_2"),
                sub_id_3=r.get("sub_id_3"),
                sub_id_4=r.get("sub_id_4"),
                sub_id_5=r.get("sub_id_5"),
                country=r.get("country"),
                country_code=r.get("country_code"),
                region=r.get("region"),
                city=r.get("city"),
                os=r.get("os"),
                browser=r.get("browser"),
                device_type=r.get("device_type"),
                device_model=r.get("device_model"),
                language=r.get("language"),
                connection_type=r.get("connection_type"),
                operator=r.get("operator"),
                isp=r.get("isp"),
                ip=r.get("ip"),
                referrer=r.get("referrer"),
                domain=r.get("domain"),
                destination=r.get("destination"),
                is_bot=_safe_bool(r.get("is_bot")),
                is_unique_campaign=_safe_bool(r.get("is_unique_campaign")),
                is_unique_stream=_safe_bool(r.get("is_unique_stream")),
                is_unique_global=_safe_bool(r.get("is_unique_global")),
                is_lead=_safe_bool(r.get("is_lead")),
                is_sale=_safe_bool(r.get("is_sale")),
                is_rejected=_safe_bool(r.get("is_rejected")),
                cost=_safe_decimal(r.get("cost")),
                revenue=_safe_decimal(r.get("revenue")),
                profit=_safe_decimal(r.get("profit")),
                updated_at=now,
            ).on_conflict_do_nothing(index_elements=["click_id"])
        )
        count += 1

    db.commit()
    return SyncResult(success=True, message="OK", details={"clicks_synced": count})


def sync_conversions_log(db: Session, api: KeitaroAPIService, date_from: str, date_to: str) -> SyncResult:
    ok, data, err = api.get_conversions_log(date_from, date_to)
    if not ok:
        return SyncResult(success=False, message=f"API error: {err}", errors=[err or ""])

    rows = data.get("rows", []) if isinstance(data, dict) else []
    if not rows:
        return SyncResult(success=True, message="No conversions for range", details={"conversions": 0})

    db.execute(
        text("DELETE FROM k_conversions_log WHERE (postback_datetime::date BETWEEN :d1 AND :d2) OR (click_datetime::date BETWEEN :d1 AND :d2)"),
        {"d1": date_from, "d2": date_to},
    )

    now = datetime.utcnow()
    count = 0
    for r in rows:
        cid = r.get("conversion_id") or r.get("id")
        if not cid:
            continue
        db.execute(
            insert(KConversionLog).values(
                conversion_id=str(cid),
                campaign_id=_safe_int(r.get("campaign_id")),
                offer_id=_safe_int(r.get("offer_id")),
                landing_id=_safe_int(r.get("landing_id")),
                stream_id=_safe_int(r.get("stream_id")),
                affiliate_network_id=_safe_int(r.get("affiliate_network_id")),
                ts_id=_safe_int(r.get("ts_id")),
                status=r.get("status"),
                revenue=_safe_decimal(r.get("revenue")),
                conversion_type=r.get("conversion_type"),
                postback_datetime=_safe_ts(r.get("postback_datetime")),
                click_datetime=_safe_ts(r.get("click_datetime")),
                ad_campaign_id=r.get("ad_campaign_id"),
                external_id=r.get("external_id"),
                creative_id=r.get("creative_id"),
                sub_id=r.get("sub_id"),
                sub_id_1=r.get("sub_id_1"),
                sub_id_2=r.get("sub_id_2"),
                sub_id_3=r.get("sub_id_3"),
                sub_id_4=r.get("sub_id_4"),
                sub_id_5=r.get("sub_id_5"),
                country=r.get("country"),
                country_code=r.get("country_code"),
                region=r.get("region"),
                city=r.get("city"),
                os=r.get("os"),
                browser=r.get("browser"),
                device_type=r.get("device_type"),
                device_model=r.get("device_model"),
                language=r.get("language"),
                ip=r.get("ip"),
                updated_at=now,
            ).on_conflict_do_nothing(index_elements=["conversion_id"])
        )
        count += 1

    db.commit()
    return SyncResult(success=True, message="OK", details={"conversions_synced": count})


# ---------------------------------------------------------------------------
# Rebuild k_daily_stats from logs
# ---------------------------------------------------------------------------

def rebuild_daily_stats(db: Session, date_from: str, date_to: str) -> SyncResult:
    """Rebuild k_daily_stats by aggregating k_clicks_log + k_conversions_log for the date range."""

    # Delete existing stats for the range
    db.execute(
        text("DELETE FROM k_daily_stats WHERE data_date BETWEEN :d1 AND :d2"),
        {"d1": date_from, "d2": date_to},
    )

    # Aggregate clicks
    db.execute(text("""
        INSERT INTO k_daily_stats (
            data_date, ad_campaign_id, campaign_id, offer_id, stream_id,
            affiliate_network_id, ts_id, country_code,
            clicks, unique_clicks, leads, sales, rejected,
            revenue, cost, profit, updated_at
        )
        SELECT
            cl.click_datetime::date AS data_date,
            cl.ad_campaign_id,
            cl.campaign_id,
            cl.offer_id,
            cl.stream_id,
            cl.affiliate_network_id,
            cl.ts_id,
            cl.country_code,
            COUNT(*) AS clicks,
            COUNT(*) FILTER (WHERE cl.is_unique_campaign = true) AS unique_clicks,
            COUNT(*) FILTER (WHERE cl.is_lead = true) AS leads,
            COUNT(*) FILTER (WHERE cl.is_sale = true) AS sales,
            COUNT(*) FILTER (WHERE cl.is_rejected = true) AS rejected,
            COALESCE(SUM(cl.revenue), 0) AS revenue,
            COALESCE(SUM(cl.cost), 0) AS cost,
            COALESCE(SUM(cl.profit), 0) AS profit,
            NOW() AS updated_at
        FROM k_clicks_log cl
        WHERE cl.click_datetime::date BETWEEN :d1 AND :d2
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    """), {"d1": date_from, "d2": date_to})

    # Update sales/revenue from conversions log
    db.execute(text("""
        WITH conv_agg AS (
            SELECT
                cl.click_datetime::date AS data_date,
                cv.ad_campaign_id,
                cv.campaign_id,
                cv.offer_id,
                cv.stream_id,
                cv.affiliate_network_id,
                cv.ts_id,
                cv.country_code,
                COUNT(*) FILTER (WHERE cv.status = 'lead') AS conv_leads,
                COUNT(*) FILTER (WHERE cv.status = 'sale') AS conv_sales,
                COUNT(*) FILTER (WHERE cv.status = 'rejected') AS conv_rejected,
                COALESCE(SUM(cv.revenue), 0) AS conv_revenue
            FROM k_conversions_log cv
            LEFT JOIN k_clicks_log cl ON cl.click_id = cv.conversion_id
            WHERE cv.postback_datetime::date BETWEEN :d1 AND :d2
               OR cv.click_datetime::date BETWEEN :d1 AND :d2
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        )
        UPDATE k_daily_stats ds SET
            sales = ds.sales + ca.conv_sales,
            leads = ds.leads + ca.conv_leads,
            rejected = ds.rejected + ca.conv_rejected,
            revenue = ds.revenue + ca.conv_revenue,
            updated_at = NOW()
        FROM conv_agg ca
        WHERE ds.data_date = ca.data_date
          AND ds.ad_campaign_id IS NOT DISTINCT FROM ca.ad_campaign_id
          AND ds.campaign_id IS NOT DISTINCT FROM ca.campaign_id
          AND ds.offer_id IS NOT DISTINCT FROM ca.offer_id
          AND ds.stream_id IS NOT DISTINCT FROM ca.stream_id
          AND ds.affiliate_network_id IS NOT DISTINCT FROM ca.affiliate_network_id
          AND ds.ts_id IS NOT DISTINCT FROM ca.ts_id
          AND ds.country_code IS NOT DISTINCT FROM ca.country_code
    """), {"d1": date_from, "d2": date_to})

    db.commit()

    row_count = db.execute(
        text("SELECT COUNT(*) FROM k_daily_stats WHERE data_date BETWEEN :d1 AND :d2"),
        {"d1": date_from, "d2": date_to},
    ).scalar()

    return SyncResult(
        success=True,
        message="OK",
        details={"stats_rows": row_count},
    )
