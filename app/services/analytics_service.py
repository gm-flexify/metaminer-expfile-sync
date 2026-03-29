"""Deep analytics: join FB + Keitaro data by ad_campaign_id.

Multi-level grouping: geo -> affiliate_network -> traffic_source -> offer -> k_campaign -> stream.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def deep_analytics(
    db: Session,
    date_from: date,
    date_to: date,
    group_by: str = "campaign",
    country: Optional[str] = None,
    ad_campaign_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return joined FB + Keitaro analytics.

    group_by levels:
      - campaign: one row per FB campaign
      - country: per campaign + country
      - offer: per campaign + country + offer
      - stream: per campaign + country + offer + k_campaign + stream (deepest)
    """

    group_cols_map = {
        "campaign": "fb.campaign_id",
        "country": "fb.campaign_id, COALESCE(ks.country_code, fb.country)",
        "offer": "fb.campaign_id, COALESCE(ks.country_code, fb.country), ks.offer_id, ko.name",
        "stream": (
            "fb.campaign_id, COALESCE(ks.country_code, fb.country), "
            "ks.offer_id, ko.name, ks.campaign_id, kc.name, ks.stream_id"
        ),
    }

    select_cols_map = {
        "campaign": """
            fb.campaign_id AS fb_campaign_id,
            MAX(fc.name) AS fb_campaign_name,
            MAX(fc.account_id) AS account_id
        """,
        "country": """
            fb.campaign_id AS fb_campaign_id,
            MAX(fc.name) AS fb_campaign_name,
            MAX(fc.account_id) AS account_id,
            COALESCE(ks.country_code, fb.country) AS country
        """,
        "offer": """
            fb.campaign_id AS fb_campaign_id,
            MAX(fc.name) AS fb_campaign_name,
            MAX(fc.account_id) AS account_id,
            COALESCE(ks.country_code, fb.country) AS country,
            ks.offer_id,
            ko.name AS offer_name,
            MAX(kan.name) AS affiliate_network
        """,
        "stream": """
            fb.campaign_id AS fb_campaign_id,
            MAX(fc.name) AS fb_campaign_name,
            MAX(fc.account_id) AS account_id,
            COALESCE(ks.country_code, fb.country) AS country,
            ks.offer_id,
            ko.name AS offer_name,
            MAX(kan.name) AS affiliate_network,
            ks.campaign_id AS k_campaign_id,
            kc.name AS k_campaign_name,
            MAX(kts.name) AS traffic_source,
            ks.stream_id
        """,
    }

    group_cols = group_cols_map.get(group_by, group_cols_map["campaign"])
    select_cols = select_cols_map.get(group_by, select_cols_map["campaign"])

    where_extra = ""
    params: Dict[str, Any] = {"d1": date_from.isoformat(), "d2": date_to.isoformat()}
    if country:
        where_extra += " AND (fb.country = :country OR ks.country_code = :country)"
        params["country"] = country.upper()
    if ad_campaign_id:
        where_extra += " AND fb.campaign_id = :ad_cid"
        params["ad_cid"] = ad_campaign_id

    sql = f"""
    WITH fb AS (
        SELECT
            campaign_id,
            country,
            SUM(spend) AS spend,
            SUM(impressions) AS impressions,
            SUM(reach) AS reach,
            SUM(link_clicks) AS link_clicks,
            SUM(clicks) AS clicks,
            SUM(leads) AS fb_leads,
            SUM(installs) AS fb_installs
        FROM fb_daily_insights
        WHERE data_date BETWEEN :d1 AND :d2
        GROUP BY campaign_id, country
    )
    SELECT
        {select_cols},

        -- FB metrics
        SUM(fb.spend) AS fb_spend,
        SUM(fb.impressions) AS fb_impressions,
        SUM(fb.reach) AS fb_reach,
        SUM(fb.link_clicks) AS fb_link_clicks,
        SUM(fb.clicks) AS fb_clicks,
        SUM(fb.fb_leads) AS fb_leads,
        SUM(fb.fb_installs) AS fb_installs,

        -- KT metrics
        COALESCE(SUM(ks.clicks), 0) AS kt_clicks,
        COALESCE(SUM(ks.unique_clicks), 0) AS kt_unique_clicks,
        COALESCE(SUM(ks.leads), 0) AS kt_leads,
        COALESCE(SUM(ks.sales), 0) AS kt_sales,
        COALESCE(SUM(ks.rejected), 0) AS kt_rejected,
        COALESCE(SUM(ks.revenue), 0) AS kt_revenue,
        COALESCE(SUM(ks.cost), 0) AS kt_cost,

        -- Calculated
        CASE WHEN SUM(fb.fb_leads) > 0
             THEN ROUND(SUM(fb.spend) / SUM(fb.fb_leads), 2) ELSE 0 END AS cpl,
        CASE WHEN COALESCE(SUM(ks.leads), 0) > 0
             THEN ROUND(SUM(fb.spend) / SUM(ks.leads), 2) ELSE 0 END AS cp_reg,
        CASE WHEN COALESCE(SUM(ks.sales), 0) > 0
             THEN ROUND(SUM(fb.spend) / SUM(ks.sales), 2) ELSE 0 END AS cp_ftd,
        CASE WHEN SUM(fb.spend) > 0
             THEN ROUND((COALESCE(SUM(ks.revenue), 0) - SUM(fb.spend)) / SUM(fb.spend) * 100, 2)
             ELSE 0 END AS roi

    FROM fb
    LEFT JOIN fb_campaigns fc ON fb.campaign_id = fc.campaign_id
    LEFT JOIN k_daily_stats ks
        ON ks.ad_campaign_id = fb.campaign_id
        AND ks.data_date BETWEEN :d1 AND :d2
    LEFT JOIN k_offers ko ON ks.offer_id = ko.offer_id
    LEFT JOIN k_campaigns kc ON ks.campaign_id = kc.campaign_id
    LEFT JOIN k_affiliate_networks kan ON ks.affiliate_network_id = kan.network_id
    LEFT JOIN k_traffic_sources kts ON ks.ts_id = kts.source_id

    WHERE 1=1 {where_extra}

    GROUP BY {group_cols}
    ORDER BY fb_spend DESC NULLS LAST
    """

    result = db.execute(text(sql), params)
    columns = list(result.keys())
    return [dict(zip(columns, row)) for row in result.fetchall()]
