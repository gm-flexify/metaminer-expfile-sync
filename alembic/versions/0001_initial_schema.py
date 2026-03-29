"""Initial schema with all tables and countries seed.

Revision ID: 0001
Revises:
Create Date: 2026-03-27
"""
from typing import Sequence, Union
import csv
import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text, inspect

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -- countries
    op.create_table(
        "countries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("alpha-2", sa.String(10), nullable=False),
        sa.Column("alpha-3", sa.String(10), nullable=True),
        sa.Column("country-code", sa.String(10), nullable=True),
        sa.Column("iso_3166-2", sa.String(50), nullable=True),
        sa.Column("region", sa.String(100), nullable=True),
        sa.Column("sub-region", sa.String(100), nullable=True),
        sa.Column("intermediate-region", sa.String(100), nullable=True),
        sa.Column("region-code", sa.String(10), nullable=True),
        sa.Column("sub-region-code", sa.String(10), nullable=True),
        sa.Column("intermediate-region-code", sa.String(10), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("alpha-2"),
    )

    # -- fb_campaigns
    op.create_table(
        "fb_campaigns",
        sa.Column("campaign_id", sa.String(50), nullable=False),
        sa.Column("name", sa.String(500), nullable=True),
        sa.Column("account_id", sa.String(50), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("campaign_id"),
    )

    # -- fb_daily_insights
    op.create_table(
        "fb_daily_insights",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("campaign_id", sa.String(50), nullable=False),
        sa.Column("data_date", sa.Date(), nullable=False),
        sa.Column("country", sa.String(10), nullable=True),
        sa.Column("spend", sa.Numeric(12, 4), nullable=True),
        sa.Column("impressions", sa.Integer(), nullable=True),
        sa.Column("reach", sa.Integer(), nullable=True),
        sa.Column("clicks", sa.Integer(), nullable=True),
        sa.Column("link_clicks", sa.Integer(), nullable=True),
        sa.Column("frequency", sa.Numeric(10, 4), nullable=True),
        sa.Column("cpm", sa.Numeric(10, 4), nullable=True),
        sa.Column("cpc", sa.Numeric(10, 4), nullable=True),
        sa.Column("ctr", sa.Numeric(10, 4), nullable=True),
        sa.Column("leads", sa.Integer(), nullable=True),
        sa.Column("installs", sa.Integer(), nullable=True),
        sa.Column("purchases", sa.Integer(), nullable=True),
        sa.Column("registrations", sa.Integer(), nullable=True),
        sa.Column("subscriptions", sa.Integer(), nullable=True),
        sa.Column("unique_clicks", sa.Integer(), nullable=True),
        sa.Column("unique_ctr", sa.Numeric(10, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["campaign_id"], ["fb_campaigns.campaign_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("campaign_id", "data_date", "country", name="uq_fb_insight"),
    )
    op.create_index("ix_fb_daily_insights_campaign_id", "fb_daily_insights", ["campaign_id"])
    op.create_index("ix_fb_daily_insights_data_date", "fb_daily_insights", ["data_date"])
    op.create_index("ix_fb_daily_insights_country", "fb_daily_insights", ["country"])

    # -- k_groups
    op.create_table(
        "k_groups",
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("type", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("group_id"),
    )

    # -- k_affiliate_networks
    op.create_table(
        "k_affiliate_networks",
        sa.Column("network_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("postback_url", sa.Text(), nullable=True),
        sa.Column("offer_param", sa.String(255), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("network_id"),
    )

    # -- k_traffic_sources
    op.create_table(
        "k_traffic_sources",
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("postback_url", sa.Text(), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("source_id"),
    )

    # -- k_campaigns
    op.create_table(
        "k_campaigns",
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("alias", sa.String(255), nullable=True),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("cost_type", sa.String(20), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["k_groups.group_id"]),
        sa.ForeignKeyConstraint(["source_id"], ["k_traffic_sources.source_id"]),
        sa.PrimaryKeyConstraint("campaign_id"),
    )

    # -- k_offers
    op.create_table(
        "k_offers",
        sa.Column("offer_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("network_id", sa.Integer(), nullable=True),
        sa.Column("country", sa.String(255), nullable=True),
        sa.Column("payout_type", sa.String(10), nullable=True),
        sa.Column("payout_value", sa.Numeric(10, 4), nullable=True),
        sa.Column("payout_currency", sa.String(10), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["k_groups.group_id"]),
        sa.ForeignKeyConstraint(["network_id"], ["k_affiliate_networks.network_id"]),
        sa.PrimaryKeyConstraint("offer_id"),
    )

    # -- k_landings
    op.create_table(
        "k_landings",
        sa.Column("landing_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("group_id", sa.Integer(), nullable=True),
        sa.Column("type", sa.String(50), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["k_groups.group_id"]),
        sa.PrimaryKeyConstraint("landing_id"),
    )

    # -- k_streams
    op.create_table(
        "k_streams",
        sa.Column("stream_id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("type", sa.String(50), nullable=True),
        sa.Column("schema", sa.String(50), nullable=True),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["campaign_id"], ["k_campaigns.campaign_id"]),
        sa.PrimaryKeyConstraint("stream_id"),
    )

    # -- k_stream_offers
    op.create_table(
        "k_stream_offers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("stream_id", sa.Integer(), nullable=False),
        sa.Column("offer_id", sa.Integer(), nullable=False),
        sa.Column("share", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(50), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(["stream_id"], ["k_streams.stream_id"]),
        sa.ForeignKeyConstraint(["offer_id"], ["k_offers.offer_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stream_id", "offer_id", name="uq_stream_offer"),
    )

    # -- k_clicks_log
    op.create_table(
        "k_clicks_log",
        sa.Column("click_id", sa.String(50), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=True),
        sa.Column("offer_id", sa.Integer(), nullable=True),
        sa.Column("landing_id", sa.Integer(), nullable=True),
        sa.Column("stream_id", sa.Integer(), nullable=True),
        sa.Column("affiliate_network_id", sa.Integer(), nullable=True),
        sa.Column("ts_id", sa.Integer(), nullable=True),
        sa.Column("click_datetime", sa.TIMESTAMP(), nullable=True),
        sa.Column("ad_campaign_id", sa.String(255), nullable=True),
        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("creative_id", sa.String(255), nullable=True),
        sa.Column("sub_id", sa.String(255), nullable=True),
        sa.Column("sub_id_1", sa.String(255), nullable=True),
        sa.Column("sub_id_2", sa.String(255), nullable=True),
        sa.Column("sub_id_3", sa.String(255), nullable=True),
        sa.Column("sub_id_4", sa.String(255), nullable=True),
        sa.Column("sub_id_5", sa.String(255), nullable=True),
        sa.Column("country", sa.String(255), nullable=True),
        sa.Column("country_code", sa.String(10), nullable=True),
        sa.Column("region", sa.String(255), nullable=True),
        sa.Column("city", sa.String(255), nullable=True),
        sa.Column("os", sa.String(50), nullable=True),
        sa.Column("browser", sa.String(50), nullable=True),
        sa.Column("device_type", sa.String(50), nullable=True),
        sa.Column("device_model", sa.String(255), nullable=True),
        sa.Column("language", sa.String(50), nullable=True),
        sa.Column("connection_type", sa.String(50), nullable=True),
        sa.Column("operator", sa.String(255), nullable=True),
        sa.Column("isp", sa.String(255), nullable=True),
        sa.Column("ip", sa.String(255), nullable=True),
        sa.Column("referrer", sa.Text(), nullable=True),
        sa.Column("domain", sa.String(255), nullable=True),
        sa.Column("destination", sa.Text(), nullable=True),
        sa.Column("is_bot", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_unique_campaign", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_unique_stream", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_unique_global", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_lead", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_sale", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("is_rejected", sa.Boolean(), nullable=True, server_default="false"),
        sa.Column("cost", sa.Numeric(12, 4), nullable=True, server_default="0"),
        sa.Column("revenue", sa.Numeric(12, 4), nullable=True, server_default="0"),
        sa.Column("profit", sa.Numeric(12, 4), nullable=True, server_default="0"),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("click_id"),
    )
    op.create_index("ix_k_clicks_log_click_datetime", "k_clicks_log", ["click_datetime"])
    op.create_index("ix_k_clicks_log_ad_campaign_id", "k_clicks_log", ["ad_campaign_id"])
    op.create_index("ix_k_clicks_log_country_code", "k_clicks_log", ["country_code"])

    # -- k_conversions_log
    op.create_table(
        "k_conversions_log",
        sa.Column("conversion_id", sa.String(50), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=True),
        sa.Column("offer_id", sa.Integer(), nullable=True),
        sa.Column("landing_id", sa.Integer(), nullable=True),
        sa.Column("stream_id", sa.Integer(), nullable=True),
        sa.Column("affiliate_network_id", sa.Integer(), nullable=True),
        sa.Column("ts_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("revenue", sa.Numeric(12, 4), nullable=True),
        sa.Column("conversion_type", sa.String(50), nullable=True),
        sa.Column("postback_datetime", sa.TIMESTAMP(), nullable=True),
        sa.Column("click_datetime", sa.TIMESTAMP(), nullable=True),
        sa.Column("ad_campaign_id", sa.String(255), nullable=True),
        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("creative_id", sa.String(255), nullable=True),
        sa.Column("sub_id", sa.String(255), nullable=True),
        sa.Column("sub_id_1", sa.String(255), nullable=True),
        sa.Column("sub_id_2", sa.String(255), nullable=True),
        sa.Column("sub_id_3", sa.String(255), nullable=True),
        sa.Column("sub_id_4", sa.String(255), nullable=True),
        sa.Column("sub_id_5", sa.String(255), nullable=True),
        sa.Column("country", sa.String(255), nullable=True),
        sa.Column("country_code", sa.String(10), nullable=True),
        sa.Column("region", sa.String(255), nullable=True),
        sa.Column("city", sa.String(255), nullable=True),
        sa.Column("os", sa.String(50), nullable=True),
        sa.Column("browser", sa.String(50), nullable=True),
        sa.Column("device_type", sa.String(50), nullable=True),
        sa.Column("device_model", sa.String(255), nullable=True),
        sa.Column("language", sa.String(50), nullable=True),
        sa.Column("ip", sa.String(255), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("conversion_id"),
    )
    op.create_index("ix_k_conversions_log_postback_datetime", "k_conversions_log", ["postback_datetime"])
    op.create_index("ix_k_conversions_log_ad_campaign_id", "k_conversions_log", ["ad_campaign_id"])
    op.create_index("ix_k_conversions_log_country_code", "k_conversions_log", ["country_code"])

    # -- k_daily_stats
    op.create_table(
        "k_daily_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("data_date", sa.Date(), nullable=False),
        sa.Column("ad_campaign_id", sa.String(255), nullable=True),
        sa.Column("campaign_id", sa.Integer(), nullable=True),
        sa.Column("offer_id", sa.Integer(), nullable=True),
        sa.Column("stream_id", sa.Integer(), nullable=True),
        sa.Column("affiliate_network_id", sa.Integer(), nullable=True),
        sa.Column("ts_id", sa.Integer(), nullable=True),
        sa.Column("country_code", sa.String(10), nullable=True),
        sa.Column("clicks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_clicks", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("leads", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sales", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("revenue", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("cost", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("profit", sa.Numeric(12, 4), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "data_date", "ad_campaign_id", "campaign_id", "offer_id",
            "stream_id", "country_code", "affiliate_network_id", "ts_id",
            name="uq_k_daily_stats",
        ),
    )
    op.create_index("ix_k_daily_stats_data_date", "k_daily_stats", ["data_date"])
    op.create_index("ix_k_daily_stats_ad_campaign_id", "k_daily_stats", ["ad_campaign_id"])
    op.create_index("ix_k_daily_stats_ad_campaign_date", "k_daily_stats", ["ad_campaign_id", "data_date"])

    # -- Seed countries from CSV
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "countries.csv")
    if os.path.exists(csv_path):
        conn = op.get_bind()
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                alpha2 = row.get("alpha-2", "").strip()
                if not alpha2:
                    continue
                exists = conn.execute(
                    text('SELECT 1 FROM countries WHERE "alpha-2" = :a2 LIMIT 1'),
                    {"a2": alpha2},
                ).fetchone()
                if not exists:
                    conn.execute(
                        text(
                            'INSERT INTO countries (name, "alpha-2", "alpha-3", "country-code", '
                            '"iso_3166-2", region, "sub-region", "intermediate-region", '
                            '"region-code", "sub-region-code", "intermediate-region-code") '
                            "VALUES (:name, :a2, :a3, :cc, :iso, :region, :sub, :inter, :rc, :src, :irc)"
                        ),
                        {
                            "name": row.get("name", ""),
                            "a2": alpha2,
                            "a3": row.get("alpha-3", ""),
                            "cc": row.get("country-code", ""),
                            "iso": row.get("iso_3166-2", ""),
                            "region": row.get("region", ""),
                            "sub": row.get("sub-region", ""),
                            "inter": row.get("intermediate-region", ""),
                            "rc": row.get("region-code", ""),
                            "src": row.get("sub-region-code", ""),
                            "irc": row.get("intermediate-region-code", ""),
                        },
                    )


def downgrade() -> None:
    op.drop_table("k_daily_stats")
    op.drop_table("k_conversions_log")
    op.drop_table("k_clicks_log")
    op.drop_table("k_stream_offers")
    op.drop_table("k_streams")
    op.drop_table("k_landings")
    op.drop_table("k_offers")
    op.drop_table("k_campaigns")
    op.drop_table("k_traffic_sources")
    op.drop_table("k_affiliate_networks")
    op.drop_table("k_groups")
    op.drop_table("fb_daily_insights")
    op.drop_table("fb_campaigns")
    op.drop_table("countries")
