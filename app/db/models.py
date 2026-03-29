"""SQLAlchemy models for all 14 tables."""

from datetime import datetime, date as date_type
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    TIMESTAMP,
    UniqueConstraint,
    ForeignKey,
    Index,
)
from app.db.database import Base


# ---------------------------------------------------------------------------
# Countries (reference)
# ---------------------------------------------------------------------------
class Country(Base):
    __tablename__ = "countries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=True)
    alpha_2 = Column("alpha-2", String(10), unique=True, nullable=False)
    alpha_3 = Column("alpha-3", String(10), nullable=True)
    country_code = Column("country-code", String(10), nullable=True)
    iso_3166_2 = Column("iso_3166-2", String(50), nullable=True)
    region = Column(String(100), nullable=True)
    sub_region = Column("sub-region", String(100), nullable=True)
    intermediate_region = Column("intermediate-region", String(100), nullable=True)
    region_code = Column("region-code", String(10), nullable=True)
    sub_region_code = Column("sub-region-code", String(10), nullable=True)
    intermediate_region_code = Column("intermediate-region-code", String(10), nullable=True)


# ---------------------------------------------------------------------------
# Facebook tables
# ---------------------------------------------------------------------------
class FbCampaign(Base):
    __tablename__ = "fb_campaigns"

    campaign_id = Column(String(50), primary_key=True)
    name = Column(String(500), nullable=True)
    account_id = Column(String(50), nullable=True)
    status = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class FbDailyInsight(Base):
    __tablename__ = "fb_daily_insights"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    campaign_id = Column(
        String(50),
        ForeignKey("fb_campaigns.campaign_id"),
        nullable=False,
        index=True,
    )
    data_date = Column(Date, nullable=False, index=True)
    country = Column(String(10), nullable=True, index=True)

    spend = Column(Numeric(12, 4), nullable=True)
    impressions = Column(Integer, nullable=True)
    reach = Column(Integer, nullable=True)
    clicks = Column(Integer, nullable=True)
    link_clicks = Column(Integer, nullable=True)
    frequency = Column(Numeric(10, 4), nullable=True)
    cpm = Column(Numeric(10, 4), nullable=True)
    cpc = Column(Numeric(10, 4), nullable=True)
    ctr = Column(Numeric(10, 4), nullable=True)
    leads = Column(Integer, nullable=True)
    installs = Column(Integer, nullable=True)
    purchases = Column(Integer, nullable=True)
    registrations = Column(Integer, nullable=True)
    subscriptions = Column(Integer, nullable=True)
    unique_clicks = Column(Integer, nullable=True)
    unique_ctr = Column(Numeric(10, 4), nullable=True)

    created_at = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("campaign_id", "data_date", "country", name="uq_fb_insight"),
    )


# ---------------------------------------------------------------------------
# Keitaro reference tables
# ---------------------------------------------------------------------------
class KGroup(Base):
    __tablename__ = "k_groups"

    group_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    type = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KAffiliateNetwork(Base):
    __tablename__ = "k_affiliate_networks"

    network_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    postback_url = Column(Text, nullable=True)
    offer_param = Column(String(255), nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KTrafficSource(Base):
    __tablename__ = "k_traffic_sources"

    source_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    postback_url = Column(Text, nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KCampaign(Base):
    __tablename__ = "k_campaigns"

    campaign_id = Column(Integer, primary_key=True)
    alias = Column(String(255), nullable=True)
    name = Column(String(255), nullable=True)
    group_id = Column(Integer, ForeignKey("k_groups.group_id"), nullable=True)
    source_id = Column(Integer, ForeignKey("k_traffic_sources.source_id"), nullable=True)
    cost_type = Column(String(20), nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KOffer(Base):
    __tablename__ = "k_offers"

    offer_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    group_id = Column(Integer, ForeignKey("k_groups.group_id"), nullable=True)
    network_id = Column(
        Integer, ForeignKey("k_affiliate_networks.network_id"), nullable=True
    )
    country = Column(String(255), nullable=True)
    payout_type = Column(String(10), nullable=True)
    payout_value = Column(Numeric(10, 4), nullable=True)
    payout_currency = Column(String(10), nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KLanding(Base):
    __tablename__ = "k_landings"

    landing_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    group_id = Column(Integer, ForeignKey("k_groups.group_id"), nullable=True)
    type = Column(String(50), nullable=True)
    url = Column(Text, nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KStream(Base):
    __tablename__ = "k_streams"

    stream_id = Column(Integer, primary_key=True)
    campaign_id = Column(
        Integer, ForeignKey("k_campaigns.campaign_id"), nullable=True
    )
    name = Column(String(255), nullable=True)
    type = Column(String(50), nullable=True)
    schema_ = Column("schema", String(50), nullable=True)
    position = Column(Integer, nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KStreamOffer(Base):
    __tablename__ = "k_stream_offers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stream_id = Column(
        Integer, ForeignKey("k_streams.stream_id"), nullable=False, index=True
    )
    offer_id = Column(
        Integer, ForeignKey("k_offers.offer_id"), nullable=False, index=True
    )
    share = Column(Integer, nullable=True)
    state = Column(String(50), nullable=True)
    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("stream_id", "offer_id", name="uq_stream_offer"),
    )


# ---------------------------------------------------------------------------
# Keitaro log tables
# ---------------------------------------------------------------------------
class KClickLog(Base):
    __tablename__ = "k_clicks_log"

    click_id = Column(String(50), primary_key=True)
    campaign_id = Column(Integer, nullable=True)
    offer_id = Column(Integer, nullable=True)
    landing_id = Column(Integer, nullable=True)
    stream_id = Column(Integer, nullable=True)
    affiliate_network_id = Column(Integer, nullable=True)
    ts_id = Column(Integer, nullable=True)

    click_datetime = Column(TIMESTAMP, nullable=True, index=True)
    ad_campaign_id = Column(String(255), nullable=True, index=True)
    external_id = Column(String(255), nullable=True)
    creative_id = Column(String(255), nullable=True)
    sub_id = Column(String(255), nullable=True)
    sub_id_1 = Column(String(255), nullable=True)
    sub_id_2 = Column(String(255), nullable=True)
    sub_id_3 = Column(String(255), nullable=True)
    sub_id_4 = Column(String(255), nullable=True)
    sub_id_5 = Column(String(255), nullable=True)

    country = Column(String(255), nullable=True)
    country_code = Column(String(10), nullable=True, index=True)
    region = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True)
    os = Column(String(50), nullable=True)
    browser = Column(String(50), nullable=True)
    device_type = Column(String(50), nullable=True)
    device_model = Column(String(255), nullable=True)
    language = Column(String(50), nullable=True)
    connection_type = Column(String(50), nullable=True)
    operator = Column(String(255), nullable=True)
    isp = Column(String(255), nullable=True)
    ip = Column(String(255), nullable=True)
    referrer = Column(Text, nullable=True)
    domain = Column(String(255), nullable=True)
    destination = Column(Text, nullable=True)

    is_bot = Column(Boolean, nullable=True, default=False)
    is_unique_campaign = Column(Boolean, nullable=True, default=False)
    is_unique_stream = Column(Boolean, nullable=True, default=False)
    is_unique_global = Column(Boolean, nullable=True, default=False)
    is_lead = Column(Boolean, nullable=True, default=False)
    is_sale = Column(Boolean, nullable=True, default=False)
    is_rejected = Column(Boolean, nullable=True, default=False)

    cost = Column(Numeric(12, 4), nullable=True, default=Decimal("0"))
    revenue = Column(Numeric(12, 4), nullable=True, default=Decimal("0"))
    profit = Column(Numeric(12, 4), nullable=True, default=Decimal("0"))

    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class KConversionLog(Base):
    __tablename__ = "k_conversions_log"

    conversion_id = Column(String(50), primary_key=True)
    campaign_id = Column(Integer, nullable=True)
    offer_id = Column(Integer, nullable=True)
    landing_id = Column(Integer, nullable=True)
    stream_id = Column(Integer, nullable=True)
    affiliate_network_id = Column(Integer, nullable=True)
    ts_id = Column(Integer, nullable=True)

    status = Column(String(50), nullable=True)
    revenue = Column(Numeric(12, 4), nullable=True)
    conversion_type = Column(String(50), nullable=True)
    postback_datetime = Column(TIMESTAMP, nullable=True, index=True)
    click_datetime = Column(TIMESTAMP, nullable=True)

    ad_campaign_id = Column(String(255), nullable=True, index=True)
    external_id = Column(String(255), nullable=True)
    creative_id = Column(String(255), nullable=True)
    sub_id = Column(String(255), nullable=True)
    sub_id_1 = Column(String(255), nullable=True)
    sub_id_2 = Column(String(255), nullable=True)
    sub_id_3 = Column(String(255), nullable=True)
    sub_id_4 = Column(String(255), nullable=True)
    sub_id_5 = Column(String(255), nullable=True)

    country = Column(String(255), nullable=True)
    country_code = Column(String(10), nullable=True, index=True)
    region = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True)
    os = Column(String(50), nullable=True)
    browser = Column(String(50), nullable=True)
    device_type = Column(String(50), nullable=True)
    device_model = Column(String(255), nullable=True)
    language = Column(String(50), nullable=True)
    ip = Column(String(255), nullable=True)

    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


# ---------------------------------------------------------------------------
# Keitaro aggregated daily stats
# ---------------------------------------------------------------------------
class KDailyStat(Base):
    __tablename__ = "k_daily_stats"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    data_date = Column(Date, nullable=False, index=True)

    ad_campaign_id = Column(String(255), nullable=True, index=True)
    campaign_id = Column(Integer, nullable=True)
    offer_id = Column(Integer, nullable=True)
    stream_id = Column(Integer, nullable=True)
    affiliate_network_id = Column(Integer, nullable=True)
    ts_id = Column(Integer, nullable=True)
    country_code = Column(String(10), nullable=True)

    clicks = Column(Integer, nullable=False, server_default="0")
    unique_clicks = Column(Integer, nullable=False, server_default="0")
    leads = Column(Integer, nullable=False, server_default="0")
    sales = Column(Integer, nullable=False, server_default="0")
    rejected = Column(Integer, nullable=False, server_default="0")
    revenue = Column(Numeric(12, 4), nullable=False, server_default="0")
    cost = Column(Numeric(12, 4), nullable=False, server_default="0")
    profit = Column(Numeric(12, 4), nullable=False, server_default="0")

    updated_at = Column(
        TIMESTAMP, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint(
            "data_date",
            "ad_campaign_id",
            "campaign_id",
            "offer_id",
            "stream_id",
            "country_code",
            "affiliate_network_id",
            "ts_id",
            name="uq_k_daily_stats",
        ),
        Index(
            "ix_k_daily_stats_ad_campaign_date",
            "ad_campaign_id",
            "data_date",
        ),
    )
