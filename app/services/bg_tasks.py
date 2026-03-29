"""Background task helpers with concurrency control.

Architecture:
- FB import → triggers kt_sync_for_date_range() in background (FastAPI BackgroundTasks)
- KT sync uses a threading.Lock so only ONE sync runs at a time
- If a sync is already running, new requests are queued (blocking=True) — they wait
  their turn, so every import's date range eventually gets synced
- Import semaphore limits concurrent XLSX/CSV file processing to avoid memory spikes
"""

import logging
import threading
from typing import Optional

from app.core.config import settings
from app.db.database import SessionLocal

logger = logging.getLogger(__name__)

# ── Concurrency controls ────────────────────────────────────────────────────

# One KT sync at a time — prevents two rebuild_daily_stats from racing on same dates
_kt_sync_lock = threading.Lock()

# Max concurrent FB file imports (each loads XLSX into memory — limit to avoid OOM)
_import_semaphore = threading.Semaphore(3)


# ── Background tasks ────────────────────────────────────────────────────────

def kt_sync_for_date_range(date_from: str, date_to: str) -> None:
    """Sync KT clicks + conversions + rebuild k_daily_stats for a date range.

    Called automatically as a background task after each FB file import.
    Queues if another sync is running (blocking acquire) so no imports are missed.
    Silently skips if Keitaro is not configured.
    """
    if not settings.keitaro_api_url or not settings.keitaro_api_key:
        logger.debug("Keitaro not configured — skipping background sync")
        return

    logger.info("BG KT sync queued: %s → %s", date_from, date_to)
    _kt_sync_lock.acquire()  # wait for any running sync to finish

    db = SessionLocal()
    try:
        from app.services.keitaro_api_service import KeitaroAPIService
        from app.services.keitaro_sync_service import (
            rebuild_daily_stats,
            sync_clicks_log_chunked,
            sync_conversions_log,
        )

        api = KeitaroAPIService()
        logger.info("BG KT sync start: %s → %s", date_from, date_to)

        clicks = sync_clicks_log_chunked(db, api, date_from, date_to)
        convs = sync_conversions_log(db, api, date_from, date_to)
        stats = rebuild_daily_stats(db, date_from, date_to)

        logger.info(
            "BG KT sync done: clicks=%s convs=%s stats_rows=%s",
            clicks.details.get("clicks_synced", 0),
            convs.details.get("conversions_synced", 0),
            stats.details.get("stats_rows", 0),
        )
    except Exception as e:
        logger.error("BG KT sync failed (%s→%s): %s", date_from, date_to, e, exc_info=True)
    finally:
        db.close()
        _kt_sync_lock.release()


def acquire_import_slot() -> bool:
    """Try to acquire an import slot (non-blocking).
    Returns True if slot acquired, False if all slots busy (429 response).
    """
    return _import_semaphore.acquire(blocking=False)


def release_import_slot() -> None:
    _import_semaphore.release()
