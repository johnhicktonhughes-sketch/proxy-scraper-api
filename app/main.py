from datetime import datetime, timezone
import logging
from typing import List, Literal

import os

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import bindparam, func, or_, text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import ScrapeTask
from app.schemas import (
    ScrapeTaskCreate,
    ScrapeTaskListResponse,
    ScrapeTaskOut,
    ScrapeTaskRecentItem,
    ScrapeTaskRecentResponse,
    ScrapeTaskRelatedByUrlItem,
    ScrapeTaskRelatedByUrlResponse,
    ScrapeTaskUpdate,
    FailedScrapeTask,
    FailedScrapeTaskListResponse,
    EasyliveAuctionAnalytics,
    EasyliveAuctionAnalyticsResponse,
    ScrapeTaskStatusSummary,
    ScrapeTaskUrlSummaryItem,
    ScrapeTaskUrlSummaryResponse,
    ListingSnapshotByUrlPatternItem,
    ListingSnapshotByUrlPatternResponse,
    ListingSnapshotResponse,
    AuctioneerLotsResponse,
    AuctioneerLotsSummary,
    AuctioneerPriceSummary,
    AuctioneerPriceSummaryResponse,
    ListingResponse,
    AuctioneerNameListResponse,
)


logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="API key required to access endpoints.",
)

ENUMS = {
    "site": ["easylive", "the_saleroom"],
    "task_type": ["discover", "listing", "rescrape", "catalogue", "auction_times"],
    "status": ["pending", "running", "done", "failed"],
}


def require_api_key(api_key: str | None = Depends(api_key_header)) -> None:
    expected = os.getenv("API_KEY")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_KEY is not configured",
        )
    if api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
        )


app = FastAPI(title="Scrape Tasks API", dependencies=[Depends(require_api_key)])
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/scrape_tasks", response_model=ScrapeTaskListResponse)
def list_scrape_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    task_type: Literal[
        "discover", "listing", "rescrape", "catalogue", "auction_times"
    ]
    | None = Query(None),
    status: Literal["pending", "running", "done", "failed"] | None = Query(None),
    site: Literal["easylive", "the_saleroom"] | None = Query(None),
    scheduled_at: datetime | None = Query(None),
    scheduled_at_from: datetime | None = Query(None),
    scheduled_at_to: datetime | None = Query(None),
    created_at: datetime | None = Query(None),
    created_at_from: datetime | None = Query(None),
    created_at_to: datetime | None = Query(None),
):
    query = db.query(ScrapeTask)
    if task_type is not None:
        query = query.filter(ScrapeTask.task_type == task_type)
    if status is not None:
        query = query.filter(ScrapeTask.status == status)
    if site is not None:
        query = query.filter(ScrapeTask.site == site)
    if scheduled_at is not None:
        query = query.filter(ScrapeTask.scheduled_at == scheduled_at)
    if scheduled_at_from is not None:
        query = query.filter(ScrapeTask.scheduled_at >= scheduled_at_from)
    if scheduled_at_to is not None:
        query = query.filter(ScrapeTask.scheduled_at <= scheduled_at_to)
    if created_at is not None:
        query = query.filter(ScrapeTask.created_at == created_at)
    if created_at_from is not None:
        query = query.filter(ScrapeTask.created_at >= created_at_from)
    if created_at_to is not None:
        query = query.filter(ScrapeTask.created_at <= created_at_to)

    total = query.count()
    items = query.offset(offset).limit(limit).all()
    return {"total": total, "items": items}


@app.get("/scrape_tasks/enums")
def get_scrape_task_enums(db: Session = Depends(get_db)):
    auction_times = [
        row[0]
        for row in db.query(ScrapeTask.meta["auction_time"].astext)
        .filter(ScrapeTask.meta["auction_time"].astext.isnot(None))
        .filter(ScrapeTask.meta["auction_time"].astext != "")
        .distinct()
        .order_by(ScrapeTask.meta["auction_time"].astext.asc())
        .all()
    ]
    return {**ENUMS, "auction_times": auction_times}


@app.get("/scrape_tasks/next_pending", response_model=ScrapeTaskListResponse)
def list_next_pending_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=500),
):
    now = datetime.now(timezone.utc)
    query = (
        db.query(ScrapeTask)
        .filter(ScrapeTask.status == "pending")
        .filter(or_(ScrapeTask.scheduled_at.is_(None), ScrapeTask.scheduled_at <= now))
        .order_by(ScrapeTask.scheduled_at.asc().nulls_last())
    )
    total = query.count()
    items = query.limit(limit).all()
    return {"total": total, "items": items}


@app.get("/scrape_tasks/{task_id}", response_model=ScrapeTaskOut)
def get_scrape_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(ScrapeTask).filter(ScrapeTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return task


@app.post("/scrape_tasks", response_model=ScrapeTaskOut, status_code=201)
def create_scrape_task(payload: ScrapeTaskCreate, db: Session = Depends(get_db)):
    data = payload.model_dump(exclude_unset=True)
    if data.get("status") is None:
        data["status"] = "pending"

    existing_pending_query = (
        db.query(ScrapeTask)
        .filter(ScrapeTask.site == data["site"])
        .filter(ScrapeTask.url == data["url"])
        .filter(ScrapeTask.task_type == data["task_type"])
        .filter(ScrapeTask.status == "pending")
    )
    scheduled_at = data.get("scheduled_at")
    if scheduled_at is None:
        existing_pending_query = existing_pending_query.filter(
            ScrapeTask.scheduled_at.is_(None)
        )
    else:
        existing_pending_query = existing_pending_query.filter(
            ScrapeTask.scheduled_at == scheduled_at
        )

    existing_pending = existing_pending_query.first()
    if existing_pending:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": (
                    "Scrape task is already pending for this site/url/task_type "
                    f"(id={existing_pending.id})"
                ),
                "id": existing_pending.id,
            },
        )

    task = ScrapeTask(**data)
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


@app.patch("/scrape_tasks/{task_id}", response_model=ScrapeTaskOut)
def update_scrape_task(
    task_id: int, payload: ScrapeTaskUpdate, db: Session = Depends(get_db)
):
    task = db.query(ScrapeTask).filter(ScrapeTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")

    data = payload.model_dump(exclude_unset=True)
    if data.get("meta") is None and "meta" in data:
        data.pop("meta")

    for key, value in data.items():
        setattr(task, key, value)

    task.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(task)
    return task


@app.delete("/scrape_tasks/{task_id}", status_code=204)
def delete_scrape_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(ScrapeTask).filter(ScrapeTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if task.status not in {"pending", "failed"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only pending or failed tasks can be deleted",
        )
    db.delete(task)
    db.commit()


@app.delete("/scrape_tasks/related/by_url")
def delete_scrape_task_related_records(
    url: str = Query(...),
    dry_run: bool = Query(False),
    db: Session = Depends(get_db),
):
    task_ids = [
        row[0]
        for row in db.execute(
            text(
                """
                SELECT id
                FROM scrape_tasks
                WHERE url LIKE :url_pattern AND task_type = 'catalogue'
                """
            ),
            {"url_pattern": f"{url}%"},
        ).all()
    ]
    if not task_ids:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if dry_run:
        logger.info(
            "Dry run and delete /scrape_tasks/related url=%s task_ids=%s", url, task_ids
        )
        return {"task_ids": task_ids, "dry_run": True}

    task_run_ids = [
        row[0]
        for row in db.execute(
            text("SELECT id FROM task_runs WHERE task_id IN :task_ids").bindparams(
                bindparam("task_ids", expanding=True)
            ),
            {"task_ids": task_ids},
        ).all()
    ]
    listing_ids = [
        row[0]
        for row in db.execute(
            text(
                """
                SELECT DISTINCT ltr.listing_id
                FROM listing_task_runs ltr
                JOIN task_runs tr ON tr.id = ltr.task_run_id
                WHERE tr.task_id IN :task_ids
                """
            ).bindparams(bindparam("task_ids", expanding=True)),
            {"task_ids": task_ids},
        ).all()
    ]

    if task_run_ids:
        db.execute(
            text(
                "DELETE FROM listing_task_runs WHERE task_run_id IN :task_run_ids"
            ).bindparams(bindparam("task_run_ids", expanding=True)),
            {"task_run_ids": task_run_ids},
        )
    if listing_ids:
        db.execute(
            text(
                "DELETE FROM listing_snapshots WHERE listing_id IN :listing_ids"
            ).bindparams(bindparam("listing_ids", expanding=True)),
            {"listing_ids": listing_ids},
        )
        db.execute(
            text("DELETE FROM listings WHERE id IN :listing_ids").bindparams(
                bindparam("listing_ids", expanding=True)
            ),
            {"listing_ids": listing_ids},
        )

    if task_run_ids:
        db.execute(
            text("DELETE FROM task_runs WHERE id IN :task_run_ids").bindparams(
                bindparam("task_run_ids", expanding=True)
            ),
            {"task_run_ids": task_run_ids},
        )
    db.commit()
    return {"task_ids": task_ids, "dry_run": False}


@app.get(
    "/scrape_tasks/related/by_url",
    response_model=ScrapeTaskRelatedByUrlResponse,
)
def list_scrape_tasks_related_by_url(
    url: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    url_pattern = f"{url}%"
    total = db.execute(
        text(
            """
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM scrape_tasks st
                WHERE st.url LIKE :url_pattern
                GROUP BY
                    st.url,
                    st.task_type,
                    st.status,
                    st.meta->>'source'
            ) grouped
            """
        ),
        {"url_pattern": url_pattern},
    ).scalar()
    items_query = text(
        """
        SELECT
            MAX(st.id) AS id,
            st.url,
            st.task_type,
            st.status,
            st.meta->>'source' AS source,
            MIN(st.created_at) AS created_at,
            MAX(st.updated_at) AS updated_at,
            MAX(st.scheduled_at) AS scheduled_at,
            COUNT(DISTINCT l.id) AS listings
        FROM scrape_tasks st
        LEFT JOIN task_runs tr ON tr.task_id = st.id
        LEFT JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
        LEFT JOIN listings l ON l.id = ltr.listing_id
        LEFT JOIN listing_snapshots ls ON ls.listing_id = l.id
        WHERE st.url LIKE :url_pattern
        GROUP BY
            st.url,
            st.task_type,
            st.status,
            st.meta->>'source'
        ORDER BY MAX(st.updated_at) DESC
        OFFSET :offset
        LIMIT :limit
        """
    )
    rows = db.execute(
        items_query,
        {"url_pattern": url_pattern, "offset": offset, "limit": limit},
    ).mappings().all()
    items = [ScrapeTaskRelatedByUrlItem(**row) for row in rows]
    return {"total": total or 0, "items": items}


@app.get(
    "/scrape_tasks/summary/by_url", response_model=ScrapeTaskUrlSummaryResponse
)
def list_scrape_tasks_summary_by_url(
    url: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    url_pattern = f"%{url}%"
    status_rows = db.execute(
        text(
            """
            SELECT status, COUNT(*) AS total
            FROM scrape_tasks
            WHERE url LIKE :url_pattern
            GROUP BY status
            """
        ),
        {"url_pattern": url_pattern},
    ).all()
    status_counts = {row[0]: row[1] for row in status_rows}
    done_total = status_counts.get("done", 0)
    todo_total = sum(status_counts.values()) - done_total

    items_query = text(
        """
        WITH task_listing_counts AS (
            SELECT
                st.id AS task_id,
                COUNT(DISTINCT l.id) AS listing_count,
                COUNT(ls.id) AS snapshot_count
            FROM scrape_tasks st
            LEFT JOIN task_runs tr ON tr.task_id = st.id
            LEFT JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
            LEFT JOIN listings l ON l.id = ltr.listing_id
            LEFT JOIN listing_snapshots ls ON ls.listing_id = l.id
            WHERE st.url LIKE :url_pattern
            GROUP BY st.id
        )
        SELECT
            st.*,
            COALESCE(tlc.listing_count, 0) AS listing_count,
            COALESCE(tlc.snapshot_count, 0) AS snapshot_count
        FROM scrape_tasks st
        LEFT JOIN task_listing_counts tlc ON tlc.task_id = st.id
        WHERE st.url LIKE :url_pattern
        ORDER BY st.created_at DESC
        """
    )
    rows = db.execute(items_query, {"url_pattern": url_pattern}).mappings().all()
    items = [ScrapeTaskUrlSummaryItem(**row) for row in rows]
    done_items = [item for item in items if item.status == "done"]
    todo_items = [item for item in items if item.status != "done"]
    return {
        "done_total": done_total,
        "todo_total": todo_total,
        "done_items": done_items,
        "todo_items": todo_items,
    }


@app.get(
    "/scrape_tasks/listing_snapshots/by_url_pattern",
    response_model=ListingSnapshotByUrlPatternResponse,
)
def list_listing_snapshots_by_url_pattern(
    url_pattern: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    if "%" not in url_pattern:
        url_pattern = f"%{url_pattern}%"
    total = db.execute(
        text(
            """
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM scrape_tasks st
                LEFT JOIN task_runs tr ON tr.task_id = st.id
                LEFT JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
                LEFT JOIN listings l ON l.id = ltr.listing_id
                LEFT JOIN listing_snapshots ls ON ls.listing_id = l.id
                WHERE st.url LIKE :url_pattern
                GROUP BY ls.listing_id, l.url, st.url, l.title
            ) grouped
            """
        ),
        {"url_pattern": url_pattern},
    ).scalar()
    items_query = text(
        """
        SELECT
            ls.listing_id,
            st.url AS scrape_url,
            l.url AS lot_url,
            l.title,
            COUNT(DISTINCT ls.id) AS snapshots,
            SUM(
                CASE WHEN ls.snapshot_type = 'pre_auction' THEN 1 ELSE 0 END
            ) AS pre_auction_snapshots,
            SUM(
                CASE WHEN ls.snapshot_type = 'post_auction' THEN 1 ELSE 0 END
            ) AS post_auction_snapshots,
            MAX(ls.data->>'auction_start') AS auction_start,
            MAX(ls.data->>'auction_end') AS auction_end,
            MAX(ls.data->>'estimate_low') AS est_lo,
            MAX(ls.data->>'estimate_high') AS est_hi,
            MAX(ls.data->>'sold_price') AS sold
        FROM scrape_tasks st
        LEFT JOIN task_runs tr ON tr.task_id = st.id
        LEFT JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
        LEFT JOIN listings l ON l.id = ltr.listing_id
        LEFT JOIN listing_snapshots ls ON ls.listing_id = l.id
        WHERE st.url LIKE :url_pattern
        GROUP BY ls.listing_id, l.url, st.url, l.title
        ORDER BY ls.listing_id NULLS LAST
        OFFSET :offset
        LIMIT :limit
        """
    )
    rows = db.execute(
        items_query,
        {"url_pattern": url_pattern, "offset": offset, "limit": limit},
    ).mappings().all()
    items = [ListingSnapshotByUrlPatternItem(**row) for row in rows]
    return {"total": total or 0, "items": items}


@app.get(
    "/analytics/easylive/auctions", response_model=EasyliveAuctionAnalyticsResponse
)
def list_easylive_auction_analytics(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
):
    status_counts = dict(
        db.query(ScrapeTask.status, func.count(ScrapeTask.id))
        .group_by(ScrapeTask.status)
        .all()
    )
    summary = ScrapeTaskStatusSummary(
        total=sum(status_counts.values()),
        pending=status_counts.get("pending", 0),
        running=status_counts.get("running", 0),
        done=status_counts.get("done", 0),
        failed=status_counts.get("failed", 0),
    )
    query = text(
        """
        WITH base AS (
            SELECT
                split_part(tr.url, '?', 1) AS url_no_query,
                tr.auctioneer_name,
                tr.stats
            FROM scrape_tasks st
            JOIN task_runs tr ON tr.task_id = st.id
            WHERE st.task_type = 'catalogue'
              AND st.site = 'easylive'
              AND tr.url LIKE '%/catalogue/%'
        )
        SELECT
            auctioneer_name,
            split_part(split_part(url_no_query, 'catalogue/', 2), '/', 1) AS catalogue_id,
            split_part(split_part(url_no_query, 'catalogue/', 2), '/', 2) AS auction_id,
            NULLIF(split_part(split_part(url_no_query, 'catalogue/', 2), '/', 3), '') AS slug,
            COUNT(*) AS run_count,
            SUM((stats->>'lots_found')::int) AS lots_scraped,
            SUM((stats->>'hammer_prices_found')::int) AS hammer_prices_found
        FROM base
        GROUP BY 1, 2, 3, 4
        ORDER BY lots_scraped DESC NULLS LAST
        LIMIT :limit
        """
    )
    rows = db.execute(query, {"limit": limit}).mappings().all()
    return {
        "summary": summary,
        "items": [EasyliveAuctionAnalytics(**row) for row in rows],
    }


@app.get("/analytics/scrape_tasks/pending_future", response_model=ScrapeTaskListResponse)
def list_pending_future_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=500),
):
    now = datetime.now(timezone.utc)
    query = (
        db.query(ScrapeTask)
        .filter(ScrapeTask.status == "pending")
        .filter(ScrapeTask.scheduled_at.isnot(None))
        .filter(ScrapeTask.scheduled_at > now)
        .order_by(ScrapeTask.scheduled_at.asc().nulls_last())
    )
    total = query.count()
    items = query.limit(limit).all()
    return {"total": total, "items": items}


@app.get(
    "/analytics/scrape_tasks/recent", response_model=ScrapeTaskRecentResponse
)
def list_recent_scrape_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
):
    query = text(
        """
        WITH listing_counts AS (
            SELECT
                st.id AS task_id,
                COUNT(DISTINCT l.id) AS listing_count
            FROM scrape_tasks st
            LEFT JOIN task_runs tr ON tr.task_id = st.id
            LEFT JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
            LEFT JOIN listings l ON l.id = ltr.listing_id
            GROUP BY st.id
        )
        SELECT
            st.*,
            COALESCE(lc.listing_count, 0) AS listing_count
        FROM scrape_tasks st
        LEFT JOIN listing_counts lc ON lc.task_id = st.id
        WHERE st.status = 'done'
        ORDER BY st.updated_at DESC NULLS LAST, st.created_at DESC
        LIMIT :limit
        """
    )
    rows = db.execute(query, {"limit": limit}).mappings().all()
    items = [ScrapeTaskRecentItem(**row) for row in rows]
    return {"total": len(items), "items": items}


@app.get("/analytics/scrape_tasks/running", response_model=ScrapeTaskListResponse)
def list_running_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    task_type: Literal[
        "discover", "listing", "rescrape", "catalogue", "auction_times"
    ]
    | None = Query(None),
    site: Literal["easylive", "the_saleroom"] | None = Query(None),
):
    query = db.query(ScrapeTask).filter(ScrapeTask.status == "running")
    if task_type is not None:
        query = query.filter(ScrapeTask.task_type == task_type)
    if site is not None:
        query = query.filter(ScrapeTask.site == site)
    query = query.order_by(
        ScrapeTask.updated_at.desc().nulls_last(),
        ScrapeTask.created_at.desc(),
    )
    total = query.count()
    items = query.offset(offset).limit(limit).all()
    return {"total": total, "items": items}


@app.get(
    "/analytics/scrape_tasks/failed", response_model=FailedScrapeTaskListResponse
)
def list_failed_scrape_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    task_type: Literal[
        "discover", "listing", "rescrape", "catalogue", "auction_times"
    ]
    | None = Query(None),
    site: Literal["easylive", "the_saleroom"] | None = Query(None),
):
    query = db.query(ScrapeTask).filter(ScrapeTask.status == "failed")
    if task_type is not None:
        query = query.filter(ScrapeTask.task_type == task_type)
    if site is not None:
        query = query.filter(ScrapeTask.site == site)
    query = query.order_by(
        ScrapeTask.updated_at.desc().nulls_last(),
        ScrapeTask.created_at.desc(),
    )
    total = query.count()
    items = query.offset(offset).limit(limit).all()
    return {
        "total": total,
        "items": [
            FailedScrapeTask(
                id=task.id,
                site=task.site,
                url=task.url,
                task_type=task.task_type,
                status=task.status,
                scheduled_at=task.scheduled_at,
                locked_at=task.locked_at,
                attempts=task.attempts,
                max_attempts=task.max_attempts,
                failure_reason=task.last_error,
                meta=task.meta,
                created_at=task.created_at,
                updated_at=task.updated_at,
            )
            for task in items
        ],
    }


@app.get(
    "/analytics/auctioneers/prices",
    response_model=AuctioneerPriceSummaryResponse,
)
def list_auctioneer_price_summary(db: Session = Depends(get_db)):
    query = text(
        """
        WITH prices AS (
            SELECT DISTINCT
                l.*,
                tr.auctioneer_name,
                NULLIF(ls.data->>'estimate_low', '')::numeric AS est_lo,
                NULLIF(ls.data->>'estimate_high', '')::numeric AS est_hi,
                ls.data->'auction_end' AS ended,
                NULLIF(ls.data->>'sold_price', '')::numeric AS sold,
                ls.data
            FROM task_runs tr
            JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
            JOIN listings l ON l.id = ltr.listing_id
            JOIN listing_snapshots ls ON ls.listing_id = l.id
            ORDER BY l.id DESC
        )
        SELECT
            auctioneer_name,
            COUNT(DISTINCT id) AS lots_analysed,
            ROUND(AVG(est_lo), 2) AS est_lo,
            ROUND(AVG(est_hi), 2) AS est_hi,
            ROUND(AVG(sold), 2) AS sold
        FROM prices
        GROUP BY auctioneer_name
        """
    )
    rows = db.execute(query).mappings().all()
    items = [AuctioneerPriceSummary(**row) for row in rows]
    return {"total": len(items), "items": items}


@app.get(
    "/listing_snapshots/by_catalogue", response_model=ListingSnapshotResponse
)
def list_listing_snapshots_by_catalogue(
    catalogue_url: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    base_sql = """
        FROM task_runs tr
        JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
        JOIN listings l ON l.id = ltr.listing_id
        JOIN listing_snapshots ls ON ls.listing_id = l.id
        LEFT JOIN scrape_tasks st ON st.id = tr.task_id
        WHERE tr.url LIKE :catalogue_url_pattern OR st.url = :catalogue_url
    """
    count_query = text(f"SELECT COUNT(*) {base_sql}")
    distinct_listings_query = text(
        f"SELECT COUNT(DISTINCT l.id) {base_sql}"
    )
    total = (
        db.execute(
            count_query,
            {
                "catalogue_url": catalogue_url,
                "catalogue_url_pattern": f"{catalogue_url}%",
            },
        ).scalar()
        or 0
    )
    total_listings = (
        db.execute(
            distinct_listings_query,
            {
                "catalogue_url": catalogue_url,
                "catalogue_url_pattern": f"{catalogue_url}%",
            },
        ).scalar()
        or 0
    )

    items_query = text(
        f"""
        SELECT
            ls.*,
            l.id AS listing_id,
            ltr.task_run_id AS task_run_id,
            tr.url AS task_run_url,
            st.id AS scrape_task_id,
            st.url AS scrape_task_url
        {base_sql}
        ORDER BY ls.id DESC
        LIMIT :limit OFFSET :offset
        """
    )
    rows = db.execute(
        items_query,
        {
            "catalogue_url": catalogue_url,
            "catalogue_url_pattern": f"{catalogue_url}%",
            "limit": limit,
            "offset": offset,
        },
    ).mappings().all()
    next_offset = None
    if offset + len(rows) < total:
        next_offset = offset + len(rows)
    return {
        "total": total,
        "total_listings": total_listings,
        "next_offset": next_offset,
        "items": [dict(row) for row in rows],
    }


@app.get(
    "/analytics/easylive/auctioneer_lots", response_model=AuctioneerLotsResponse
)
def list_easylive_auctioneer_lots(db: Session = Depends(get_db)):
    base_sql = """
        FROM task_runs tr
        JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
        JOIN listings l ON l.id = ltr.listing_id
        JOIN listing_snapshots ls ON ls.listing_id = l.id
        WHERE tr.auctioneer_name IS NOT NULL
    """
    items_query = text(
        f"""
        SELECT
            tr.auctioneer_name,
            COUNT(DISTINCT l.id) AS distinct_lots,
            MAX(ls.created_at) AS latest_snapshot_created_at
        {base_sql}
        GROUP BY tr.auctioneer_name
        ORDER BY distinct_lots DESC
        """
    )
    total_query = text(f"SELECT COUNT(DISTINCT l.id) {base_sql}")
    rows = db.execute(items_query).mappings().all()
    total_lots = db.execute(total_query).scalar() or 0
    return {
        "total_lots": total_lots,
        "items": [AuctioneerLotsSummary(**row) for row in rows],
    }


@app.get("/listings/by_auctioneer", response_model=ListingResponse)
def list_listings_by_auctioneer(
    auctioneer_name: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    base_sql = """
        FROM task_runs tr
        JOIN listing_task_runs ltr ON ltr.task_run_id = tr.id
        JOIN listings l ON l.id = ltr.listing_id
        JOIN listing_snapshots ls ON ls.listing_id = l.id
        WHERE tr.auctioneer_name = :auctioneer
    """
    distinct_count_query = text(f"SELECT COUNT(DISTINCT l.id) {base_sql}")
    total_listings = (
        db.execute(distinct_count_query, {"auctioneer": auctioneer_name}).scalar() or 0
    )
    snapshot_count_query = text(f"SELECT COUNT(*) {base_sql}")
    total_snapshots = (
        db.execute(snapshot_count_query, {"auctioneer": auctioneer_name}).scalar() or 0
    )
    averages_query = text(
        f"""
        SELECT
            AVG(NULLIF(ls.data->>'estimate_low', '')::numeric) AS avg_estimate_low,
            AVG(NULLIF(ls.data->>'estimate_high', '')::numeric) AS avg_estimate_high,
            AVG(NULLIF(ls.data->>'sold_price', '')::numeric) AS avg_sold_price
        {base_sql}
        """
    )
    averages_row = (
        db.execute(averages_query, {"auctioneer": auctioneer_name}).mappings().first()
    ) or {}
    items_query = text(
        f"""
        SELECT DISTINCT l.*
            , ls.data->>'estimate_low' as est_lo
            , ls.data->>'estimate_high' as est_hi
            , ls.data->'auction_end' as ended
            , ls.data->'sold_price' as sold
            , ls.data
        {base_sql}
        ORDER BY l.id DESC
        LIMIT :limit OFFSET :offset
        """
    )
    rows = db.execute(
        items_query,
        {"auctioneer": auctioneer_name, "limit": limit, "offset": offset},
    ).mappings().all()
    next_offset = None
    if offset + len(rows) < total_listings:
        next_offset = offset + len(rows)
    return {
        "total": total_listings,
        "total_snapshots": total_snapshots,
        "avg_estimate_low": averages_row.get("avg_estimate_low"),
        "avg_estimate_high": averages_row.get("avg_estimate_high"),
        "avg_sold_price": averages_row.get("avg_sold_price"),
        "next_offset": next_offset,
        "items": [dict(row) for row in rows],
    }


@app.get("/listings/auctioneers", response_model=AuctioneerNameListResponse)
def list_listing_auctioneers(
    db: Session = Depends(get_db),
    limit: int = Query(1000, ge=1, le=5000),
):
    query = text(
        """
        SELECT DISTINCT tr.auctioneer_name
        FROM task_runs tr
        WHERE tr.auctioneer_name IS NOT NULL
          AND trim(tr.auctioneer_name) <> ''
        ORDER BY tr.auctioneer_name ASC
        LIMIT :limit
        """
    )
    rows = db.execute(query, {"limit": limit}).all()
    names = [row[0] for row in rows]
    return {"total": len(names), "items": names}
