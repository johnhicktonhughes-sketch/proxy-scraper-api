from datetime import datetime, timezone
from typing import List, Literal

import os

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import ScrapeTask
from app.schemas import (
    ScrapeTaskCreate,
    ScrapeTaskListResponse,
    ScrapeTaskOut,
    ScrapeTaskUpdate,
    EasyliveAuctionAnalytics,
    EasyliveAuctionAnalyticsResponse,
    ScrapeTaskStatusSummary,
)


api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="API key required to access endpoints.",
)

ENUMS = {
    "site": ["easylive", "the_saleroom"],
    "task_type": ["discover", "listing", "rescrape", "catalogue"],
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
    task_type: Literal["discover", "listing", "rescrape", "catalogue"] | None = Query(None),
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
def get_scrape_task_enums():
    return ENUMS


@app.get("/scrape_tasks/next_pending", response_model=ScrapeTaskListResponse)
def list_next_pending_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=500),
):
    now = datetime.now(timezone.utc)
    query = (
        db.query(ScrapeTask)
        .filter(ScrapeTask.status == "pending")
        .filter(ScrapeTask.scheduled_at.isnot(None))
        .filter(ScrapeTask.scheduled_at <= now)
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
                tr.stats
            FROM scrape_tasks st
            JOIN task_runs tr ON tr.task_id = st.id
            WHERE st.task_type = 'catalogue'
              AND st.site = 'easylive'
              AND tr.url LIKE '%/catalogue/%'
        )
        SELECT
            url_no_query,
            split_part(split_part(url_no_query, 'catalogue/', 2), '/', 1) AS catalogue_id,
            split_part(split_part(url_no_query, 'catalogue/', 2), '/', 2) AS auction_id,
            NULLIF(split_part(split_part(url_no_query, 'catalogue/', 2), '/', 3), '') AS slug,
            COUNT(*) AS run_count,
            SUM((stats->>'lots_found')::int) AS lots_scraped
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
