from datetime import datetime, timezone
from typing import List, Literal

import os

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import ScrapeTask
from app.schemas import (
    ScrapeTaskCreate,
    ScrapeTaskListResponse,
    ScrapeTaskOut,
    ScrapeTaskUpdate,
)


ENUMS = {
    "site": ["easylive", "the_saleroom"],
    "task_type": ["discover", "listing", "rescrape", "catalogue"],
    "status": ["pending", "running", "done", "failed"],
}


def require_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    expected = os.getenv("API_KEY")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_KEY is not configured",
        )
    if x_api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
        )


app = FastAPI(title="Scrape Tasks API", dependencies=[Depends(require_api_key)])


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/scrape_tasks", response_model=ScrapeTaskListResponse)
def list_scrape_tasks(
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=500),
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


@app.get("/scrape_tasks/{task_id}", response_model=ScrapeTaskOut)
def get_scrape_task(task_id: int, db: Session = Depends(get_db)):
    task = db.query(ScrapeTask).filter(ScrapeTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return task


@app.post("/scrape_tasks", response_model=ScrapeTaskOut, status_code=201)
def create_scrape_task(payload: ScrapeTaskCreate, db: Session = Depends(get_db)):
    data = payload.model_dump(exclude_unset=True)
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
