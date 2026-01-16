from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class ScrapeTaskBase(BaseModel):
    site: Literal["easylive", "the_saleroom"]
    url: str
    task_type: Literal["discover", "listing", "rescrape", "catalogue"]
    status: Optional[Literal["pending", "running", "done", "failed"]] = None
    scheduled_at: Optional[datetime] = None
    locked_at: Optional[datetime] = None
    attempts: Optional[int] = None
    max_attempts: Optional[int] = None
    last_error: Optional[str] = None
    meta: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ScrapeTaskCreate(ScrapeTaskBase):
    status: Optional[Literal["pending", "running", "done", "failed"]] = "pending"


class ScrapeTaskUpdate(BaseModel):
    site: Optional[Literal["easylive", "the_saleroom"]] = None
    url: Optional[str] = None
    task_type: Optional[Literal["discover", "listing", "rescrape", "catalogue"]] = None
    status: Optional[Literal["pending", "running", "done", "failed"]] = None
    scheduled_at: Optional[datetime] = None
    locked_at: Optional[datetime] = None
    attempts: Optional[int] = None
    max_attempts: Optional[int] = None
    last_error: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class ScrapeTaskOut(ScrapeTaskBase):
    id: int
    status: str
    attempts: int
    max_attempts: int
    meta: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class ScrapeTaskListResponse(BaseModel):
    total: int
    items: list[ScrapeTaskOut]
