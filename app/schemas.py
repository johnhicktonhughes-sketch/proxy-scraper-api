from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


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

    model_config = ConfigDict(from_attributes=True)


class ScrapeTaskListResponse(BaseModel):
    total: int
    items: list[ScrapeTaskOut]


class EasyliveAuctionAnalytics(BaseModel):
    auctioneer_name: str | None = None
    catalogue_id: str
    auction_id: str
    slug: str | None = None
    run_count: int
    lots_scraped: int | None = None
    hammer_prices_found: int | None = None


class ScrapeTaskStatusSummary(BaseModel):
    total: int
    pending: int
    running: int
    done: int
    failed: int


class EasyliveAuctionAnalyticsResponse(BaseModel):
    summary: ScrapeTaskStatusSummary
    items: list[EasyliveAuctionAnalytics]


class ListingSnapshotResponse(BaseModel):
    total: int
    items: list[Dict[str, Any]]


class AuctioneerLotsSummary(BaseModel):
    auctioneer_name: str | None = None
    distinct_lots: int
    latest_snapshot_created_at: datetime | None = None


class AuctioneerLotsResponse(BaseModel):
    total_lots: int
    items: list[AuctioneerLotsSummary]


class ListingResponse(BaseModel):
    total: int
    items: list[Dict[str, Any]]


class AuctioneerNameListResponse(BaseModel):
    total: int
    items: list[str]
