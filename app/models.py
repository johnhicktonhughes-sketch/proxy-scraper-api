from sqlalchemy import BigInteger, Column, DateTime, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class ScrapeTask(Base):
    __tablename__ = "scrape_tasks"

    id = Column(BigInteger, primary_key=True, index=True)
    site = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    task_type = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    attempts = Column(Integer, nullable=False, server_default="0")
    max_attempts = Column(Integer, nullable=False, server_default="5")
    last_error = Column(Text, nullable=True)
    meta = Column(JSONB, nullable=False, server_default="{}")
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

