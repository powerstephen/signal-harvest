from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Float
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utcnow():
    return datetime.now(timezone.utc)


class ProspectSession(Base):
    __tablename__ = "prospect_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_name = Column(String(255), default="")
    mode = Column(String(20), default="domain")
    input_data = Column(Text, default="")
    country = Column(String(100), default="")
    notes = Column(Text, default="")
    status = Column(String(20), default="pending")
    log = Column(Text, default="")
    lead_count = Column(Integer, default=0)
    started_at = Column(DateTime, default=utcnow)
    finished_at = Column(DateTime, nullable=True)

    leads = relationship("Prospect", back_populates="session", cascade="all, delete-orphan")


class Prospect(Base):
    __tablename__ = "prospects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("prospect_sessions.id"), nullable=False, index=True)

    company = Column(String(255), default="")
    website = Column(String(500), default="")
    industry = Column(String(100), default="")
    employee_count = Column(String(50), default="")
    description = Column(Text, default="")
    country = Column(String(100), default="")

    first_name = Column(String(100), default="")
    last_name = Column(String(100), default="")
    email = Column(String(255), default="")
    phone = Column(String(64), default="")
    job_title = Column(String(255), default="")
    linkedin_url = Column(String(500), default="")

    signal = Column(String(255), default="")
    relevance_score = Column(Float, default=0.0)
    relevance_reason = Column(String(500), default="")
    source_url = Column(String(500), default="")

    found_at = Column(DateTime, default=utcnow)

    session = relationship("ProspectSession", back_populates="leads")
