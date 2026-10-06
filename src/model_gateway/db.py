"""短事务保存授权及元数据，不在模型调用期间持有数据库连接。"""

import hashlib
import time

from sqlalchemy import (
    JSON,
    Boolean,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    create_engine,
    delete,
    func,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class Application(Base):
    __tablename__ = "applications"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    model_allowlist: Mapped[list] = mapped_column(JSON)
    concurrency: Mapped[int] = mapped_column(Integer, default=2)


class RequestRecord(Base):
    __tablename__ = "requests"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    app_id: Mapped[str] = mapped_column(ForeignKey("applications.id"))
    model: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(24))
    error: Mapped[str] = mapped_column(String(64), default="")
    started_at: Mapped[float] = mapped_column(Double)
    deadline_at: Mapped[float] = mapped_column(Double)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    first_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bytes_out: Mapped[int] = mapped_column(Integer, default=0)
    usage: Mapped[dict] = mapped_column(JSON, default=dict)
    __table_args__ = (
        Index("ix_app_started_id", "app_id", "started_at", "id"),
        Index("ix_status_deadline", "status", "deadline_at"),
    )


def key_hash(key):
    return hashlib.sha256(key.encode()).hexdigest()


class Database:
    def __init__(self, url, pool_size=None):
        options = {}
        if url.startswith("mysql+pymysql:"):
            options["connect_args"] = {"connect_timeout": 3, "read_timeout": 5, "write_timeout": 5}
            if pool_size:
                options.update(pool_size=pool_size, max_overflow=0, pool_timeout=1)
        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=300, **options)

    def authenticate(self, key):
        with Session(self.engine) as session:
            app = session.scalar(
                select(Application).where(
                    Application.key_hash == key_hash(key), Application.enabled.is_(True)
                )
            )
            if app is None:
                return None
            return {"id": app.id, "models": app.model_allowlist, "concurrency": app.concurrency}

    def create_record(self, request_id, app_id, model, total_seconds):
        now = time.time()
        with Session(self.engine) as session, session.begin():
            session.add(
                RequestRecord(
                    id=request_id,
                    app_id=app_id,
                    model=model,
                    status="accepted",
                    started_at=now,
                    deadline_at=now + total_seconds,
                )
            )

    def update_record(self, request_id, **fields):
        with Session(self.engine) as session, session.begin():
            return session.execute(
                update(RequestRecord)
                .where(
                    RequestRecord.id == request_id,
                    RequestRecord.status.in_(("accepted", "running")),
                )
                .values(**fields)
            ).rowcount

    def get_record(self, app_id, request_id):
        with Session(self.engine) as session:
            row = session.scalar(
                select(RequestRecord).where(
                    RequestRecord.id == request_id, RequestRecord.app_id == app_id
                )
            )
            return self.serialize(row) if row else None

    def list_records(self, app_id, before=None, limit=25, *, status=None, model=None, since=None):
        with Session(self.engine) as session:
            query = select(RequestRecord).where(RequestRecord.app_id == app_id)
            filters = []
            if status:
                filters.append(RequestRecord.status == status)
            if model:
                filters.append(RequestRecord.model == model)
            if since is not None:
                filters.append(RequestRecord.started_at >= since)
            query = query.where(*filters)
            if before:
                anchor = session.scalar(
                    select(RequestRecord).where(
                        RequestRecord.id == before, RequestRecord.app_id == app_id, *filters
                    )
                )
                if anchor is None:
                    return None
                query = query.where(
                    (RequestRecord.started_at < anchor.started_at)
                    | (
                        (RequestRecord.started_at == anchor.started_at)
                        & (RequestRecord.id < anchor.id)
                    )
                )
            rows = session.scalars(
                query.order_by(RequestRecord.started_at.desc(), RequestRecord.id.desc()).limit(
                    limit + 1
                )
            ).all()
            return {
                "items": [self.serialize(row) for row in rows[:limit]],
                "next_cursor": rows[limit - 1].id if len(rows) > limit else None,
            }

    def statistics(self, app_id, hours=24):
        since = time.time() - hours * 3600
        with Session(self.engine) as session:
            rows = (
                session.execute(
                    select(
                        RequestRecord.model,
                        RequestRecord.status,
                        func.count().label("requests"),
                        func.avg(RequestRecord.first_ms).label("average_first_frame_ms"),
                        func.avg(
                            (RequestRecord.finished_at - RequestRecord.started_at) * 1000
                        ).label("average_terminal_ms"),
                        func.sum(RequestRecord.bytes_out).label("attempted_bytes"),
                        func.sum(
                            func.coalesce(RequestRecord.usage["total_tokens"].as_integer(), 0)
                        ).label("observed_total_tokens"),
                    )
                    .where(RequestRecord.app_id == app_id, RequestRecord.started_at >= since)
                    .group_by(RequestRecord.model, RequestRecord.status)
                    .order_by(RequestRecord.model, RequestRecord.status)
                )
                .mappings()
                .all()
            )
            return {
                "hours": hours,
                "since": since,
                "total_requests": sum(row["requests"] for row in rows),
                "groups": [
                    {
                        key: float(value)
                        if key.startswith("average") and value is not None
                        else int(value)
                        if key in ("requests", "attempted_bytes", "observed_total_tokens")
                        else value
                        for key, value in row.items()
                    }
                    for row in rows
                ],
            }

    def reconcile(self, grace=5):
        now = time.time()
        with Session(self.engine) as session, session.begin():
            return session.execute(
                update(RequestRecord)
                .where(
                    RequestRecord.status.in_(("accepted", "running")),
                    RequestRecord.deadline_at < now - grace,
                )
                .values(
                    status="abandoned", error="process_lost_or_finalize_failed", finished_at=now
                )
            ).rowcount

    def purge(self, retention_days):
        with Session(self.engine) as session, session.begin():
            return session.execute(
                delete(RequestRecord).where(
                    RequestRecord.finished_at < time.time() - retention_days * 86400
                )
            ).rowcount

    @staticmethod
    def serialize(row):
        return {column.name: getattr(row, column.name) for column in row.__table__.columns}
