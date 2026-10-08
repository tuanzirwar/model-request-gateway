"""短事务保存授权及元数据，不在模型调用期间持有数据库连接。"""

import hashlib
import json
import time
from contextlib import nullcontext

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Computed,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    create_engine,
    delete,
    func,
    select,
    text,
    update,
)
from sqlalchemy.exc import SQLAlchemyError
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


class CoordinationState(Base):
    """命名空间配置指纹跨 Redis 重启持久化，不存放请求的活动额度。"""

    __tablename__ = "coordination_states"
    namespace: Mapped[str] = mapped_column(String(80), primary_key=True)
    policy_hash: Mapped[str] = mapped_column(String(64))


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
    # 派生列由数据库计算；统计索引不必逐行读取JSON，且不引入两份手动维护的账单值。
    usage_total_tokens: Mapped[int] = mapped_column(
        BigInteger,
        Computed("COALESCE(JSON_EXTRACT(`usage`, '$.total_tokens'), 0)", persisted=False),
    )
    __table_args__ = (
        Index("ix_app_started_id", "app_id", "started_at", "id"),
        Index("ix_app_status_started_id", "app_id", "status", "started_at", "id"),
        Index("ix_app_model_started_id", "app_id", "model", "started_at", "id"),
        Index("ix_status_deadline", "status", "deadline_at"),
        Index("ix_finished_id", "finished_at", "id"),
        Index(
            "ix_app_statistics",
            "app_id",
            "started_at",
            "model",
            "status",
            "first_ms",
            "finished_at",
            "bytes_out",
            "usage_total_tokens",
        ),
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
        self.auth_engine = self.engine
        if url.startswith("mysql+pymysql:"):
            # 鉴权只有SELECT，独立只读用途的自动提交池避免每次查询后的ROLLBACK。
            # 写入引擎仍使用真实事务，COMMIT/回滚及持久化强度不变。
            read_options = dict(options)
            read_options["connect_args"] = {**options["connect_args"], "autocommit": True}
            read_options.update(pool_size=min(pool_size or 4, 2), max_overflow=0, pool_timeout=1)
            self.auth_engine = create_engine(
                url,
                isolation_level="AUTOCOMMIT",
                skip_autocommit_rollback=True,
                pool_pre_ping=True,
                pool_recycle=300,
                **read_options,
            )

    def batch_authenticate(self, messages):
        hashes = [key_hash(args[0]) for args, _ in messages]
        with self.auth_engine.connect() as connection:
            rows = connection.execute(
                select(Application.__table__).where(
                    Application.key_hash.in_(set(hashes)), Application.enabled.is_(True)
                )
            ).mappings()
            subjects = {
                row["key_hash"]: {
                    "id": row["id"],
                    "models": row["model_allowlist"],
                    "concurrency": row["concurrency"],
                }
                for row in rows
            }
        # 只合并同一批的读取，不缓存授权，下一批仍读取停用/轮换结果。
        return [subjects.get(value) for value in hashes]

    def register_coordination(self, namespace, policy_hash):
        """唯一键处理多个进程的首次注册；配置冲突不允许各自使用不同额度。"""
        table = CoordinationState.__table__
        with self.engine.begin() as connection:
            if self.engine.dialect.name == "mysql":
                from sqlalchemy.dialects.mysql import insert

                statement = insert(table).values(namespace=namespace, policy_hash=policy_hash)
                # IGNORE 只用于固定、已校验的两字段注册，不吞掉业务记录写入错误。
                changed = connection.execute(statement.prefix_with("IGNORE")).rowcount
            else:
                from sqlalchemy.dialects.sqlite import insert

                statement = insert(table).values(namespace=namespace, policy_hash=policy_hash)
                changed = connection.execute(statement.on_conflict_do_nothing()).rowcount
            stored = connection.scalar(
                select(CoordinationState.policy_hash).where(
                    CoordinationState.namespace == namespace
                )
            )
            if stored != policy_hash:
                raise ValueError("coordination_policy_mismatch")
            return changed == 1

    def batch_create_record(self, messages):
        rows = []
        for args, fields in messages:
            request_id, app_id, model, seconds = args
            now = fields.get("started_at", time.time())
            rows.append(
                dict(
                    id=request_id,
                    app_id=app_id,
                    model=model,
                    status=fields.get("status", "accepted"),
                    started_at=now,
                    deadline_at=fields.get("deadline_at", now + seconds),
                    error="",
                    bytes_out=0,
                    usage={},
                )
            )
        with self.engine.begin() as connection:
            connection.execute(RequestRecord.__table__.insert(), rows)
        # 离开begin才完成COMMIT；事务失败时整批失败，不提前确认。
        return [None] * len(messages)

    def batch_update_record(self, messages, *, connection=None, ensure_existing=False):
        allowed = {"status", "error", "finished_at", "first_ms", "bytes_out", "usage"}
        groups = []
        for args, fields in messages:
            if not fields or set(fields) - allowed:
                raise ValueError("非法元数据更新字段")
            # 同一ID保持先后顺序，禁止CASE内出现重复ID吞掉后一条状态。
            if (
                not groups
                or set(groups[-1][0][1]) != set(fields)
                or (
                    groups[-1][0][1].get("status")
                    in ("succeeded", "failed", "cancelled", "rejected")
                )
                != (fields.get("status") in ("succeeded", "failed", "cancelled", "rejected"))
                or any(item[0][0] == args[0] for item in groups[-1])
            ):
                groups.append([])
            groups[-1].append((args, fields))
        with self.engine.begin() if connection is None else nullcontext(connection) as connection:
            for group in groups:
                parameters, assignments = {}, []
                for index, (args, _) in enumerate(group):
                    parameters[f"id{index}"] = args[0]
                for field in sorted(group[0][1]):
                    column = connection.dialect.identifier_preparer.quote(field)
                    cases = []
                    for index, (_, values) in enumerate(group):
                        name = f"v_{field}_{index}"
                        value = values[field]
                        parameters[name] = json.dumps(value) if field == "usage" else value
                        cases.append(f"WHEN :id{index} THEN :{name}")
                    assignments.append(f"{column}=CASE id {' '.join(cases)} ELSE {column} END")
                ids = ",".join(f":id{index}" for index in range(len(group)))
                states = "'accepted','running'"
                if group[0][1].get("status") in ("succeeded", "failed", "cancelled", "rejected"):
                    states += ",'abandoned'"
                changed = connection.execute(
                    text(
                        f"UPDATE requests SET {','.join(assignments)} WHERE id IN ({ids}) "
                        f"AND status IN ({states})"
                    ),
                    parameters,
                ).rowcount
                if ensure_existing and changed < len(group):
                    ids = {args[0] for args, _ in group}
                    existing = set(
                        connection.scalars(
                            select(RequestRecord.id).where(RequestRecord.id.in_(ids))
                        )
                    )
                    if ids - existing:
                        # 父记录的创建若尚在待重投区，更新也必须保留，不能将0行
                        # UPDATE当作成功ACK后丢掉最终状态。
                        raise SQLAlchemyError("metadata_parent_not_committed")
        # 热路径不依赖单条rowcount；逐条接口update_record仍保留原契约。
        return [None] * len(messages)

    def authenticate(self, key):
        with self.auth_engine.connect() as connection:
            app = (
                connection.execute(
                    select(
                        Application.id, Application.model_allowlist, Application.concurrency
                    ).where(Application.key_hash == key_hash(key), Application.enabled.is_(True))
                )
                .mappings()
                .first()
            )
            if app is None:
                return None
            return {
                "id": app["id"],
                "models": app["model_allowlist"],
                "concurrency": app["concurrency"],
            }

    def create_record(
        self,
        request_id,
        app_id,
        model,
        total_seconds,
        *,
        status="accepted",
        started_at=None,
        deadline_at=None,
    ):
        now = time.time() if started_at is None else started_at
        with Session(self.engine) as session, session.begin():
            session.add(
                RequestRecord(
                    id=request_id,
                    app_id=app_id,
                    model=model,
                    status=status,
                    started_at=now,
                    deadline_at=now + total_seconds if deadline_at is None else deadline_at,
                )
            )

    def update_record(self, request_id, **fields):
        states = ("accepted", "running")
        if fields.get("status") in ("succeeded", "failed", "cancelled", "rejected"):
            states += ("abandoned",)
        with Session(self.engine) as session, session.begin():
            return session.execute(
                update(RequestRecord)
                .where(
                    RequestRecord.id == request_id,
                    RequestRecord.status.in_(states),
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
                        func.sum(RequestRecord.usage_total_tokens).label("observed_total_tokens"),
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
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "mysql":
                # 避免周期范围UPDATE的间隙锁阻塞新接入；各实例跳过已被持有的行。
                connection = connection.execution_options(isolation_level="READ COMMITTED")
            with Session(connection) as session, session.begin():
                ids = session.scalars(
                    select(RequestRecord.id)
                    .where(
                        RequestRecord.status.in_(("accepted", "running")),
                        RequestRecord.deadline_at < now - grace,
                    )
                    .limit(256)
                    .with_for_update(skip_locked=True)
                ).all()
                if not ids:
                    return 0
                return session.execute(
                    update(RequestRecord)
                    .where(
                        RequestRecord.id.in_(ids), RequestRecord.status.in_(("accepted", "running"))
                    )
                    .values(
                        status="abandoned", error="process_lost_or_finalize_failed", finished_at=now
                    )
                ).rowcount

    def purge(self, retention_days, batch_size=256, *, app_id=None):
        """每次只删除一批已结束记录，避免全表大事务和无界锁占用。"""
        if type(batch_size) is not int or not 1 <= batch_size <= 1024:
            raise ValueError("清理每批必须为1至1024条")
        if type(retention_days) is not int or retention_days < 1:
            raise ValueError("保留天数必须为正整数")
        cutoff = time.time() - retention_days * 86400
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "mysql":
                connection = connection.execution_options(isolation_level="READ COMMITTED")
            with Session(connection) as session, session.begin():
                query = select(RequestRecord.id).where(RequestRecord.finished_at < cutoff)
                if app_id is not None:
                    query = query.where(RequestRecord.app_id == app_id)
                ids = session.scalars(
                    query.order_by(RequestRecord.finished_at, RequestRecord.id)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                ).all()
                if not ids:
                    return 0
                return session.execute(
                    delete(RequestRecord).where(
                        RequestRecord.id.in_(ids), RequestRecord.finished_at < cutoff
                    )
                ).rowcount

    @staticmethod
    def serialize(row):
        return {
            column.name: getattr(row, column.name)
            for column in row.__table__.columns
            if column.name != "usage_total_tokens"
        }
