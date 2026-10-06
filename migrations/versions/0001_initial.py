"""授权应用与请求元数据初始结构。"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None


def upgrade():
    # 迁移固定历史结构，不导入持续变化的运行时模型。
    op.create_table(
        "applications",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("key_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("model_allowlist", sa.JSON(), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False),
    )
    op.create_table(
        "requests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("app_id", sa.String(64), sa.ForeignKey("applications.id"), nullable=False),
        sa.Column("model", sa.String(100), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("error", sa.String(64), nullable=False),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("deadline_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("first_ms", sa.Integer(), nullable=True),
        sa.Column("bytes_out", sa.Integer(), nullable=False),
        sa.Column("usage", sa.JSON(), nullable=False),
    )
    op.create_index("ix_app_started_id", "requests", ["app_id", "started_at", "id"])
    op.create_index("ix_status_deadline", "requests", ["status", "deadline_at"])


def downgrade():
    op.drop_table("requests")
    op.drop_table("applications")
