"""Unix秒时间需要双精度，避免MySQL单精度丢失短期限。"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade():
    for name in ("started_at", "deadline_at", "finished_at"):
        op.alter_column(
            "requests",
            name,
            existing_type=sa.Float(),
            type_=sa.Double(),
            existing_nullable=name == "finished_at",
        )


def downgrade():
    # 恢复单精度会丢失时间精度，不提供静默降级。
    raise RuntimeError("此迁移不支持无损降级")
