"""真实统计接口使用派生token数值列和覆盖索引，避免十万条JSON回表读取。"""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"


def upgrade():
    op.add_column(
        "requests",
        sa.Column(
            "usage_total_tokens",
            sa.BigInteger(),
            sa.Computed("COALESCE(JSON_EXTRACT(`usage`, '$.total_tokens'), 0)", persisted=False),
        ),
    )
    op.create_index(
        "ix_app_statistics",
        "requests",
        [
            "app_id",
            "started_at",
            "model",
            "status",
            "first_ms",
            "finished_at",
            "bytes_out",
            "usage_total_tokens",
        ],
    )


def downgrade():
    op.drop_index("ix_app_statistics", table_name="requests")
    op.drop_column("requests", "usage_total_tokens")
