"""按保留期限定位一小批已结束记录，避免每次清理扫描全部历史。"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"


def upgrade():
    op.alter_column(
        "coordination_states",
        "namespace",
        existing_type=sa.String(64),
        type_=sa.String(80),
        existing_nullable=False,
    )
    op.create_index("ix_finished_id", "requests", ["finished_at", "id"])


def downgrade():
    op.drop_index("ix_finished_id", table_name="requests")
