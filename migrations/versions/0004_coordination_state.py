"""保存共享容量配置指纹，识别首次部署与 Redis 状态丢失。"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"


def upgrade():
    op.create_table(
        "coordination_states",
        sa.Column("namespace", sa.String(80), primary_key=True),
        sa.Column("policy_hash", sa.String(64), nullable=False),
    )


def downgrade():
    op.drop_table("coordination_states")
