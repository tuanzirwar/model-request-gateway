"""稀疏模型和状态筛选先定位应用及筛选值，再按时间分页。"""

from alembic import op

revision = "0003"
down_revision = "0002"


def upgrade():
    op.create_index(
        "ix_app_status_started_id", "requests", ["app_id", "status", "started_at", "id"]
    )
    op.create_index("ix_app_model_started_id", "requests", ["app_id", "model", "started_at", "id"])


def downgrade():
    op.drop_index("ix_app_model_started_id", table_name="requests")
    op.drop_index("ix_app_status_started_id", table_name="requests")
