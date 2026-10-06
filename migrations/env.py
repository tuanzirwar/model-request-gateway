from alembic import context
from sqlalchemy import create_engine

from model_gateway.config import load_settings
from model_gateway.db import Base

engine = create_engine(load_settings().database_url)
with engine.connect() as connection:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()
