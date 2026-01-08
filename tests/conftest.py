import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

etl_src_local = Path(__file__).parent.parent / "etl" / "src"
etl_src_docker = Path("/app/etl_src")

if etl_src_local.exists():
    sys.path.insert(0, str(etl_src_local))
elif etl_src_docker.exists():
    sys.path.insert(0, str(etl_src_docker))

from models.base import Base


@pytest.fixture(scope="session")
def database_url():
    return os.environ.get(
        "WAREHOUSE_DATABASE_URL",
        "postgresql+psycopg2://warehouse:warehouse@localhost:5433/warehouse",
    )


@pytest.fixture(scope="session")
def engine(database_url):
    return create_engine(database_url)


@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def session(engine, tables):
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()

    yield session

    session.close()
    transaction.rollback()
    connection.close()
