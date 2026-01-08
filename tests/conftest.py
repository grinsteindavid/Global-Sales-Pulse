import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl", "src"))

from models import Base


@pytest.fixture(scope="session")
def database_url():
    return os.environ.get(
        "WAREHOUSE_DATABASE_URL",
        "postgresql+psycopg2://warehouse:warehouse@localhost:5433/warehouse_test",
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
