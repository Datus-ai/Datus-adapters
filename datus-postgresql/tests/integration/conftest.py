# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
from typing import Generator

import pytest
from datus_postgresql import PostgreSQLConfig, PostgreSQLConnector


@pytest.fixture
def config() -> PostgreSQLConfig:
    """Create PostgreSQL configuration for integration tests from environment or defaults."""
    return PostgreSQLConfig(
        host=os.getenv("POSTGRESQL_HOST", "localhost"),
        port=int(os.getenv("POSTGRESQL_PORT", "5432")),
        username=os.getenv("POSTGRESQL_USER", "test_user"),
        password=os.getenv("POSTGRESQL_PASSWORD", "test_password"),
        database=os.getenv("POSTGRESQL_DATABASE", "test"),
        schema_name=os.getenv("POSTGRESQL_SCHEMA", "public"),
    )


@pytest.fixture
def connector(config: PostgreSQLConfig) -> Generator[PostgreSQLConnector, None, None]:
    """Create and cleanup PostgreSQL connector for integration tests."""
    try:
        conn = PostgreSQLConnector(config)
        if not conn.test_connection():
            pytest.skip("Database connection test failed")
        yield conn
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
