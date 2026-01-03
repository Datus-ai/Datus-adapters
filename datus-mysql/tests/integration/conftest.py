# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
from typing import Generator

import pytest
from datus_mysql import MySQLConfig, MySQLConnector


@pytest.fixture
def config() -> MySQLConfig:
    """Create MySQL configuration for integration tests from environment or defaults."""
    return MySQLConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        username=os.getenv("MYSQL_USER", "test_user"),
        password=os.getenv("MYSQL_PASSWORD", "test_password"),
        database=os.getenv("MYSQL_DATABASE", "test"),
    )


@pytest.fixture
def connector(config: MySQLConfig) -> Generator[MySQLConnector, None, None]:
    """Create and cleanup MySQL connector for integration tests."""
    try:
        conn = MySQLConnector(config)
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
