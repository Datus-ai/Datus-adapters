# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os

import pytest
from datus_starrocks import StarRocksConfig, StarRocksConnector

# ==================== Connection Tests ====================


@pytest.mark.integration
@pytest.mark.acceptance
def test_connection_with_config_object(config: StarRocksConfig):
    """Test connection using StarRocksConfig object."""
    conn = StarRocksConnector(config)
    assert conn.test_connection()
    conn.close()


@pytest.mark.integration
@pytest.mark.acceptance
def test_connection_with_dict():
    """Test connection using dict config."""
    conn = StarRocksConnector(
        {
            "host": os.getenv("STARROCKS_HOST", "localhost"),
            "port": int(os.getenv("STARROCKS_PORT", "9030")),
            "username": os.getenv("STARROCKS_USER", "root"),
            "password": os.getenv("STARROCKS_PASSWORD", ""),
        }
    )
    assert conn.test_connection()
    conn.close()


@pytest.mark.integration
@pytest.mark.acceptance
def test_context_manager(config: StarRocksConfig):
    """Test connector as context manager."""
    with StarRocksConnector(config) as conn:
        assert conn.test_connection()
    # Connection should be closed after context


@pytest.mark.integration
def test_test_connection_method(connector: StarRocksConnector):
    """Test the test_connection method."""
    result = connector.test_connection()
    assert result is True


@pytest.mark.integration
def test_connection_cleanup_on_error(config: StarRocksConfig):
    """Test connection cleanup when errors occur."""
    conn = StarRocksConnector(config)

    try:
        conn.connect()
        # Connection is open
        assert conn.test_connection()
    finally:
        # Cleanup should handle PyMySQL errors gracefully
        conn.close()
        # Should not raise exception

    # Verify connection is closed (no exception on re-close)
    conn.close()
