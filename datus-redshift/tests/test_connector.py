# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
from typing import Generator

import pytest
from datus.tools.db_tools.mixins import MaterializedViewSupportMixin, SchemaNamespaceMixin
from datus_redshift import RedshiftConfig, RedshiftConnector

# Skip all tests if Redshift credentials are not provided
pytestmark = pytest.mark.skipif(
    not all(
        [
            os.getenv("REDSHIFT_HOST"),
            os.getenv("REDSHIFT_USERNAME"),
            os.getenv("REDSHIFT_PASSWORD"),
        ]
    ),
    reason="Redshift credentials not provided in environment variables",
)


@pytest.fixture
def config() -> RedshiftConfig:
    """Create Redshift configuration from environment."""
    return RedshiftConfig(
        host=os.getenv("REDSHIFT_HOST", ""),
        username=os.getenv("REDSHIFT_USERNAME", ""),
        password=os.getenv("REDSHIFT_PASSWORD", ""),
        database=os.getenv("REDSHIFT_DATABASE", "dev"),
        schema=os.getenv("REDSHIFT_SCHEMA", "public"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
    )


@pytest.fixture
def connector(config: RedshiftConfig) -> Generator[RedshiftConnector, None, None]:
    """Create and cleanup Redshift connector."""
    conn = RedshiftConnector(config)
    yield conn
    conn.close()


# ==================== Mixin Tests ====================


def test_connector_implements_schema_namespace_mixin(connector: RedshiftConnector):
    """Verify connector implements SchemaNamespaceMixin."""
    assert isinstance(connector, SchemaNamespaceMixin)


def test_connector_implements_materialized_view_support_mixin(connector: RedshiftConnector):
    """Verify connector implements MaterializedViewSupportMixin."""
    assert isinstance(connector, MaterializedViewSupportMixin)


# ==================== Configuration Tests ====================


def test_config_with_minimal_params():
    """Test configuration with only required parameters."""
    config = RedshiftConfig(
        host="test-cluster.region.redshift.amazonaws.com",
        username="testuser",
        password="testpass",
    )
    assert config.host == "test-cluster.region.redshift.amazonaws.com"
    assert config.username == "testuser"
    assert config.password == "testpass"
    assert config.port == 5439  # Default port
    assert config.ssl is True  # Default SSL


def test_config_with_all_params():
    """Test configuration with all parameters."""
    config = RedshiftConfig(
        host="test-cluster.region.redshift.amazonaws.com",
        username="testuser",
        password="testpass",
        database="testdb",
        schema="testschema",
        port=5440,
        ssl=False,
        timeout_seconds=60,
    )
    assert config.host == "test-cluster.region.redshift.amazonaws.com"
    assert config.database == "testdb"
    assert config.schema_name == "testschema"
    assert config.port == 5440
    assert config.ssl is False
    assert config.timeout_seconds == 60


def test_config_with_dict():
    """Test creating connector from dict config."""
    config_dict = {
        "host": "test-cluster.region.redshift.amazonaws.com",
        "username": "testuser",
        "password": "testpass",
    }
    connector = RedshiftConnector(config_dict)
    assert connector is not None
    connector.close()


# ==================== Connection Tests ====================


def test_get_type(connector: RedshiftConnector):
    """Verify connector returns correct database type."""
    assert connector.get_type() == "redshift"


def test_connection(connector: RedshiftConnector):
    """Test database connection."""
    result = connector.test_connection()
    assert result["success"] is True
    assert "message" in result


def test_simple_query(connector: RedshiftConnector):
    """Execute a simple query."""
    result = connector.execute_query("SELECT 1 as test_column")
    assert result.success is True
    assert result.row_count == 1


# ==================== Metadata Tests ====================


def test_get_databases(connector: RedshiftConnector):
    """Test retrieving databases."""
    databases = connector.get_databases(include_sys=False)
    assert isinstance(databases, list)
    assert len(databases) > 0


def test_get_schemas(connector: RedshiftConnector):
    """Test retrieving schemas."""
    schemas = connector.get_schemas(include_sys=False)
    assert isinstance(schemas, list)
    assert len(schemas) > 0
    assert "public" in schemas


def test_get_tables(connector: RedshiftConnector):
    """Test retrieving tables."""
    tables = connector.get_tables(schema_name="public")
    assert isinstance(tables, list)


def test_get_views(connector: RedshiftConnector):
    """Test retrieving views."""
    views = connector.get_views(schema_name="public")
    assert isinstance(views, list)


def test_get_materialized_views(connector: RedshiftConnector):
    """Test retrieving materialized views."""
    mvs = connector.get_materialized_views(schema_name="public")
    assert isinstance(mvs, list)


# ==================== Query Execution Tests ====================


def test_execute_csv(connector: RedshiftConnector):
    """Test CSV result format."""
    result = connector.execute_query("SELECT 1 as num, 'test' as str", result_format="csv")
    assert result.success is True
    assert result.result_format == "csv"
    assert isinstance(result.sql_return, str)


def test_execute_pandas(connector: RedshiftConnector):
    """Test pandas result format."""
    result = connector.execute_query("SELECT 1 as num, 'test' as str", result_format="pandas")
    assert result.success is True
    assert result.result_format == "pandas"


def test_execute_arrow(connector: RedshiftConnector):
    """Test arrow result format."""
    result = connector.execute_query("SELECT 1 as num, 'test' as str", result_format="arrow")
    assert result.success is True
    assert result.result_format == "arrow"


def test_execute_list(connector: RedshiftConnector):
    """Test list result format."""
    result = connector.execute_query("SELECT 1 as num, 'test' as str", result_format="list")
    assert result.success is True
    assert result.result_format == "list"
    assert isinstance(result.sql_return, list)


def test_error_handling(connector: RedshiftConnector):
    """Test error handling for invalid SQL."""
    result = connector.execute_query("SELECT * FROM nonexistent_table_xyz")
    assert result.success is False
    assert result.error is not None


# ==================== Schema Information Tests ====================


def test_get_schema_for_table(connector: RedshiftConnector):
    """Test retrieving table schema."""
    tables = connector.get_tables(schema_name="public")
    if tables:
        schema_info = connector.get_schema(schema_name="public", table_name=tables[0])
        assert isinstance(schema_info, list)
        if schema_info:
            # Check column information
            for col in schema_info[:-1]:  # Skip summary dict
                assert "name" in col
                assert "type" in col
                assert "nullable" in col


def test_get_sample_rows(connector: RedshiftConnector):
    """Test getting sample rows."""
    tables = connector.get_tables(schema_name="public")
    if tables:
        samples = connector.get_sample_rows(schema_name="public", tables=[tables[0]], top_n=3)
        assert isinstance(samples, list)
        if samples:
            assert "table_name" in samples[0]
            assert "sample_rows" in samples[0]


# ==================== Full Name Tests ====================


def test_full_name_with_schema(connector: RedshiftConnector):
    """Test full name generation with schema."""
    full_name = connector.full_name(schema_name="myschema", table_name="mytable")
    assert full_name == '"myschema"."mytable"'


def test_full_name_without_schema(connector: RedshiftConnector):
    """Test full name generation without schema."""
    full_name = connector.full_name(table_name="mytable")
    assert full_name == '"mytable"'


def test_full_name_with_database(connector: RedshiftConnector):
    """Test full name generation with database."""
    full_name = connector.full_name(database_name="mydb", schema_name="myschema", table_name="mytable")
    assert full_name == '"mydb"."myschema"."mytable"'


# ==================== Identifier Tests ====================


def test_identifier_generation(connector: RedshiftConnector):
    """Test identifier generation."""
    identifier = connector.identifier(database_name="mydb", schema_name="myschema", table_name="mytable")
    assert "mydb" in identifier
    assert "myschema" in identifier
    assert "mytable" in identifier
