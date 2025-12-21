# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
import uuid
from typing import Generator

import pytest
from datus.utils.exceptions import DatusException
from datus_clickhouse import ClickHouseConfig, ClickHouseConnector


@pytest.fixture
def config() -> ClickHouseConfig:
    """Create ClickHouse configuration from environment or defaults."""
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        username=os.getenv("CLICKHOUSE_USER", "root"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "test"),
    )


@pytest.fixture
def connector(config: ClickHouseConfig) -> Generator[ClickHouseConnector, None, None]:
    """Create and cleanup ClickHouse connector."""
    conn = ClickHouseConnector(config)
    yield conn
    conn.close()

# ==================== Connection Tests ====================


def test_connection_with_config_object(config: ClickHouseConfig):
    """Test connection using config object."""
    conn = ClickHouseConnector(config)
    assert conn.test_connection()
    conn.close()


def test_connection_with_dict():
    """Test connection using dict config."""
    conn = ClickHouseConnector(
        {
            "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
            "username": os.getenv("CLICKHOUSE_USER", "root"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        }
    )
    assert conn.test_connection()
    conn.close()


# ==================== Database Tests ====================


def test_get_databases(connector: ClickHouseConnector):
    """Test getting list of databases."""
    databases = connector.get_databases()
    assert isinstance(databases, list)
    assert len(databases) > 0


def test_get_databases_exclude_system(connector: ClickHouseConnector):
    """Test that system databases are excluded by default."""
    databases = connector.get_databases(include_sys=False)
    system_dbs = {"default", "INFORMATION_SCHEMA", "system"}
    for db in databases:
        assert db not in system_dbs


# ==================== Table Metadata Tests ====================


def test_get_tables(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting table list."""
    tables = connector.get_tables(database_name=config.database)
    assert isinstance(tables, list)


def test_get_tables_with_ddl(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting tables with DDL."""
    # Create a test table first
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_table_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            `id` Int64,
            `name` Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY id
    """
    )

    try:
        tables = connector.get_tables_with_ddl(database_name=config.database, tables=[table_name])

        if len(tables) > 0:
            table = tables[0]
            assert "table_name" in table
            assert "definition" in table
            assert table["table_type"] == "table"
            assert "database_name" in table
            assert table["schema_name"] == ""
            assert "identifier" in table
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


# ==================== View Tests ====================


def test_get_views(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting view list."""
    views = connector.get_views(database_name=config.database)
    assert isinstance(views, list)


def test_get_views_with_ddl(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting views with DDL."""
    # Create a test view first
    suffix = uuid.uuid4().hex[:8]
    view_name = f"test_view_{suffix}"
    table_name = f"test_table_{suffix}"

    connector.switch_context(database_name=config.database)

    # Create base table
    connector.execute_ddl(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
             `id` Int64,
            `name` Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY id
    """
    )

    # Create view
    connector.execute_ddl(f"CREATE VIEW {view_name} AS SELECT * FROM {table_name}")

    try:
        views = connector.get_views_with_ddl(database_name=config.database)

        if len(views) > 0:
            view = [v for v in views if v["table_name"] == view_name]
            if view:
                assert "definition" in view[0]
                assert view[0]["table_type"] == "view"
    finally:
        connector.execute_ddl(f"DROP VIEW IF EXISTS {view_name}")
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


# ==================== Schema Tests ====================


def test_get_schema(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting table schema."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_schema_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            `id` String,
            `type` Nullable(String),
            `flag` Nullable(Int64),
            `entry_type` Nullable(String),
            `cnt` Nullable(Int64),
            `dt` String DEFAULT '1971-01-01',
        ) ENGINE = MergeTree()
        ORDER BY id
    """
    )

    try:
        schema = connector.get_schema(database_name=config.database, table_name=table_name)

        assert len(schema) == 6

        # Check flag column
        flag_col = next(col for col in schema if col["name"] == "flag")
        assert flag_col["pk"] is False
        assert "int64" in flag_col["type"].lower()

        # Check type column
        type_col = next(col for col in schema if col["name"] == "type")
        assert type_col["nullable"] is True
        assert "string" in type_col["type"].lower()
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


# ==================== Sample Data Tests ====================


def test_get_sample_rows(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test getting sample rows."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_sample_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
             `id` Int64,
            `name` Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY id
    """
    )

    # Insert test data
    connector.execute_insert(
        f"""
        INSERT INTO {table_name} (id, name) VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
    """
    )

    try:
        sample_rows = connector.get_sample_rows(database_name=config.database, tables=[table_name], top_n=2)

        assert len(sample_rows) == 1
        assert sample_rows[0]["table_name"] == table_name
        assert "sample_rows" in sample_rows[0]
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


# ==================== SQL Execution Tests ====================


def test_execute_select(connector: ClickHouseConnector):
    """Test executing SELECT query."""
    result = connector.execute({"sql_query": "SELECT 1 as num"}, result_format="list")
    assert result.success
    assert not result.error
    assert result.sql_return == [{"num": 1}]


def test_execute_ddl(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test DDL operations."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_ddl_{suffix}"

    connector.switch_context(database_name=config.database)

    try:
        # CREATE
        create_result = connector.execute_ddl(
            f"""
            CREATE TABLE {table_name} (
            `id` Int64,
            `name` Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY id
        """
        )
        assert create_result.success

        # ALTER
        alter_result = connector.execute_ddl(f"ALTER TABLE {table_name} ADD COLUMN age INT")
        assert alter_result.success

    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


def test_execute_insert(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test INSERT operation."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_insert_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            `id` Int64,
            `name` Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY id
    """
    )

    try:
        insert_result = connector.execute_insert(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        assert insert_result.success
        assert insert_result.row_count == 2

        # Verify
        query_result = connector.execute(
            {"sql_query": f"SELECT id, name FROM {table_name} ORDER BY id"}, result_format="list"
        )
        assert query_result.sql_return == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


def test_execute_update(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test UPDATE operation."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_update_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            `id` Int64,
            `name` Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY id
    """
    )

    try:
        # Insert initial data
        connector.execute_insert(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice'), (2, 'Bob')")

        # Update
        update_result = connector.execute_update(f"UPDATE {table_name} SET name = 'Alice Updated' WHERE id = 1")
        assert update_result.success
        assert update_result.row_count == 1

        # Verify
        query_result = connector.execute(
            {"sql_query": f"SELECT name FROM {table_name} WHERE id = 1"}, result_format="list"
        )
        assert query_result.sql_return == [{"name": "Alice Updated"}]
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


def test_execute_delete(connector: ClickHouseConnector, config: ClickHouseConfig):
    """Test DELETE operation."""
    suffix = uuid.uuid4().hex[:8]
    table_name = f"test_delete_{suffix}"

    connector.switch_context(database_name=config.database)
    connector.execute_ddl(
        f"""
        CREATE TABLE {table_name} (
            `id` Int64,
            `name` Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY id
    """
    )

    try:
        # Insert initial data
        connector.execute_insert(f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice'), (2, 'Bob')")

        # Delete
        delete_result = connector.execute_delete(f"DELETE FROM {table_name} WHERE id = 2")
        assert delete_result.success
        assert delete_result.row_count == 1

        # Verify
        query_result = connector.execute({"sql_query": f"SELECT id FROM {table_name}"}, result_format="list")
        assert query_result.sql_return == [{"id": 1}]
    finally:
        connector.execute_ddl(f"DROP TABLE IF EXISTS {table_name}")


# ==================== Error Handling Tests ====================


def test_exception_on_syntax_error(connector: ClickHouseConnector):
    """Test exception on SQL syntax error."""
    with pytest.raises(DatusException):
        connector.execute({"sql_query": "INVALID SQL SYNTAX"})


def test_exception_on_nonexistent_table(connector: ClickHouseConnector):
    """Test exception on non-existent table."""
    with pytest.raises(DatusException):
        connector.execute({"sql_query": f"SELECT * FROM nonexistent_table_{uuid.uuid4().hex}"})


# ==================== Utility Tests ====================


def test_full_name_with_database(connector: ClickHouseConnector):
    """Test full_name with database."""
    full_name = connector.full_name(database_name="mydb", table_name="mytable")
    assert full_name == "`mydb`.`mytable`"


def test_full_name_without_database(connector: ClickHouseConnector):
    """Test full_name without database."""
    full_name = connector.full_name(table_name="mytable")
    assert full_name == "`mytable`"


def test_identifier(connector: ClickHouseConnector):
    """Test identifier generation."""
    identifier = connector.identifier(database_name="mydb", table_name="mytable")
    assert identifier == "mydb.mytable"
