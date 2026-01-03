# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import pytest
from datus_starrocks import StarRocksConfig, StarRocksConnector

# ==================== Table Metadata Tests ====================


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_tables(connector: StarRocksConnector):
    """Test getting table list."""
    tables = connector.get_tables()
    assert isinstance(tables, list)


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_tables_with_ddl(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting tables with DDL definitions."""
    tables = connector.get_tables_with_ddl(catalog_name=config.catalog)

    if len(tables) > 0:
        table = tables[0]
        assert "table_name" in table
        assert "definition" in table
        assert table["table_type"] == "table"
        assert "database_name" in table
        assert table["schema_name"] == ""
        assert table["catalog_name"] == config.catalog
        assert "identifier" in table
        # Identifier should be catalog.database.table (3 parts)
        assert len(table["identifier"].split(".")) == 3


@pytest.mark.integration
def test_get_tables_with_catalog_filter(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting tables with catalog filter."""
    tables = connector.get_tables(catalog_name=config.catalog, database_name=config.database)
    assert isinstance(tables, list)

    # If database exists, should have some tables
    if config.database:
        # Tables list may be empty for new databases
        pass


@pytest.mark.integration
def test_get_tables_metadata_includes_catalog(connector: StarRocksConnector, config: StarRocksConfig):
    """Test that table metadata includes catalog_name."""
    tables = connector.get_tables_with_ddl(catalog_name=config.catalog, database_name=config.database)

    for table in tables:
        assert "catalog_name" in table
        assert table["catalog_name"] == config.catalog


# ==================== View Metadata Tests ====================


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_views(connector: StarRocksConnector):
    """Test getting view list."""
    views = connector.get_views()
    assert isinstance(views, list)


@pytest.mark.integration
def test_get_views_with_ddl(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting views with DDL definitions."""
    views = connector.get_views_with_ddl(catalog_name=config.catalog)

    if len(views) > 0:
        view = views[0]
        assert "table_name" in view
        assert "definition" in view
        assert view["table_type"] == "view"
        assert "database_name" in view
        assert view["schema_name"] == ""
        assert "catalog_name" in view


@pytest.mark.integration
def test_get_views_identifier_format(connector: StarRocksConnector, config: StarRocksConfig):
    """Test view identifier includes catalog."""
    views = connector.get_views_with_ddl(catalog_name=config.catalog, database_name=config.database)

    if len(views) > 0:
        view = views[0]
        identifier_parts = view["identifier"].split(".")
        # Identifier should be catalog.database.view (3 parts)
        assert len(identifier_parts) == 3
        assert identifier_parts[0] == view["catalog_name"]
        assert identifier_parts[1] == view["database_name"]
        assert identifier_parts[2] == view["table_name"]


# ==================== Sample Data Tests ====================


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_sample_rows_default(connector: StarRocksConnector):
    """Test getting sample rows with defaults."""
    sample_rows = connector.get_sample_rows()
    assert isinstance(sample_rows, list)


@pytest.mark.integration
def test_get_sample_rows_with_catalog(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting sample rows for specific catalog and database."""
    sample_rows = connector.get_sample_rows(catalog_name=config.catalog, database_name=config.database)

    if len(sample_rows) > 0:
        item = sample_rows[0]
        assert "database_name" in item
        assert "table_name" in item
        assert "catalog_name" in item
        assert item["schema_name"] == ""
        assert "identifier" in item
        # Identifier should be catalog.database.table (3 parts)
        assert len(item["identifier"].split(".")) == 3
        assert "sample_rows" in item


@pytest.mark.integration
def test_get_sample_rows_specific_tables(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting sample rows for specific tables."""
    # First get available tables
    tables = connector.get_tables(catalog_name=config.catalog, database_name=config.database)

    if len(tables) > 0:
        table_name = tables[0]
        sample_rows = connector.get_sample_rows(
            catalog_name=config.catalog, database_name=config.database, tables=[table_name], top_n=3
        )

        assert len(sample_rows) == 1
        assert sample_rows[0]["table_name"] == table_name
        assert sample_rows[0]["catalog_name"] == config.catalog
    else:
        pytest.skip("No tables available in test database")
