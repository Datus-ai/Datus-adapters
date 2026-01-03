# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import pytest
from datus_starrocks import StarRocksConfig, StarRocksConnector

# ==================== Materialized View Tests (MaterializedViewSupportMixin) ====================


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_materialized_views(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting materialized view list."""
    mvs = connector.get_materialized_views(catalog_name=config.catalog)
    assert isinstance(mvs, list)
    # May be empty if no materialized views exist


@pytest.mark.integration
@pytest.mark.acceptance
def test_get_materialized_views_with_ddl(connector: StarRocksConnector):
    """Test getting materialized views with DDL definitions."""
    mvs = connector.get_materialized_views_with_ddl()

    if len(mvs) > 0:
        mv = mvs[0]
        assert "table_name" in mv
        assert "definition" in mv
        assert mv["table_type"] == "mv"
        assert "database_name" in mv
        assert mv["schema_name"] == ""
        assert "catalog_name" in mv


@pytest.mark.integration
def test_materialized_view_identifier_includes_catalog(connector: StarRocksConnector, config: StarRocksConfig):
    """Test materialized view identifier includes catalog."""
    mvs = connector.get_materialized_views_with_ddl(catalog_name=config.catalog)

    if len(mvs) > 0:
        mv = mvs[0]
        identifier_parts = mv["identifier"].split(".")
        # Identifier should be catalog.database.mv (3 parts)
        assert len(identifier_parts) == 3
        assert identifier_parts[0] == mv["catalog_name"]
        assert identifier_parts[1] == mv["database_name"]
        assert identifier_parts[2] == mv["table_name"]


@pytest.mark.integration
def test_get_materialized_views_from_specific_catalog(connector: StarRocksConnector, config: StarRocksConfig):
    """Test getting materialized views from specific catalog."""
    # Switch to the target catalog
    connector.switch_catalog(config.catalog)

    mvs = connector.get_materialized_views(catalog_name=config.catalog, database_name=config.database)
    assert isinstance(mvs, list)

    # Verify all MVs belong to the specified catalog
    mvs_with_ddl = connector.get_materialized_views_with_ddl(catalog_name=config.catalog, database_name=config.database)
    for mv in mvs_with_ddl:
        assert mv["catalog_name"] == config.catalog
        if config.database:
            assert mv["database_name"] == config.database
