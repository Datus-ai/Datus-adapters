# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from unittest.mock import MagicMock, patch

import pytest
from datus.tools.db_tools.mixins import CatalogSupportMixin, MaterializedViewSupportMixin
from datus.utils.constants import DBType
from datus_starrocks import StarRocksConfig, StarRocksConnector

# ==================== Initialization Tests ====================


@pytest.mark.acceptance
def test_connector_initialization_with_config_object():
    """Test connector initialization with StarRocksConfig object."""
    config = StarRocksConfig(
        host="localhost",
        port=9030,
        username="test_user",
        password="test_pass",
        catalog="test_catalog",
        database="testdb",
    )

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert connector.starrocks_config == config
        assert connector.catalog_name == "test_catalog"
        assert connector.dialect == DBType.STARROCKS


@pytest.mark.acceptance
def test_connector_initialization_with_dict():
    """Test connector initialization with dict config."""
    config_dict = {
        "host": "192.168.1.100",
        "port": 9031,
        "username": "admin",
        "password": "secret",
        "catalog": "custom_catalog",
        "database": "mydb",
    }

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config_dict)

        assert isinstance(connector.starrocks_config, StarRocksConfig)
        assert connector.catalog_name == "custom_catalog"
        assert connector.dialect == DBType.STARROCKS


def test_connector_initialization_invalid_type():
    """Test that connector raises TypeError for invalid config type."""
    with pytest.raises(TypeError, match="config must be StarRocksConfig or dict"):
        StarRocksConnector("invalid_config")


def test_connector_stores_starrocks_config():
    """Test that connector stores StarRocksConfig object."""
    config = StarRocksConfig(username="test_user", catalog="my_catalog")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert hasattr(connector, "starrocks_config")
        assert connector.starrocks_config.catalog == "my_catalog"


def test_connector_passes_mysql_config_to_parent():
    """Test that connector converts and passes MySQLConfig to parent."""
    config = StarRocksConfig(
        host="localhost",
        port=9030,
        username="user",
        password="pass",
        database="db",
    )

    with patch("datus_mysql.MySQLConnector.__init__") as mock_init:
        StarRocksConnector(config)

        mock_init.assert_called_once()
        mysql_config = mock_init.call_args[0][0]
        assert mysql_config.host == "localhost"
        assert mysql_config.port == 9030
        assert mysql_config.username == "user"


# ==================== Catalog Functionality Unit Tests ====================


@pytest.mark.acceptance
def test_default_catalog_returns_default_catalog():
    """Test that default_catalog returns 'default_catalog'."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert connector.default_catalog() == "default_catalog"


def test_reset_catalog_to_default_with_empty():
    """Test reset_catalog_to_default with empty string."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.reset_catalog_to_default("")
        assert result == "default_catalog"


def test_reset_catalog_to_default_with_def():
    """Test reset_catalog_to_default with 'def' string."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.reset_catalog_to_default("def")
        assert result == "default_catalog"


def test_reset_catalog_to_default_preserves_custom():
    """Test reset_catalog_to_default preserves custom catalog."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.reset_catalog_to_default("my_catalog")
        assert result == "my_catalog"


@pytest.mark.acceptance
def test_switch_catalog_updates_catalog_name():
    """Test that switch_catalog updates catalog_name attribute."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.switch_context = MagicMock()

        connector.switch_catalog("new_catalog")

        assert connector.catalog_name == "new_catalog"


def test_switch_catalog_calls_switch_context():
    """Test that switch_catalog calls switch_context."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.switch_context = MagicMock()

        connector.switch_catalog("target_catalog")

        connector.switch_context.assert_called_once_with(catalog_name="target_catalog")


def test_before_metadata_query_switches_catalog():
    """Test that _before_metadata_query switches to target catalog."""
    config = StarRocksConfig(username="test_user", catalog="original_catalog")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.switch_context = MagicMock()

        connector._before_metadata_query(catalog_name="target_catalog")

        connector.switch_context.assert_called_once_with(catalog_name="target_catalog")


def test_before_metadata_query_no_switch_if_same():
    """Test that _before_metadata_query doesn't switch if catalog is same."""
    config = StarRocksConfig(username="test_user", catalog="same_catalog")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.switch_context = MagicMock()

        connector._before_metadata_query(catalog_name="same_catalog")

        connector.switch_context.assert_not_called()


# ==================== full_name() Method Tests ====================


@pytest.mark.acceptance
def test_full_name_with_catalog_and_database():
    """Test full_name with catalog, database, and table."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(catalog_name="my_catalog", database_name="my_db", table_name="my_table")

        assert result == "`my_catalog`.`my_db`.`my_table`"


def test_full_name_with_catalog_only():
    """Test full_name with catalog and table only."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(catalog_name="my_catalog", table_name="my_table")

        assert result == "`my_table`"


def test_full_name_with_database_no_catalog():
    """Test full_name with database and table, no explicit catalog (uses default)."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(database_name="my_db", table_name="my_table")

        # Empty catalog is reset to default_catalog, so result includes it
        assert result == "`default_catalog`.`my_db`.`my_table`"


def test_full_name_table_only():
    """Test full_name with table only."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(table_name="my_table")

        assert result == "`my_table`"


def test_full_name_resets_empty_catalog_to_default():
    """Test full_name resets empty catalog to default."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(catalog_name="", database_name="db", table_name="table")

        # Empty catalog is reset to default_catalog
        assert result == "`default_catalog`.`db`.`table`"


@pytest.mark.acceptance
def test_full_name_quotes_identifiers():
    """Test full_name adds backticks to identifiers."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(catalog_name="catalog", database_name="database", table_name="table")

        assert result.count("`") == 6  # 3 pairs of backticks


def test_full_name_with_special_characters():
    """Test full_name with special characters in names."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        result = connector.full_name(catalog_name="test-catalog", database_name="test_db", table_name="test-table")

        assert "`test-catalog`" in result
        assert "`test_db`" in result
        assert "`test-table`" in result


# ==================== _sqlalchemy_schema() Tests ====================


def test_sqlalchemy_schema_with_catalog_and_database():
    """Test _sqlalchemy_schema returns catalog.database format."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.database_name = "my_db"
        connector.catalog_name = "my_catalog"
        connector.support_catalog = MagicMock(return_value=True)

        result = connector._sqlalchemy_schema(catalog_name="test_catalog", database_name="test_db")

        assert result == "test_catalog.test_db"


def test_sqlalchemy_schema_with_catalog_only():
    """Test _sqlalchemy_schema returns None when no database."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.database_name = None
        connector.catalog_name = "my_catalog"
        connector.support_catalog = MagicMock(return_value=True)

        result = connector._sqlalchemy_schema(catalog_name="test_catalog")

        assert result is None


def test_sqlalchemy_schema_without_catalog_support():
    """Test _sqlalchemy_schema when catalog not supported."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.database_name = "my_db"
        connector.support_catalog = MagicMock(return_value=False)

        result = connector._sqlalchemy_schema(database_name="test_db")

        assert result == "test_db"


def test_sqlalchemy_schema_uses_default_catalog():
    """Test _sqlalchemy_schema uses default catalog when not specified."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.database_name = "my_db"
        connector.catalog_name = None
        connector.support_catalog = MagicMock(return_value=True)

        result = connector._sqlalchemy_schema(database_name="test_db")

        assert "default_catalog" in result
        assert result == "default_catalog.test_db"


# ==================== close() Method PyMySQL Error Handling Tests ====================


@pytest.mark.acceptance
def test_close_ignores_struct_pack_error():
    """Test close ignores struct.pack error."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = MagicMock()
        connector.engine = None

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("struct.pack error")):
            # Should not raise exception
            connector.close()

            assert connector.connection is None


def test_close_ignores_com_quit_error():
    """Test close ignores COM_QUIT error."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = MagicMock()
        connector.engine = None

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("COMMAND.COM_QUIT failed")):
            # Should not raise exception
            connector.close()

            assert connector.connection is None


def test_close_ignores_required_argument_error():
    """Test close ignores 'required argument is not an integer' error."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = MagicMock()
        connector.engine = None

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("required argument is not an integer")):
            # Should not raise exception
            connector.close()

            assert connector.connection is None


def test_close_clears_connection_on_pymysql_error():
    """Test close clears connection variables on PyMySQL error."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = MagicMock()
        connector.engine = None

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("struct.error")):
            connector.close()

            assert connector.connection is None


def test_close_disposes_engine_on_error():
    """Test close disposes engine on PyMySQL error."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = None
        mock_engine = MagicMock()
        connector.engine = mock_engine

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("struct.pack")):
            connector.close()

            # Engine should be disposed and set to None
            mock_engine.dispose.assert_called_once()
            assert connector.engine is None


def test_close_reraises_unexpected_errors():
    """Test close reraises unexpected errors."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connection = None
        connector.engine = None

        with patch("datus_mysql.MySQLConnector.close", side_effect=Exception("Unexpected error")):
            with pytest.raises(Exception, match="Unexpected error"):
                connector.close()


# ==================== Utility Method Tests ====================


def test_to_dict_includes_catalog():
    """Test to_dict includes catalog field."""
    config = StarRocksConfig(username="test_user", catalog="my_catalog")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.host = "localhost"
        connector.port = 9030
        connector.user = "test_user"
        connector.database_name = "testdb"

        result = connector.to_dict()

        assert result["db_type"] == DBType.STARROCKS
        assert result["catalog"] == "my_catalog"
        assert result["host"] == "localhost"
        assert result["port"] == 9030


def test_get_type_returns_starrocks():
    """Test get_type returns STARROCKS."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert connector.get_type() == DBType.STARROCKS


def test_context_manager_support():
    """Test connector supports context manager protocol."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)
        connector.connect = MagicMock()
        connector.close = MagicMock()

        # Test context manager
        with connector as conn:
            assert conn is connector
            connector.connect.assert_called_once()

        connector.close.assert_called_once()


# ==================== Mixin Interface Tests ====================


@pytest.mark.acceptance
def test_implements_catalog_support_mixin():
    """Test connector implements CatalogSupportMixin."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert isinstance(connector, CatalogSupportMixin)


def test_implements_materialized_view_support_mixin():
    """Test connector implements MaterializedViewSupportMixin."""
    config = StarRocksConfig(username="test_user")

    with patch("datus_mysql.MySQLConnector.__init__", return_value=None):
        connector = StarRocksConnector(config)

        assert isinstance(connector, MaterializedViewSupportMixin)
