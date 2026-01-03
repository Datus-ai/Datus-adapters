# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from unittest.mock import patch

import pytest
from datus.utils.exceptions import DatusException
from datus_mysql import MySQLConfig, MySQLConnector


@pytest.mark.acceptance
def test_connector_initialization_with_config_object():
    """Test connector initialization with MySQLConfig object."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="test_user",
        password="test_pass",
        database="testdb",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)

        assert connector.config == config
        assert connector.host == "localhost"
        assert connector.port == 3306
        assert connector.username == "test_user"
        assert connector.password == "test_pass"
        assert connector.database_name == "testdb"


@pytest.mark.acceptance
def test_connector_initialization_with_dict():
    """Test connector initialization with dict config."""
    config_dict = {
        "host": "192.168.1.100",
        "port": 3307,
        "username": "admin",
        "password": "secret",
        "database": "mydb",
    }

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config_dict)

        assert connector.host == "192.168.1.100"
        assert connector.port == 3307
        assert connector.username == "admin"
        assert connector.password == "secret"
        assert connector.database_name == "mydb"


def test_connector_initialization_invalid_type():
    """Test that connector raises TypeError for invalid config type."""
    with pytest.raises(TypeError, match="config must be MySQLConfig or dict"):
        MySQLConnector("invalid_config")


@pytest.mark.acceptance
def test_connector_connection_string_basic():
    """Test connection string generation with basic config."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "mysql+pymysql://user:pass@localhost:3306/db" in connection_string
        assert "charset=utf8mb4" in connection_string
        assert "autocommit=true" in connection_string


@pytest.mark.acceptance
def test_connector_connection_string_special_password():
    """Test connection string generation with special characters in password."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="p@ss!w0rd#$%",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        # Password should be URL encoded
        assert "p%40ss%21w0rd%23%24%25" in connection_string


def test_connector_connection_string_no_password():
    """Test connection string generation with empty password."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "mysql+pymysql://user:@localhost:3306/db" in connection_string


def test_connector_connection_string_no_database():
    """Test connection string generation without database."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database=None,
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "mysql+pymysql://user:pass@localhost:3306/" in connection_string


def test_connector_connection_string_custom_charset():
    """Test connection string with custom charset."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database="db",
        charset="utf8",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "charset=utf8" in connection_string


def test_connector_connection_string_autocommit_false():
    """Test connection string with autocommit disabled."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database="db",
        autocommit=False,
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__") as mock_init:
        MySQLConnector(config)

        call_args = mock_init.call_args
        connection_string = call_args[0][0]

        assert "autocommit=false" in connection_string


@pytest.mark.acceptance
def test_sys_databases():
    """Test _sys_databases returns correct system databases."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)
        sys_dbs = connector._sys_databases()

        assert sys_dbs == {"sys", "information_schema", "performance_schema", "mysql"}
        assert isinstance(sys_dbs, set)


def test_sys_schemas():
    """Test _sys_schemas returns same as _sys_databases."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)
        sys_schemas = connector._sys_schemas()
        sys_dbs = connector._sys_databases()

        assert sys_schemas == sys_dbs


@pytest.mark.acceptance
def test_quote_identifier_basic():
    """Test _quote_identifier with basic identifier."""
    assert MySQLConnector._quote_identifier("table_name") == "`table_name`"


@pytest.mark.acceptance
def test_quote_identifier_with_backticks():
    """Test _quote_identifier escapes backticks."""
    assert MySQLConnector._quote_identifier("table`name") == "`table``name`"


def test_quote_identifier_with_multiple_backticks():
    """Test _quote_identifier escapes multiple backticks."""
    assert MySQLConnector._quote_identifier("ta`ble`name") == "`ta``ble``name`"


def test_quote_identifier_empty_string():
    """Test _quote_identifier with empty string."""
    assert MySQLConnector._quote_identifier("") == "``"


def test_quote_identifier_special_characters():
    """Test _quote_identifier with special characters."""
    assert MySQLConnector._quote_identifier("table-name_123") == "`table-name_123`"


@pytest.mark.acceptance
def test_full_name_with_database():
    """Test full_name method with database."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)
        full_name = connector.full_name(database_name="mydb", table_name="mytable")

        assert full_name == "`mydb`.`mytable`"


def test_full_name_without_database():
    """Test full_name method without database."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)
        full_name = connector.full_name(table_name="mytable")

        assert full_name == "`mytable`"


def test_full_name_with_special_characters():
    """Test full_name with special characters (not escaped in this method)."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)
        full_name = connector.full_name(database_name="my`db", table_name="my`table")

        # full_name doesn't escape backticks, just wraps with backticks
        assert full_name == "`my`db`.`my`table`"


def test_identifier_with_database():
    """Test identifier method with database (inherited from base class)."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        with patch("datus_sqlalchemy.SQLAlchemyConnector.identifier") as mock_identifier:
            mock_identifier.return_value = "mydb.mytable"
            connector = MySQLConnector(config)
            identifier = connector.identifier(database_name="mydb", table_name="mytable")

            assert identifier == "mydb.mytable"


def test_identifier_without_database():
    """Test identifier method without database (inherited from base class)."""
    config = MySQLConfig(username="user")

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        with patch("datus_sqlalchemy.SQLAlchemyConnector.identifier") as mock_identifier:
            mock_identifier.return_value = "mytable"
            connector = MySQLConnector(config)
            identifier = connector.identifier(table_name="mytable")

        assert identifier == "mytable"


@pytest.mark.acceptance
def test_get_metadata_config_valid_table_type():
    """Test _get_metadata_config with valid table type."""
    from datus_mysql.connector import _get_metadata_config

    config = _get_metadata_config("table")
    assert config.show_table == "TABLES"
    assert config.show_create_table == "TABLE"
    assert config.info_table == "TABLES"
    assert config.table_types == ["TABLE", "BASE TABLE"]


def test_get_metadata_config_view_type():
    """Test _get_metadata_config with view type."""
    from datus_mysql.connector import _get_metadata_config

    config = _get_metadata_config("view")
    assert config.show_table == "VIEWS"
    assert config.show_create_table == "VIEW"
    assert config.info_table == "VIEWS"


@pytest.mark.acceptance
def test_get_metadata_config_invalid_type():
    """Test _get_metadata_config with invalid table type."""
    from datus_mysql.connector import _get_metadata_config

    with pytest.raises(DatusException, match="Invalid table type"):
        _get_metadata_config("invalid_type")


def test_connector_stores_config():
    """Test that connector stores the config object."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database="db",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)

        assert connector.config == config
        assert isinstance(connector.config, MySQLConfig)


def test_connector_database_name_attribute():
    """Test that connector sets database_name attribute."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database="testdb",
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)

        assert connector.database_name == "testdb"


def test_connector_database_name_empty_when_none():
    """Test that database_name is empty string when config.database is None."""
    config = MySQLConfig(
        host="localhost",
        port=3306,
        username="user",
        password="pass",
        database=None,
    )

    with patch("datus_sqlalchemy.SQLAlchemyConnector.__init__", return_value=None):
        connector = MySQLConnector(config)

        assert connector.database_name == ""
