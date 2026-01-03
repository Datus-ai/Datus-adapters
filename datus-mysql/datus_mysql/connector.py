# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Optional, Set, Union, override
from urllib.parse import quote_plus

from datus.schemas.base import TABLE_TYPE
from datus.tools.db_tools.base import list_to_in_str
from datus.utils.constants import DBType
from datus.utils.exceptions import DatusException, ErrorCode
from datus.utils.loggings import get_logger
from datus_sqlalchemy import SQLAlchemyConnector
from pydantic import BaseModel, Field
from sqlalchemy import text

from .config import MySQLConfig

logger = get_logger(__name__)


class TableMetadataNames(BaseModel):
    """Metadata configuration for different MySQL object types."""

    show_table: str = Field(..., description="SHOW command keyword")
    show_create_table: str = Field(..., description="SHOW CREATE command keyword")
    info_table: str = Field(..., description="INFORMATION_SCHEMA table name")
    table_types: Optional[List[str]] = Field(default=None, description="TABLE_TYPE values in INFORMATION_SCHEMA")


# Metadata configuration for MySQL objects
METADATA_DICT: Dict[TABLE_TYPE, TableMetadataNames] = {
    "table": TableMetadataNames(
        show_table="TABLES", show_create_table="TABLE", info_table="TABLES", table_types=["TABLE", "BASE TABLE"]
    ),
    "view": TableMetadataNames(
        show_table="VIEWS",
        show_create_table="VIEW",
        info_table="VIEWS",
    ),
    "mv": TableMetadataNames(
        show_table="MATERIALIZED VIEWS",
        show_create_table="MATERIALIZED VIEW",
        info_table="MATERIALIZED_VIEWS",
    ),
}


def _get_metadata_config(table_type: TABLE_TYPE) -> TableMetadataNames:
    """Get metadata configuration for given table type."""
    if table_type not in METADATA_DICT:
        raise DatusException(ErrorCode.COMMON_FIELD_INVALID, f"Invalid table type '{table_type}'")
    return METADATA_DICT[table_type]


class MySQLConnector(SQLAlchemyConnector):
    """MySQL database connector."""

    def __init__(self, config: Union[MySQLConfig, dict]):
        """
        Initialize MySQL connector.

        Args:
            config: MySQLConfig object or dict with configuration
        """
        # Handle config object or dict
        if isinstance(config, dict):
            config = MySQLConfig(**config)
        elif not isinstance(config, MySQLConfig):
            raise TypeError(f"config must be MySQLConfig or dict, got {type(config)}")

        self.config = config
        self.host = config.host
        self.port = config.port
        self.username = config.username
        self.password = config.password
        database = config.database or ""

        # URL encode password to handle special characters
        encoded_password = quote_plus(self.password) if self.password else ""

        # Build connection string
        connection_string = (
            f"mysql+pymysql://{self.username}:{encoded_password}@{self.host}:{self.port}/"
            f"{database}?charset={config.charset}&autocommit={'true' if config.autocommit else 'false'}"
        )

        super().__init__(connection_string, dialect=DBType.MYSQL)
        self.database_name = database

    # ==================== System Resources ====================

    @override
    def _sys_databases(self) -> Set[str]:
        """System databases to filter out."""
        return {"sys", "information_schema", "performance_schema", "mysql"}

    @override
    def _sys_schemas(self) -> Set[str]:
        """System schemas to filter out (same as databases for MySQL)."""
        return self._sys_databases()

    # ==================== Utility Methods ====================

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Safely wrap identifiers with backticks for MySQL-compatible dialects."""
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    # ==================== Metadata Retrieval ====================

    def _get_metadata(
        self,
        table_type: TABLE_TYPE = "table",
        catalog_name: str = "",
        database_name: str = "",
    ) -> List[Dict[str, str]]:
        """
        Get metadata for tables/views from INFORMATION_SCHEMA.

        Args:
            table_type: Type of object (table, view, mv)
            catalog_name: Catalog name (unused in MySQL)
            database_name: Database name to query

        Returns:
            List of metadata dictionaries
        """
        self.connect()
        database_name = database_name or self.database_name

        # Build WHERE clause
        if database_name:
            where = f"TABLE_SCHEMA = '{database_name}'"
        else:
            where = f"{list_to_in_str('TABLE_SCHEMA not in', list(self._sys_databases()))}"

        # Get metadata configuration
        metadata_config = _get_metadata_config(table_type)

        # Build and execute query
        type_filter = list_to_in_str("and TABLE_TYPE in ", metadata_config.table_types)
        query = (
            f"SELECT TABLE_SCHEMA, TABLE_NAME "
            f"FROM information_schema.{metadata_config.info_table} "
            f"WHERE {where} {type_filter}"
        )

        query_result = self._execute_pandas(query)

        # Format results
        result = []
        for i in range(len(query_result)):
            db_name = query_result["TABLE_SCHEMA"][i]
            tb_name = query_result["TABLE_NAME"][i]
            result.append(
                {
                    "identifier": self.identifier(database_name=db_name, table_name=tb_name),
                    "catalog_name": "",
                    "schema_name": "",
                    "database_name": db_name,
                    "table_name": tb_name,
                    "table_type": table_type,
                }
            )
        return result

    def _show_create(self, full_name: str, create_type: str) -> str:
        """
        Execute SHOW CREATE statement to get DDL.

        Args:
            full_name: Fully-qualified table name
            create_type: Object type (TABLE, VIEW, etc.)

        Returns:
            DDL statement as string
        """
        sql = f"SHOW CREATE {create_type} {full_name}"
        ddl_result = self._execute_pandas(sql)
        if not ddl_result.empty and len(ddl_result.columns) >= 2:
            return str(ddl_result.iloc[0, 1])
        return f"-- DDL not available for {full_name}"

    def _get_objects_with_ddl(
        self,
        table_type: TABLE_TYPE = "table",
        tables: Optional[List[str]] = None,
        catalog_name: str = "",
        database_name: str = "",
    ) -> List[Dict[str, str]]:
        """
        Get metadata with DDL statements.

        Args:
            table_type: Type of object
            tables: Optional list of specific tables to retrieve
            catalog_name: Catalog name (unused)
            database_name: Database name

        Returns:
            List of metadata dictionaries with DDL
        """
        result = []
        filter_tables = self._reset_filter_tables(tables, catalog_name, database_name)
        metadata_config = _get_metadata_config(table_type)

        for meta in self._get_metadata(table_type, catalog_name, database_name):
            full_name = self.full_name(database_name=meta["database_name"], table_name=meta["table_name"])

            # Skip if not in filter list
            if filter_tables and full_name not in filter_tables:
                continue

            # Get DDL
            try:
                ddl = self._show_create(full_name, metadata_config.show_create_table)
            except Exception as e:
                logger.warning(f"Could not get DDL for {full_name}: {e}")
                ddl = f"-- DDL not available for {meta['table_name']}"

            meta["definition"] = ddl
            result.append(meta)

        return result

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of table names."""
        return [meta["table_name"] for meta in self._get_metadata("table", catalog_name, database_name)]

    @override
    def get_tables_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", tables: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Get tables with DDL statements."""
        return self._get_objects_with_ddl("table", tables, catalog_name, database_name)

    @override
    def get_views_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[Dict[str, str]]:
        """Get views with DDL statements."""
        return self._get_objects_with_ddl("view", None, catalog_name, database_name)

    @override
    def get_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Get table schema using INFORMATION_SCHEMA.

        Args:
            catalog_name: Catalog name (unused)
            database_name: Database name
            schema_name: Schema name (unused)
            table_name: Table name

        Returns:
            List of column information dictionaries
        """
        if not table_name:
            return []

        database_name = database_name or self.database_name

        # Use INFORMATION_SCHEMA to get schema with comments
        sql = f"""
            SELECT
                COLUMN_NAME as Field,
                COLUMN_TYPE as Type,
                IS_NULLABLE as `Null`,
                COLUMN_KEY as `Key`,
                COLUMN_DEFAULT as `Default`,
                COLUMN_COMMENT as Comment
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{database_name}'
              AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        query_result = self._execute_pandas(sql)

        result = []
        for i in range(len(query_result)):
            result.append(
                {
                    "cid": i,
                    "name": query_result["Field"][i],
                    "type": query_result["Type"][i],
                    "nullable": query_result["Null"][i] == "YES",
                    "default_value": query_result["Default"][i],
                    "pk": query_result["Key"][i] == "PRI",
                    "comment": query_result["Comment"][i],
                }
            )
        return result

    # ==================== Database/Schema Management ====================

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of databases (MySQL uses schemas as databases)."""
        return super().get_schemas(catalog_name=catalog_name, include_sys=include_sys)

    @override
    def get_schemas(self, catalog_name: str = "", database_name: str = "", include_sys: bool = False) -> List[str]:
        """MySQL doesn't have separate schemas, return empty list."""
        return []

    @override
    def _sqlalchemy_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> Optional[str]:
        """Get schema name for SQLAlchemy Inspector (database name in MySQL)."""
        return database_name or self.database_name

    @override
    def do_switch_context(self, catalog_name: str = "", database_name: str = "", schema_name: str = ""):
        """Switch database context using USE statement."""
        if database_name:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {self._quote_identifier(database_name)}"))

    # ==================== Sample Data ====================

    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: TABLE_TYPE = "table",
    ) -> List[Dict[str, str]]:
        """Get sample rows from tables."""
        # Delegate to base class for unsupported table types (e.g., "full")
        if table_type == "full" or table_type not in METADATA_DICT:
            return super().get_sample_rows(
                tables=tables,
                top_n=top_n,
                catalog_name=catalog_name,
                database_name=database_name,
                schema_name=schema_name,
                table_type=table_type,
            )

        self.connect()
        database_name = database_name or self.database_name
        result = []

        # If specific tables provided, query those
        if tables:
            for table_name in tables:
                full_name = self.full_name(
                    catalog_name=catalog_name, database_name=database_name, table_name=table_name
                )
                sql = f"SELECT * FROM {full_name} LIMIT {top_n}"
                df = self._execute_pandas(sql)
                if not df.empty:
                    result.append(
                        {
                            "identifier": self.identifier(
                                catalog_name=catalog_name, database_name=database_name, table_name=table_name
                            ),
                            "catalog_name": catalog_name,
                            "database_name": database_name,
                            "schema_name": "",
                            "table_name": table_name,
                            "sample_rows": df.to_csv(index=False),
                        }
                    )
            return result

        # Otherwise get metadata and query all tables
        metadata = self._get_metadata(table_type, "", database_name)
        for meta in metadata:
            full_name = self.full_name(database_name=meta["database_name"], table_name=meta["table_name"])
            sql = f"SELECT * FROM {full_name} LIMIT {top_n}"
            df = self._execute_pandas(sql)
            if not df.empty:
                result.append(
                    {
                        "identifier": meta["identifier"],
                        "catalog_name": meta["catalog_name"],
                        "database_name": meta["database_name"],
                        "schema_name": "",
                        "table_name": meta["table_name"],
                        "sample_rows": df.to_csv(index=False),
                    }
                )
        return result

    # ==================== Utility Methods ====================

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Build fully-qualified table name."""
        if database_name:
            return f"`{database_name}`.`{table_name}`"
        return f"`{table_name}`"

    @override
    def _reset_filter_tables(
        self, tables: Optional[List[str]] = None, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[str]:
        """Reset filter tables with full names."""
        database_name = database_name or self.database_name
        return super()._reset_filter_tables(tables, "", database_name, "")
