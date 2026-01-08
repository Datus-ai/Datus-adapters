# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from .config import ClickHouseConfig
from .connector import ClickHouseConnector

__version__ = "0.1.0"
__all__ = ["ClickHouseConnector", "ClickHouseConfig", "register"]


def register():
    """Register ClickHouse connector with Datus registry."""
    from datus.tools.db_tools import connector_registry

    connector_registry.register("clickhouse", ClickHouseConnector)
