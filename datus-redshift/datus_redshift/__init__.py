"""
Redshift adapter for Datus Agent.

This module provides the RedshiftConnector class that enables Datus to connect
to Amazon Redshift databases and perform queries, metadata retrieval, and
other database operations.
"""

# Import the connector registry from Datus to register our connector
from datus.tools.db_tools import connector_registry

# Import our configuration and connector classes
from .config import RedshiftConfig
from .connector import RedshiftConnector

# Version of this adapter
__version__ = "0.1.0"

# Export these classes so they can be imported with: from datus_redshift import RedshiftConnector, RedshiftConfig
__all__ = ["RedshiftConnector", "RedshiftConfig", "register"]


def register():
    """
    Register Redshift connector with Datus registry.
    
    This function registers the RedshiftConnector class with the Datus connector
    registry under the name "redshift". This allows Datus to automatically discover
    and use this connector when configured to connect to Redshift databases.
    """
    # Register our connector with the name "redshift"
    # Now users can specify dialect="redshift" in their Datus configuration
    connector_registry.register("redshift", RedshiftConnector)


# Auto-register when this module is imported
# This means the connector is automatically available when you install this package
register()

