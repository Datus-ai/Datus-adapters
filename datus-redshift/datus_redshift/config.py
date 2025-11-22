
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RedshiftConfig(BaseModel):
    """
    Configuration class for Amazon Redshift database connections.
    
    This class uses Pydantic for validation and configuration management.
    It defines all the parameters needed to connect to a Redshift cluster.
    """

    # ConfigDict settings:
    # - extra="forbid": Prevents additional fields not defined in the model
    # - populate_by_name=True: Allows using field aliases (like 'schema' for 'schema_name')
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # Required fields (must be provided when creating a RedshiftConfig instance)

    # The Redshift cluster endpoint (e.g., "my-cluster.abc123.us-west-2.redshift.amazonaws.com")
    host: str = Field(..., description="Redshift cluster endpoint")

    # The username for authentication
    
    username: str = Field(..., description="Redshift username")

    # The password for authentication (optional when using IAM authentication)
    
    password: Optional[str] = Field(default=None, description="Redshift password (required unless using IAM authentication)")

    # Optional fields (have default values)

    # Port number (default 5439 is the standard Redshift port)
    
    
    port: int = Field(default=5439, description="Redshift server port")

    # Default database to connect to (optional - if None, connects to default database)
    
    database: Optional[str] = Field(default=None, description="Default database name")

    # Default schema within the database (optional - if None, uses 'public' schema)
    # The alias="schema" allows you to pass "schema" instead of "schema_name" when creating the config
    
    schema_name: Optional[str] = Field(default=None, alias="schema", description="Default schema name")

    # Connection timeout in seconds (how long to wait before giving up on connection)
    
    timeout_seconds: int = Field(default=30, description="Connection timeout in seconds")

    # SSL mode for secure connections (default is "verify-ca" for production security)
    # Options: "disable", "allow", "prefer", "require", "verify-ca", "verify-full"
    
    ssl: bool = Field(default=True, description="Enable SSL connection")

    # IAM authentication (for AWS IAM-based authentication instead of password)
    
    iam: bool = Field(default=False, description="Use IAM authentication")

    # Cluster identifier (needed for IAM authentication)
    
    cluster_identifier: Optional[str] = Field(default=None, description="Redshift cluster identifier for IAM auth")

    # AWS region (needed for IAM authentication)
    
    region: Optional[str] = Field(default=None, description="AWS region for IAM auth")

    # AWS credentials (needed for IAM authentication)
    
    access_key_id: Optional[str] = Field(default=None, description="AWS access key ID for IAM auth")
    secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key for IAM auth")
    
    @model_validator(mode='after')
    def validate_authentication(self):
        """
        Validate that either password or IAM authentication is properly configured.
        
        Rules:
        - If IAM is False, password must be provided
        - If IAM is True, password is optional (IAM auth will be used)
        
        Raises:
            ValueError: If validation fails
        """
        if not self.iam and not self.password:
            raise ValueError(
                "Password is required when IAM authentication is disabled. "
                "Either provide a password or set iam=True for IAM authentication."
            )
        
        return self
