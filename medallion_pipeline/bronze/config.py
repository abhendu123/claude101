import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RedshiftConfig:
    host: str = ""
    port: int = 5439
    database: str = ""
    schema: str = "public"
    user: str = ""
    password: str = ""
    # Azure Blob Storage staging (used for large-volume loads)
    azure_storage_account: Optional[str] = None
    azure_container: Optional[str] = None
    azure_blob_prefix: str = "staging"
    azure_account_key: Optional[str] = None
    azure_connection_string: Optional[str] = None
    azure_sas_token: Optional[str] = None
    connect_timeout: int = 30

    @classmethod
    def from_env(cls) -> "RedshiftConfig":
        return cls(
            host=os.getenv("REDSHIFT_HOST", ""),
            port=int(os.getenv("REDSHIFT_PORT", "5439")),
            database=os.getenv("REDSHIFT_DATABASE", ""),
            schema=os.getenv("REDSHIFT_SCHEMA", "public"),
            user=os.getenv("REDSHIFT_USER", ""),
            password=os.getenv("REDSHIFT_PASSWORD", ""),
            azure_storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            azure_container=os.getenv("AZURE_CONTAINER"),
            azure_blob_prefix=os.getenv("AZURE_BLOB_PREFIX", "staging"),
            azure_account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
            azure_connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            azure_sas_token=os.getenv("AZURE_SAS_TOKEN"),
        )

    def connection_string(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class SourceDBConfig:
    host: str = ""
    port: int = 0
    database: str = ""
    schema: str = ""
    user: str = ""
    password: str = ""
    driver: Optional[str] = None
    service_name: Optional[str] = None  # Oracle
    sid: Optional[str] = None           # Oracle
    connect_timeout: int = 30
    extra_params: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str) -> "SourceDBConfig":
        return cls(
            host=os.getenv(f"{prefix}_HOST", ""),
            port=int(os.getenv(f"{prefix}_PORT", "0")),
            database=os.getenv(f"{prefix}_DATABASE", ""),
            schema=os.getenv(f"{prefix}_SCHEMA", ""),
            user=os.getenv(f"{prefix}_USER", ""),
            password=os.getenv(f"{prefix}_PASSWORD", ""),
            driver=os.getenv(f"{prefix}_DRIVER"),
            service_name=os.getenv(f"{prefix}_SERVICE_NAME"),
            sid=os.getenv(f"{prefix}_SID"),
        )