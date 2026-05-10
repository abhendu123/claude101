import io
import logging
import uuid
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from config import RedshiftConfig

logger = logging.getLogger(__name__)


class RedshiftLoader:
    def __init__(self, config: RedshiftConfig):
        self.config = config
        self._engine = None

    def get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_string(),
                connect_args={"connect_timeout": self.config.connect_timeout},
                pool_pre_ping=True,
            )
        return self._engine

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",
        chunksize: int = 10_000,
        use_azure_staging: bool = False,
    ) -> None:
        target_schema = schema or self.config.schema
        if df.empty:
            logger.warning(f"Empty DataFrame — skipping load to {target_schema}.{table_name}")
            return

        if use_azure_staging and self.config.azure_storage_account and self.config.azure_container:
            self._stage_to_azure_blob(df, table_name, target_schema)

        self._load_direct(df, table_name, target_schema, if_exists, chunksize)

    def _load_direct(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: str,
        chunksize: int,
    ) -> None:
        engine = self.get_engine()
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        logger.info(f"Loaded {len(df):,} rows into {schema}.{table_name}")

    def _stage_to_azure_blob(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
    ) -> None:
        """Upload a copy of the DataFrame to Azure Blob Storage for audit/lineage."""
        from azure.storage.blob import BlobServiceClient

        blob_key = f"{self.config.azure_blob_prefix}/{schema}/{table_name}/{uuid.uuid4()}.csv"
        cfg = self.config

        if cfg.azure_connection_string:
            client = BlobServiceClient.from_connection_string(cfg.azure_connection_string)
        elif cfg.azure_account_key:
            client = BlobServiceClient(
                account_url=f"https://{cfg.azure_storage_account}.blob.core.windows.net",
                credential=cfg.azure_account_key,
            )
        elif cfg.azure_sas_token:
            client = BlobServiceClient(
                account_url=f"https://{cfg.azure_storage_account}.blob.core.windows.net",
                credential=cfg.azure_sas_token,
            )
        else:
            raise ValueError(
                "RedshiftConfig requires one of: azure_connection_string, "
                "azure_account_key, or azure_sas_token for Azure Blob staging."
            )

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        blob_client = client.get_blob_client(container=cfg.azure_container, blob=blob_key)
        blob_client.upload_blob(csv_bytes, overwrite=True)
        logger.info(
            f"Staged {len(df):,} rows to "
            f"https://{cfg.azure_storage_account}.blob.core.windows.net"
            f"/{cfg.azure_container}/{blob_key}"
        )

    def execute_sql(self, sql: str) -> None:
        with self.get_engine().begin() as conn:
            conn.execute(text(sql))
        logger.info("Executed raw SQL statement")