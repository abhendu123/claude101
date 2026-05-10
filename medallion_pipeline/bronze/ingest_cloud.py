import io
import logging
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from config import RedshiftConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cloud source configuration dataclasses
# ---------------------------------------------------------------------------

@dataclass
class S3SourceConfig:
    bucket: str
    key: str                          # object key or prefix
    region: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None


@dataclass
class AzureBlobConfig:
    account_name: str
    container: str
    blob_name: str
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    sas_token: Optional[str] = None


@dataclass
class GCSConfig:
    bucket: str
    blob_name: str
    project: Optional[str] = None
    credentials_path: Optional[str] = None  # path to service-account JSON


@dataclass
class FileReadOptions:
    """Common read options shared across all cloud file ingestion functions."""
    file_format: str = "csv"               # 'csv', 'excel', 'json', 'parquet', 'orc'
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    encoding: str = "utf-8"
    header: int = 0
    skiprows: Optional[int] = None
    nrows: Optional[int] = None
    sheet_name: int = 0                    # Excel only
    orient: Optional[str] = None           # JSON only
    lines: bool = False                    # JSON NDJSON only
    dtype: Optional[dict] = None
    na_values: Optional[List[str]] = None
    parse_dates: Optional[List[str]] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_bytes(data: bytes, options: FileReadOptions) -> pd.DataFrame:
    fmt = options.file_format.lower()
    if fmt == "csv":
        kwargs = {
            "sep": options.delimiter,
            "quotechar": options.quotechar,
            "encoding": options.encoding,
            "header": options.header,
        }
        if options.escapechar:
            kwargs["escapechar"] = options.escapechar
        if options.skiprows:
            kwargs["skiprows"] = options.skiprows
        if options.nrows:
            kwargs["nrows"] = options.nrows
        if options.dtype:
            kwargs["dtype"] = options.dtype
        if options.na_values:
            kwargs["na_values"] = options.na_values
        if options.parse_dates:
            kwargs["parse_dates"] = options.parse_dates
        return pd.read_csv(io.BytesIO(data), **kwargs)

    if fmt in ("xlsx", "xls", "excel"):
        return pd.read_excel(
            io.BytesIO(data),
            sheet_name=options.sheet_name,
            header=options.header,
            skiprows=options.skiprows,
            nrows=options.nrows,
            dtype=options.dtype,
            na_values=options.na_values,
            parse_dates=options.parse_dates,
        )

    if fmt == "json":
        return pd.read_json(
            io.BytesIO(data),
            orient=options.orient,
            lines=options.lines,
            dtype=options.dtype,
        )

    if fmt == "parquet":
        return pd.read_parquet(io.BytesIO(data))

    if fmt == "orc":
        return pd.read_orc(io.BytesIO(data))

    raise ValueError(f"Unsupported file_format: '{options.file_format}'")


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def ingest_s3(
    source_config: S3SourceConfig,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    read_options: Optional[FileReadOptions] = None,
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest a file from Amazon S3 into Redshift.

    Args:
        source_config: S3 bucket, key, region, and optional credentials.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        read_options: File format and parsing options.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    import boto3

    opts = read_options or FileReadOptions()
    session_kwargs = {}
    if source_config.profile_name:
        session_kwargs["profile_name"] = source_config.profile_name
    session = boto3.Session(
        aws_access_key_id=source_config.aws_access_key_id,
        aws_secret_access_key=source_config.aws_secret_access_key,
        aws_session_token=source_config.aws_session_token,
        region_name=source_config.region,
        **session_kwargs,
    )
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=source_config.bucket, Key=source_config.key)
    data = obj["Body"].read()
    logger.info(f"Downloaded s3://{source_config.bucket}/{source_config.key} ({len(data):,} bytes)")

    df = _read_bytes(data, opts)
    logger.info(f"Parsed {len(df):,} rows")
    RedshiftLoader(redshift_config).load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


# ---------------------------------------------------------------------------
# Azure Blob Storage
# ---------------------------------------------------------------------------

def ingest_azure_blob(
    source_config: AzureBlobConfig,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    read_options: Optional[FileReadOptions] = None,
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest a file from Azure Blob Storage into Redshift.

    Args:
        source_config: Azure account, container, blob name, and credentials.
            Provide one of: account_key, connection_string, or sas_token.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        read_options: File format and parsing options.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    from azure.storage.blob import BlobServiceClient

    opts = read_options or FileReadOptions()
    cfg = source_config

    if cfg.connection_string:
        client = BlobServiceClient.from_connection_string(cfg.connection_string)
    elif cfg.account_key:
        client = BlobServiceClient(
            account_url=f"https://{cfg.account_name}.blob.core.windows.net",
            credential=cfg.account_key,
        )
    elif cfg.sas_token:
        client = BlobServiceClient(
            account_url=f"https://{cfg.account_name}.blob.core.windows.net",
            credential=cfg.sas_token,
        )
    else:
        raise ValueError("AzureBlobConfig requires account_key, connection_string, or sas_token")

    blob_client = client.get_blob_client(container=cfg.container, blob=cfg.blob_name)
    data = blob_client.download_blob().readall()
    logger.info(f"Downloaded blob {cfg.container}/{cfg.blob_name} ({len(data):,} bytes)")

    df = _read_bytes(data, opts)
    logger.info(f"Parsed {len(df):,} rows")
    RedshiftLoader(redshift_config).load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


# ---------------------------------------------------------------------------
# Google Cloud Storage
# ---------------------------------------------------------------------------

def ingest_gcs(
    source_config: GCSConfig,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    read_options: Optional[FileReadOptions] = None,
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest a file from Google Cloud Storage into Redshift.

    Args:
        source_config: GCS bucket, blob name, optional project and
            credentials_path (service-account JSON).
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        read_options: File format and parsing options.
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    from google.cloud import storage
    from google.oauth2 import service_account

    opts = read_options or FileReadOptions()
    cfg = source_config

    if cfg.credentials_path:
        creds = service_account.Credentials.from_service_account_file(cfg.credentials_path)
        gcs = storage.Client(project=cfg.project, credentials=creds)
    else:
        gcs = storage.Client(project=cfg.project)

    bucket = gcs.bucket(cfg.bucket)
    blob = bucket.blob(cfg.blob_name)
    data = blob.download_as_bytes()
    logger.info(f"Downloaded gs://{cfg.bucket}/{cfg.blob_name} ({len(data):,} bytes)")

    df = _read_bytes(data, opts)
    logger.info(f"Parsed {len(df):,} rows")
    RedshiftLoader(redshift_config).load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tgt = RedshiftConfig.from_env()
    opts = FileReadOptions(file_format="csv", delimiter=",", encoding="utf-8", parse_dates=["created_at"])

    # S3
    ingest_s3(
        source_config=S3SourceConfig(bucket="my-bucket", key="data/sample.csv", region="us-east-1"),
        redshift_config=tgt,
        target_table="raw_s3_data",
        target_schema="bronze",
        read_options=opts,
        if_exists="append",
    )

    # Azure Blob
    ingest_azure_blob(
        source_config=AzureBlobConfig(
            account_name="mystorageaccount",
            container="raw-data",
            blob_name="sample.csv",
            account_key="<account-key>",
        ),
        redshift_config=tgt,
        target_table="raw_azure_data",
        target_schema="bronze",
        read_options=opts,
        if_exists="append",
    )

    # GCS
    ingest_gcs(
        source_config=GCSConfig(
            bucket="my-gcs-bucket",
            blob_name="data/sample.csv",
            project="my-gcp-project",
            credentials_path=r"C:\keys\service_account.json",
        ),
        redshift_config=tgt,
        target_table="raw_gcs_data",
        target_schema="bronze",
        read_options=opts,
        if_exists="append",
    )