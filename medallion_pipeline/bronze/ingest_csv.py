import logging
from typing import List, Optional, Union

import pandas as pd

from config import RedshiftConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)


def ingest_csv(
    file_path: str,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # Delimiters & quoting
    delimiter: str = ",",
    quotechar: str = '"',
    escapechar: Optional[str] = None,
    doublequote: bool = True,
    # Row / column controls
    header: Union[int, str] = "infer",
    skiprows: Optional[int] = None,
    skipfooter: int = 0,
    nrows: Optional[int] = None,
    usecols: Optional[List[str]] = None,
    # Encoding & whitespace
    encoding: str = "utf-8",
    skipinitialspace: bool = False,
    # Type handling
    dtype: Optional[dict] = None,
    na_values: Optional[List[str]] = None,
    keep_default_na: bool = True,
    parse_dates: Optional[List[str]] = None,
    dayfirst: bool = False,
    # Chunked loading
    read_chunksize: Optional[int] = None,
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest a CSV file into Redshift.

    Args:
        file_path: Absolute path to the CSV file.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        delimiter: Column separator character.
        quotechar: Character used to quote fields.
        escapechar: Character used to escape the delimiter inside a field.
        doublequote: Whether quotechar inside a field is doubled.
        header: Row number(s) to use as column names.
        skiprows: Number of rows to skip at the start of the file.
        skipfooter: Number of rows to skip at the end of the file.
        nrows: Maximum number of rows to read.
        usecols: Subset of columns to load.
        encoding: File encoding (e.g. 'utf-8', 'latin-1').
        skipinitialspace: Skip spaces after delimiter.
        dtype: Dict mapping column names to data types.
        na_values: Additional strings to treat as NaN.
        keep_default_na: Whether to include the default NaN values.
        parse_dates: List of columns to parse as dates.
        dayfirst: Treat day as first in ambiguous date strings.
        read_chunksize: Rows per chunk when reading large files.
        if_exists: 'append' or 'replace' — behaviour when table exists.
        use_azure_staging: Stage a copy to Azure Blob Storage before loading to Redshift.
        load_chunksize: Rows per batch for direct INSERT.
    """
    loader = RedshiftLoader(redshift_config)

    read_kwargs = {
        "sep": delimiter,
        "quotechar": quotechar,
        "doublequote": doublequote,
        "encoding": encoding,
        "header": header,
        "skipfooter": skipfooter,
        "skipinitialspace": skipinitialspace,
        "keep_default_na": keep_default_na,
        "dayfirst": dayfirst,
    }
    if escapechar is not None:
        read_kwargs["escapechar"] = escapechar
    if skiprows is not None:
        read_kwargs["skiprows"] = skiprows
    if nrows is not None:
        read_kwargs["nrows"] = nrows
    if usecols is not None:
        read_kwargs["usecols"] = usecols
    if dtype is not None:
        read_kwargs["dtype"] = dtype
    if na_values is not None:
        read_kwargs["na_values"] = na_values
    if parse_dates is not None:
        read_kwargs["parse_dates"] = parse_dates

    first_chunk = True
    if read_chunksize:
        for chunk in pd.read_csv(file_path, chunksize=read_chunksize, **read_kwargs):
            loader.load_dataframe(
                chunk,
                target_table,
                target_schema,
                if_exists if first_chunk else "append",
                load_chunksize,
                use_azure_staging,
            )
            first_chunk = False
    else:
        df = pd.read_csv(file_path, **read_kwargs)
        logger.info(f"Read {len(df):,} rows from {file_path}")
        loader.load_dataframe(df, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = RedshiftConfig.from_env()
    ingest_csv(
        file_path=r"C:\data\sample.csv",
        redshift_config=cfg,
        target_table="raw_csv_data",
        target_schema="bronze",
        delimiter=",",
        escapechar="\\",
        encoding="utf-8",
        skiprows=0,
        nrows=None,
        parse_dates=["created_at"],
        if_exists="append",
    )