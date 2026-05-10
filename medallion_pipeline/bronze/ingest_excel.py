import logging
from typing import List, Optional, Union

import pandas as pd

from config import RedshiftConfig
from redshift_loader import RedshiftLoader

logger = logging.getLogger(__name__)


def ingest_excel(
    file_path: str,
    redshift_config: RedshiftConfig,
    target_table: str,
    target_schema: Optional[str] = None,
    # Sheet selection
    sheet_name: Union[str, int, List] = 0,
    # Row / column controls
    header: Union[int, List[int]] = 0,
    skiprows: Optional[int] = None,
    skipfooter: int = 0,
    nrows: Optional[int] = None,
    usecols: Optional[Union[str, List]] = None,
    # Type handling
    dtype: Optional[dict] = None,
    na_values: Optional[List[str]] = None,
    keep_default_na: bool = True,
    parse_dates: Optional[List[str]] = None,
    # Engine
    engine: Optional[str] = None,  # 'openpyxl', 'xlrd', 'odf', 'pyxlsb'
    # Redshift target options
    if_exists: str = "append",
    use_azure_staging: bool = False,
    load_chunksize: int = 10_000,
) -> None:
    """Ingest an Excel file (.xlsx / .xls / .ods) into Redshift.

    Args:
        file_path: Absolute path to the Excel file.
        redshift_config: Redshift connection configuration.
        target_table: Destination table name in Redshift.
        target_schema: Destination schema; falls back to redshift_config.schema.
        sheet_name: Sheet index or name; use a list to load multiple sheets.
        header: Row(s) to use as column names.
        skiprows: Rows to skip at the top of the sheet.
        skipfooter: Rows to skip at the bottom of the sheet.
        nrows: Maximum rows to read.
        usecols: Column letters/names/indices to load (e.g. 'A:D' or ['col1']).
        dtype: Dict mapping column names to data types.
        na_values: Additional strings to treat as NaN.
        keep_default_na: Whether to include the default NaN values.
        parse_dates: Columns to parse as dates.
        engine: Parsing engine ('openpyxl' for .xlsx, 'xlrd' for .xls).
        if_exists: 'append' or 'replace'.
        use_azure_staging: Use S3 staging + COPY instead of direct INSERT.
        load_chunksize: Rows per batch for direct INSERT.
    """
    loader = RedshiftLoader(redshift_config)

    read_kwargs = {
        "sheet_name": sheet_name,
        "header": header,
        "skipfooter": skipfooter,
        "keep_default_na": keep_default_na,
    }
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
    if engine is not None:
        read_kwargs["engine"] = engine

    result = pd.read_excel(file_path, **read_kwargs)

    # When sheet_name is a list or None, read_excel returns a dict
    if isinstance(result, dict):
        first = True
        for sheet, df in result.items():
            tbl = f"{target_table}_{sheet}" if len(result) > 1 else target_table
            logger.info(f"Sheet '{sheet}': {len(df):,} rows -> {tbl}")
            loader.load_dataframe(
                df,
                tbl,
                target_schema,
                if_exists if first else "append",
                load_chunksize,
                use_azure_staging,
            )
            first = False
    else:
        logger.info(f"Read {len(result):,} rows from {file_path}")
        loader.load_dataframe(result, target_table, target_schema, if_exists, load_chunksize, use_azure_staging)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = RedshiftConfig.from_env()
    ingest_excel(
        file_path=r"C:\data\sample.xlsx",
        redshift_config=cfg,
        target_table="raw_excel_data",
        target_schema="bronze",
        sheet_name=0,
        skiprows=1,
        usecols="A:F",
        parse_dates=["date_column"],
        engine="openpyxl",
        if_exists="append",
    )