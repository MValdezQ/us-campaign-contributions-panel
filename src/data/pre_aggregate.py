"""Pre-aggregation utilities for campaign finance data.

This module implements the first five steps of the pre-aggregation stage as
outlined in ``documentation/pipeline.md``.  It provides helper
functions to capture ingestion metadata, clean incoming tables, prune
unneeded fields and aggregate records using DuckDB.
"""
#%%

from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

import duckdb
import pandas as pd

#%% Paths ---------------------------------------------------------------------
# Project root is two levels up from this file (src/data/ -> project root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
METADATA_DIR = PROJECT_ROOT / "data" / "metadata"
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# In-memory buffer for ingestion metadata records
_ingest_buffer: List[Dict[str, object]] = []


def capture_ingest_metadata(
    file_name: str,
    cycle: str,
    source_format: str,
    row_count: int,
) -> None:
    """Buffer metadata about an ingested file.

    Parameters
    ----------
    file_name: str
        Name of the ingested file.
    cycle: str
        Election cycle (e.g. ``"1990"``).
    source_format: str
        Format of the source file (``"parquet"`` or ``"csv"``).
    row_count: int
        Number of records ingested from the file.
    """
    _ingest_buffer.append(
        {
            "file_name": file_name,
            "cycle": cycle,
            "source_format": source_format,
            "row_count": row_count,
            "ingest_ts": datetime.utcnow().isoformat(),
        }
    )


def flush_ingest_metadata() -> Path:
    """Write buffered ingestion metadata to ``ingest_metadata.parquet``.

    Returns
    -------
    Path
        Location of the written Parquet file.
    """
    if not _ingest_buffer:
        return METADATA_DIR / "ingest_metadata.parquet"

    df = pd.DataFrame(_ingest_buffer)
    _ingest_buffer.clear()

    out_path = METADATA_DIR / "ingest_metadata.parquet"

    con = duckdb.connect()
    if out_path.exists():
        existing = con.execute(f"SELECT * FROM '{out_path}'").fetch_df()
        df = pd.concat([existing, df], ignore_index=True)

    con.register("meta", df)
    con.execute(f"COPY meta TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    con.close()
    return out_path


# Cleaning utilities --------------------------------------------------------

def normalize_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and lowercase key identifier fields.

    Columns normalised: ``PACID``, ``ContribID``, ``Contrib``, ``RecipID``, ``Orgname``,
    ``UltOrg``.  Missing columns are ignored.
    """
    cols = ["PACID", "ContribID", "Contrib", "RecipID", "Orgname", "UltOrg"]
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower()
    return df


def sanitize_amounts(
    df: pd.DataFrame, cycle: str, invalid_dir: Path | None = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parse ``Amount`` as numeric and log rows with parse errors.

    Parameters
    ----------
    df : pandas.DataFrame
        Input table with an ``Amount`` column.
    cycle : str
        Election cycle, used to name the invalid rows file.
    invalid_dir : Path, optional
        Directory to which invalid rows are written.  Defaults to
        ``data/metadata`` under the project root.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        The sanitised dataframe and the dataframe of invalid rows.
    """
    if invalid_dir is None:
        invalid_dir = METADATA_DIR
    invalid_dir.mkdir(parents=True, exist_ok=True)

    amounts = pd.to_numeric(df["Amount"], errors="coerce")
    invalid = df[amounts.isna()].copy()
    if not invalid.empty:
        invalid["reason"] = "parse_error"
        out_path = invalid_dir / f"invalid_amounts_{cycle}.parquet"
        con = duckdb.connect()
        con.register("invalid", invalid)
        con.execute(
            f"COPY invalid TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
        con.close()

    df = df[amounts.notna()].copy()
    df["Amount"] = amounts[amounts.notna()]
    return df, invalid


# Field pruning -------------------------------------------------------------

INDIV_PRUNE_FIELDS = [
    "FECTransID",
    "Date",
    "RecipID",
    "Street",
    "City",
    "State",
    "OtherID",
    "Gender",
    "Microfilm",
    "Occupation",
    "Employer",
    "Source",
]

PAC_PRUNE_FIELDS = ["FECRecID", "Date", "FECCandID"]


def drop_fields_indivs(df: pd.DataFrame) -> pd.DataFrame:
    """Drop irrelevant individual-contribution columns."""
    return df.drop(columns=[c for c in INDIV_PRUNE_FIELDS if c in df.columns])


def drop_fields_pacs(df: pd.DataFrame) -> pd.DataFrame:
    """Drop irrelevant PAC-contribution columns."""
    return df.drop(columns=[c for c in PAC_PRUNE_FIELDS if c in df.columns])


# Aggregation routines ------------------------------------------------------

def group_and_aggregate_indivs(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate individual contributions using DuckDB."""
    print(f"Starting aggregation of {len(df)} individual contribution records")
    print(f"Amount column dtype: {df['Amount'].dtype}")
    
    # Convert Amount to numeric if it's stored as string/object
    if df['Amount'].dtype == 'object':
        print("Converting Amount column from object to numeric...")
        df = df.copy()
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        # Remove rows where Amount conversion failed
        initial_count = len(df)
        df = df.dropna(subset=['Amount'])
        final_count = len(df)
        if initial_count != final_count:
            print(f"Removed {initial_count - final_count} rows with invalid Amount values")
        print(f"Amount column now has dtype: {df['Amount'].dtype}")
    
    con = duckdb.connect()
    con.register("indivs", df)
    query = """
        SELECT
            Cycle,
            ContribID,
            Contrib,
            Orgname,
            UltOrg,
            RealCode,
            Zip,
            RecipCode,
            Type,
            CmteID,
            SUM(Amount) AS total_amount,
            COUNT(*)   AS record_count,
            CURRENT_TIMESTAMP AS agg_ts
        FROM indivs
        GROUP BY Cycle, Contrib, ContribID, Orgname, UltOrg, RealCode,
                 Zip, RecipCode, Type, CmteID
    """
    try:
        result = con.execute(query).fetch_df()
        print(f"Individual contributions aggregation completed successfully. Result shape: {result.shape}")
    except Exception as e:
        print(f"Error in DuckDB aggregation: {e}")
        print(f"Data types after conversion:")
        print(df.dtypes)
        raise
    finally:
        con.close()
    return result


def group_and_aggregate_pacs(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate PAC contributions using DuckDB."""
    print(f"Starting aggregation of {len(df)} PAC contribution records")
    print(f"Amount column dtype: {df['Amount'].dtype}")
    
    # Convert Amount to numeric if it's stored as string/object
    if df['Amount'].dtype == 'object':
        print("Converting Amount column from object to numeric...")
        df = df.copy()
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        # Remove rows where Amount conversion failed
        initial_count = len(df)
        df = df.dropna(subset=['Amount'])
        final_count = len(df)
        if initial_count != final_count:
            print(f"Removed {initial_count - final_count} rows with invalid Amount values")
        print(f"Amount column now has dtype: {df['Amount'].dtype}")
    
    con = duckdb.connect()
    con.register("pacs", df)
    query = """
        SELECT
            Cycle,
            PACID,
            CandID,
            RealCode,
            Type,
            DI,
            SUM(Amount) AS total_amount,
            COUNT(*)   AS record_count,
            CURRENT_TIMESTAMP AS agg_ts
        FROM pacs
        GROUP BY Cycle, PACID, CandID, RealCode, Type, DI
    """
    try:
        result = con.execute(query).fetch_df()
        print(f"PAC contributions aggregation completed successfully. Result shape: {result.shape}")
    except Exception as e:
        print(f"Error in DuckDB aggregation: {e}")
        print(f"Data types after conversion:")
        print(df.dtypes)
        raise
    finally:
        con.close()
    return result

# Large-file DuckDB-native aggregation (no pandas load) -------------------

_LARGE_FILE_THRESHOLD_BYTES = 1 * 1024 ** 3  # 1 GB

# Columns present in indivs that must NOT appear in indivs_agg
_INDIV_DROP = set(INDIV_PRUNE_FIELDS)
# Columns present in pacs that must NOT appear in pacs_agg
_PAC_DROP = set(PAC_PRUNE_FIELDS)


def aggregate_indivs_from_parquet_duckdb(parquet_path: Path, cycle: str) -> pd.DataFrame:
    """Aggregate indivs directly from a parquet file via DuckDB.

    Bypasses pandas entirely so the full file is never loaded into RAM.
    Applies identifier normalisation (LOWER/TRIM) and amount sanitisation
    (TRY_CAST) in SQL, mirrors what the pandas pipeline does.

    Invalid-amount rows are written to ``data/metadata/invalid_amounts_{cycle}.parquet``
    to keep parity with :func:`sanitize_amounts`.
    """
    path_str = str(parquet_path).replace("\\", "/")

    con = duckdb.connect()

    # Write out invalid rows so we have audit parity with sanitize_amounts
    invalid_out = METADATA_DIR / f"invalid_amounts_{cycle}.parquet"
    con.execute(f"""
        COPY (
            SELECT *, 'parse_error' AS reason
            FROM '{path_str}'
            WHERE TRY_CAST(Amount AS DOUBLE) IS NULL
              AND Amount IS NOT NULL
        ) TO '{str(invalid_out).replace(chr(92), "/")}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    result = con.execute(f"""
        SELECT
            Cycle,
            LOWER(TRIM(ContribID))  AS ContribID,
            LOWER(TRIM(Contrib))    AS Contrib,
            LOWER(TRIM(Orgname))    AS Orgname,
            LOWER(TRIM(UltOrg))     AS UltOrg,
            RealCode,
            Zip,
            RecipCode,
            Type,
            CmteID,
            SUM(TRY_CAST(Amount AS DOUBLE)) AS total_amount,
            COUNT(*)                        AS record_count,
            CURRENT_TIMESTAMP               AS agg_ts
        FROM '{path_str}'
        WHERE TRY_CAST(Amount AS DOUBLE) IS NOT NULL
        GROUP BY
            Cycle, ContribID, Contrib, Orgname, UltOrg,
            RealCode, Zip, RecipCode, Type, CmteID
    """).fetch_df()

    con.close()
    print(f"DuckDB-native indivs aggregation complete. Result shape: {result.shape}")
    return result


def aggregate_pacs_from_parquet_duckdb(parquet_path: Path, cycle: str) -> pd.DataFrame:
    """Aggregate pacs directly from a parquet file via DuckDB.

    Mirrors :func:`aggregate_indivs_from_parquet_duckdb` for the PAC table.
    """
    path_str = str(parquet_path).replace("\\", "/")

    con = duckdb.connect()

    invalid_out = METADATA_DIR / f"invalid_amounts_pacs_{cycle}.parquet"
    con.execute(f"""
        COPY (
            SELECT *, 'parse_error' AS reason
            FROM '{path_str}'
            WHERE TRY_CAST(Amount AS DOUBLE) IS NULL
              AND Amount IS NOT NULL
        ) TO '{str(invalid_out).replace(chr(92), "/")}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    result = con.execute(f"""
        SELECT
            Cycle,
            LOWER(TRIM(PACID))  AS PACID,
            CandID,
            RealCode,
            Type,
            DI,
            SUM(TRY_CAST(Amount AS DOUBLE)) AS total_amount,
            COUNT(*)                        AS record_count,
            CURRENT_TIMESTAMP               AS agg_ts
        FROM '{path_str}'
        WHERE TRY_CAST(Amount AS DOUBLE) IS NOT NULL
        GROUP BY Cycle, PACID, CandID, RealCode, Type, DI
    """).fetch_df()

    con.close()
    print(f"DuckDB-native pacs aggregation complete. Result shape: {result.shape}")
    return result


#%%
if __name__ == "__main__":
    import argparse as _argparse

    _parser = _argparse.ArgumentParser(description="Pre-aggregate campaign finance data by cycle.")
    _parser.add_argument(
        "--cycles",
        nargs="*",
        default=None,
        help="Cycles to process (e.g. --cycles 20 22). Defaults to all cycles.",
    )
    _args = _parser.parse_args()

    # Expand functionality to pre-aggregate all campaign cycles
    clean_dir = PROJECT_ROOT / "data" / "clean"
    derived_dir = PROJECT_ROOT / "data" / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    all_cycles = ["90", "92", "94", "96", "98", "00", "02", "04", "06", "08", "10", "12", "14", "16", "18", "20", "22"]
    campaign_cycles = _args.cycles if _args.cycles else all_cycles

    for cycle in campaign_cycles:
        print(f"Processing campaign cycle: {cycle}")

        cycle_dir = clean_dir / cycle
        indiv_file = cycle_dir / "indivs.parquet"
        pac_file = cycle_dir / "pacs.parquet"
        cycle_derived_dir = derived_dir / cycle
        cycle_derived_dir.mkdir(parents=True, exist_ok=True)

        # Aggregate individual contributions
        if indiv_file.exists():
            indiv_out_path = cycle_derived_dir / "indivs_agg.parquet"
            file_size = indiv_file.stat().st_size
            print(f"  indivs.parquet size: {file_size / 1024**3:.2f} GB")
            if file_size > _LARGE_FILE_THRESHOLD_BYTES:
                print("  Large file — using DuckDB-native aggregation (no pandas load)")
                agg_indivs = aggregate_indivs_from_parquet_duckdb(indiv_file, cycle)
            else:
                df_indivs = pd.read_parquet(indiv_file)
                df_indivs = normalize_identifiers(df_indivs)
                df_indivs, _ = sanitize_amounts(df_indivs, cycle)
                df_indivs = drop_fields_indivs(df_indivs)
                agg_indivs = group_and_aggregate_indivs(df_indivs)
            agg_indivs.to_parquet(indiv_out_path, compression="snappy")
            print(f"Saved aggregated individual contributions to {indiv_out_path}")

        # Aggregate PAC contributions
        if pac_file.exists():
            pac_out_path = cycle_derived_dir / "pacs_agg.parquet"
            file_size = pac_file.stat().st_size
            print(f"  pacs.parquet size: {file_size / 1024**3:.2f} GB")
            if file_size > _LARGE_FILE_THRESHOLD_BYTES:
                print("  Large file — using DuckDB-native aggregation (no pandas load)")
                agg_pacs = aggregate_pacs_from_parquet_duckdb(pac_file, cycle)
            else:
                df_pacs = pd.read_parquet(pac_file)
                df_pacs = normalize_identifiers(df_pacs)
                df_pacs, _ = sanitize_amounts(df_pacs, cycle)
                df_pacs = drop_fields_pacs(df_pacs)
                agg_pacs = group_and_aggregate_pacs(df_pacs)
            agg_pacs.to_parquet(pac_out_path, compression="snappy")
            print(f"Saved aggregated PAC contributions to {pac_out_path}")

