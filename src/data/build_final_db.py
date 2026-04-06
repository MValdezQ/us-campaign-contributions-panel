"""Build the final analytical database (Stage 7).

This script produces output/contributions.parquet by:
1. Processing indivs_agg_enriched and pacs_agg_enriched for each cycle
2. Joining to committees and candidates for recipient metadata
3. Filtering double-counted flows (INDIV→PAC, PAC→PAC) to ensure a clean expenditure panel
4. Deriving 'nature' from RealCode (indivs) and RecipCode (PACs)
5. Unioning all cycles into a single, high-resolution panel

This database serves as the authoritative object for multi-cycle political economy research.
"""

from pathlib import Path
from typing import Dict
import duckdb
import pandas as pd

# Project root is two levels up from this file (src/data/ -> project root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# All campaign cycles (zero-padded 2-digit strings)
ALL_CYCLES = ["90", "92", "94", "96", "98", "00", "02", "04", "06", "08", "10", "12", "14", "16", "18", "20", "22"]


def process_indivs(cycle: str) -> tuple[int, float, int, float]:
    """Process individual contributions for a single cycle.

    Filters INDIV→PAC flows, joins committees and candidates, derives nature.
    Returns: (rows_kept, $ kept, rows_filtered_indiv_pac, $ filtered_indiv_pac)
    """
    cycle_dir = DATA_DIR / "clean" / cycle
    indivs_path = str((DATA_DIR / "derived" / cycle / "indivs_agg_enriched.parquet")).replace("\\", "/")
    cmte_path = str((cycle_dir / "committees.parquet")).replace("\\", "/")
    cand_path = str((cycle_dir / "candidates.parquet")).replace("\\", "/")

    con = duckdb.connect()

    # Compute filtered row counts for logging
    filtered_stats = con.execute(f"""
        SELECT
            COUNT(*) as filtered_rows,
            SUM(total_amount) as filtered_amount
        FROM '{indivs_path}'
        WHERE UPPER(TRIM(RecipCode)) LIKE 'P%' OR UPPER(TRIM(RecipCode)) LIKE 'O%'
    """).fetch_df()

    filtered_rows = int(filtered_stats.iloc[0, 0]) if filtered_stats.iloc[0, 0] is not None else 0
    filtered_amount = float(filtered_stats.iloc[0, 1]) if filtered_stats.iloc[0, 1] is not None else 0.0

    # Main processing: filter, join, derive, and write to temp parquet
    tmp_path = str((DATA_DIR / "derived" / cycle / "indivs_final_tmp.parquet")).replace("\\", "/")

    con.execute(f"""
        COPY (
            WITH cand_deduped AS (
                -- Pick first candidate per CandID to avoid row multiplication
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY CandID ORDER BY CandID) as rn
                    FROM '{cand_path}'
                ) WHERE rn = 1
            )
            SELECT
                CAST({cycle} AS INT32) as cycle,
                'indiv' as source,
                i.org_id,
                i.Orgname as orgname,
                i.naics3,
                i.naics3_name,
                i.naics_source,
                CASE
                    WHEN UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) = 'J' THEN 'I'
                    WHEN UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) = 'Z' THEN 'P'
                    WHEN UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) = 'L' THEN 'L'
                    WHEN UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) = 'X' THEN 'O'
                    WHEN UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) = 'Y' THEN 'U'
                    ELSE 'B'
                END as nature,
                'D' as DI,
                i.total_amount,
                i.CmteID as recip_id,
                COALESCE(cnd.Name, c.PACShort) as recip_name,
                COALESCE(cnd.Party, c.Party) as recip_party,
                cnd.DistIDCurr as recip_seat,
                cnd.CRPICO as recip_incumbent
            FROM '{indivs_path}' i
            LEFT JOIN '{cmte_path}' c
                ON UPPER(TRIM(i.CmteID)) = UPPER(TRIM(c.CmteID))
            LEFT JOIN cand_deduped cnd
                ON UPPER(TRIM(c.RecipID)) = UPPER(TRIM(cnd.CandID))
            WHERE
                -- Non-business contributions (ideological J*, party Z*, labor L*, public/nonprofit X*, unknown Y*):
                -- always keep regardless of recipient — no double-counting risk since naics3=NULL by design.
                UPPER(SUBSTRING(COALESCE(i.RealCode, ''), 1, 1)) IN ('J', 'Z', 'L', 'X', 'Y')
                OR
                -- Business contributions: drop INDIV->PAC/outside-group flows to prevent double-counting
                -- with the PAC-side data (the PAC's contribution to the candidate is already captured there).
                NOT (UPPER(TRIM(i.RecipCode)) LIKE 'P%' OR UPPER(TRIM(i.RecipCode)) LIKE 'O%')
        ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    # Get row counts of kept records
    kept_stats = con.execute(f"""
        SELECT COUNT(*), SUM(total_amount)
        FROM '{tmp_path}'
    """).fetch_df()

    kept_rows = int(kept_stats.iloc[0, 0])
    kept_amount = float(kept_stats.iloc[0, 1])

    con.close()

    return kept_rows, kept_amount, filtered_rows, filtered_amount


def process_pacs(cycle: str) -> tuple[int, float, int, float]:
    """Process PAC contributions for a single cycle.

    Filters PAC→PAC transfers (Type='24A'), joins committees and candidates, derives nature.
    Returns: (rows_kept, $ kept, rows_filtered_pac_pac, $ filtered_pac_pac)
    """
    cycle_dir = DATA_DIR / "clean" / cycle
    pacs_path = str((DATA_DIR / "derived" / cycle / "pacs_agg_enriched.parquet")).replace("\\", "/")
    cmte_path = str((cycle_dir / "committees.parquet")).replace("\\", "/")
    cand_path = str((cycle_dir / "candidates.parquet")).replace("\\", "/")

    con = duckdb.connect()

    # Compute filtered row counts for logging
    filtered_stats = con.execute(f"""
        SELECT
            COUNT(*) as filtered_rows,
            SUM(total_amount) as filtered_amount
        FROM '{pacs_path}'
        WHERE Type = '24A'
    """).fetch_df()

    filtered_rows = int(filtered_stats.iloc[0, 0]) if filtered_stats.iloc[0, 0] is not None else 0
    filtered_amount = float(filtered_stats.iloc[0, 1]) if filtered_stats.iloc[0, 1] is not None else 0.0

    # Main processing: filter, join, derive, and write to temp parquet
    tmp_path = str((DATA_DIR / "derived" / cycle / "pacs_final_tmp.parquet")).replace("\\", "/")

    con.execute(f"""
        COPY (
            WITH cand_deduped AS (
                -- Pick first candidate per CandID to avoid row multiplication
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY CandID ORDER BY CandID) as rn
                    FROM '{cand_path}'
                ) WHERE rn = 1
            )
            SELECT
                CAST({cycle} AS INT32) as cycle,
                'pac' as source,
                p.org_id,
                p.UltOrg as orgname,
                p.naics3,
                p.naics3_name,
                p.naics_source,
                CASE
                    WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'B' THEN 'B'
                    WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'L' THEN 'L'
                    WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'I' THEN 'I'
                    WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'P' THEN 'P'
                    ELSE 'B'
                END as nature,
                p.DI,
                p.total_amount,
                p.CandID as recip_id,
                cnd.Name as recip_name,
                cnd.Party as recip_party,
                cnd.DistIDCurr as recip_seat,
                cnd.CRPICO as recip_incumbent
            FROM '{pacs_path}' p
            LEFT JOIN '{cmte_path}' c
                ON UPPER(TRIM(p.PACID)) = UPPER(TRIM(c.CmteID))
            LEFT JOIN cand_deduped cnd
                ON UPPER(TRIM(p.CandID)) = UPPER(TRIM(cnd.CandID))
            WHERE p.Type != '24A' OR p.Type IS NULL
        ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    # Get row counts of kept records
    kept_stats = con.execute(f"""
        SELECT COUNT(*), SUM(total_amount)
        FROM '{tmp_path}'
    """).fetch_df()

    kept_rows = int(kept_stats.iloc[0, 0])
    kept_amount = float(kept_stats.iloc[0, 1])

    con.close()

    return kept_rows, kept_amount, filtered_rows, filtered_amount


def main(cycles=None):
    """Process all cycles and produce output/contributions.parquet."""
    if cycles is None:
        cycles = ALL_CYCLES

    print("=" * 80)
    print("STAGE 7: FINAL ANALYTICAL DATABASE")
    print("=" * 80)
    print()

    temp_files = []

    # Cycle-level stats for final summary
    all_stats = {
        'indiv_rows_kept': 0,
        'indiv_amount_kept': 0.0,
        'indiv_rows_filtered': 0,
        'indiv_amount_filtered': 0.0,
        'pac_rows_kept': 0,
        'pac_amount_kept': 0.0,
        'pac_rows_filtered': 0,
        'pac_amount_filtered': 0.0,
    }

    for cycle in cycles:
        print(f"\nCycle {cycle}:")
        print("-" * 40)

        # Process indivs
        try:
            indiv_kept, indiv_amt, indiv_filt, indiv_amt_filt = process_indivs(cycle)
            all_stats['indiv_rows_kept'] += indiv_kept
            all_stats['indiv_amount_kept'] += indiv_amt
            all_stats['indiv_rows_filtered'] += indiv_filt
            all_stats['indiv_amount_filtered'] += indiv_amt_filt

            tmp_indiv = DATA_DIR / "derived" / cycle / "indivs_final_tmp.parquet"
            temp_files.append(tmp_indiv)

            print(f"  Indivs: {indiv_kept:,} rows, ${indiv_amt:,.2f}")
            print(f"    [INDIV->PAC filtered]: {indiv_filt:,} rows, ${indiv_amt_filt:,.2f}")
        except Exception as e:
            print(f"  ERROR processing indivs for cycle {cycle}: {e}")
            raise

        # Process PACs
        try:
            pac_kept, pac_amt, pac_filt, pac_amt_filt = process_pacs(cycle)
            all_stats['pac_rows_kept'] += pac_kept
            all_stats['pac_amount_kept'] += pac_amt
            all_stats['pac_rows_filtered'] += pac_filt
            all_stats['pac_amount_filtered'] += pac_amt_filt

            tmp_pac = DATA_DIR / "derived" / cycle / "pacs_final_tmp.parquet"
            temp_files.append(tmp_pac)

            print(f"  PACs: {pac_kept:,} rows, ${pac_amt:,.2f}")
            print(f"    [PAC->PAC filtered]: {pac_filt:,} rows, ${pac_amt_filt:,.2f}")
        except Exception as e:
            print(f"  ERROR processing PACs for cycle {cycle}: {e}")
            raise

    print()
    print("=" * 80)
    print("UNIONING ALL CYCLES")
    print("=" * 80)

    # Union all temp files
    if temp_files:
        temp_paths = [str(f).replace("\\", "/") for f in temp_files]
        output_path = str(OUTPUT_DIR / "contributions.parquet").replace("\\", "/")

        con = duckdb.connect()

        # Union via read_parquet with list
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet({temp_paths})
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        # Validate output
        result = con.execute(f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(total_amount) as total_amount,
                COUNT(DISTINCT cycle) as cycles_present,
                source,
                COUNT(*) as rows
            FROM '{output_path}'
            GROUP BY source
            ORDER BY source
        """).fetch_df()

        print(f"\nOutput: {output_path}")
        print(f"  Total rows: {result['rows'].sum():,}")
        print(f"  Total amount: ${result['rows'].sum() * result.iloc[0, 1] / result.iloc[0, 0]:,.2f}")  # Rough calc
        print()
        print("By source:")
        for _, row in result.iterrows():
            print(f"  {row['source']}: {row['rows']:,} rows")

        # Nature distribution
        nature_stats = con.execute(f"""
            SELECT
                source,
                nature,
                COUNT(*) as rows,
                SUM(total_amount) as amount
            FROM '{output_path}'
            GROUP BY source, nature
            ORDER BY source, nature
        """).fetch_df()

        print()
        print("Nature distribution (B=Business, I=Ideological, L=Labor, P=Party):")
        for _, row in nature_stats.iterrows():
            print(f"  {row['source'].upper()} {row['nature']}: {row['rows']:,} rows, ${row['amount']:,.2f}")

        # Recipient party match rate
        party_match = con.execute(f"""
            SELECT
                source,
                COUNT(*) as total_rows,
                SUM(CASE WHEN recip_party IS NOT NULL THEN 1 ELSE 0 END) as party_matched,
                ROUND(100.0 * SUM(CASE WHEN recip_party IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_matched
            FROM '{output_path}'
            GROUP BY source
            ORDER BY source
        """).fetch_df()

        print()
        print("Recipient party match rate:")
        for _, row in party_match.iterrows():
            print(f"  {row['source'].upper()}: {row['pct_matched']:.1f}% matched")

        con.close()

        # Delete temp files
        print()
        print(f"Cleaning up {len(temp_files)} temporary files...")
        for tmp_file in temp_files:
            if tmp_file.exists():
                tmp_file.unlink()
                print(f"  Deleted {tmp_file.name}")

    # Final summary
    print()
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    total_kept = all_stats['indiv_rows_kept'] + all_stats['pac_rows_kept']
    total_amount_kept = all_stats['indiv_amount_kept'] + all_stats['pac_amount_kept']
    total_filtered = all_stats['indiv_rows_filtered'] + all_stats['pac_rows_filtered']
    total_amount_filtered = all_stats['indiv_amount_filtered'] + all_stats['pac_amount_filtered']

    print(f"\nRows kept: {total_kept:,}")
    print(f"Amount kept: ${total_amount_kept:,.2f}")
    print()
    print(f"Rows filtered (INDIV->PAC + PAC->PAC): {total_filtered:,}")
    print(f"Amount filtered: ${total_amount_filtered:,.2f}")
    print()
    print(f"By source:")
    print(f"  Indivs: {all_stats['indiv_rows_kept']:,} rows, ${all_stats['indiv_amount_kept']:,.2f}")
    print(f"  PACs: {all_stats['pac_rows_kept']:,} rows, ${all_stats['pac_amount_kept']:,.2f}")
    print()
    print(f"Output: {OUTPUT_DIR / 'contributions.parquet'}")
    print()
    print("Stage 7 complete! [OK]")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build final analytical database (Stage 7).")
    parser.add_argument(
        "--cycles",
        nargs="*",
        default=None,
        help="Cycles to process (e.g. --cycles 20 22). Defaults to all 17 cycles.",
    )
    args = parser.parse_args()

    # Expand --cycles 20 22 into ["20", "22"]
    cycles_to_run = args.cycles if args.cycles else None
    main(cycles=cycles_to_run)
