"""
validate_stage1.py

Validates that raw TXT file data is correctly ingested into Parquet files.
Compares total amounts in raw files vs clean parquet files across all cycles.

Cross-checks:
- PACs: raw pacs{YY}.txt -> clean/pacs.parquet
- Individuals: raw indivs{YY}.txt -> clean/indivs.parquet

Copyright 2025 Martin A. Valdez
"""

import duckdb
import pandas as pd
from pathlib import Path

# Resolve project root (validate_stage1.py is at src/data/)
project_root = Path(__file__).resolve().parents[2]
raw_dir = project_root / "data" / "raw" / "political"
clean_dir = project_root / "data" / "clean"
reports_dir = project_root / "reports"

# Column definitions (from ingest_raw_files.py)
pacs_columns = ['Cycle', 'FECRecID', 'PACID', 'CandID', 'Amount', 'Date', 'RealCode', 'Type', 'DI', 'FECCandID']
indivs_columns = [
    'Cycle', 'FECTransID', 'ContribID', 'Contrib', 'RecipID', 'Orgname', 'UltOrg',
    'RealCode', 'Date', 'Amount', 'Street', 'City', 'State', 'Zip',
    'RecipCode', 'Type', 'CmteID', 'OtherID', 'Gender', 'Microfilm',
    'Occupation', 'Employer', 'Source'
]

def _discover_cycles():
    """Find all available cycles by scanning clean/ directory."""
    if not clean_dir.exists():
        print(f"Clean directory not found: {clean_dir}")
        return []
    cycles = sorted([d.name for d in clean_dir.iterdir() if d.is_dir() and d.name.isdigit()])
    return cycles

def validate_cycle(cycle: str) -> dict:
    """
    Validate a single cycle by comparing raw and clean file amounts.

    Args:
        cycle: 2-digit cycle (e.g., '00', '18', '92')

    Returns:
        Dictionary with validation results for this cycle.
    """
    result = {"cycle": cycle}
    con = duckdb.connect()

    # PACs validation
    pacs_raw_path = raw_dir / f"CampaignFin{cycle}" / f"pacs{cycle}.txt"
    pacs_clean_path = clean_dir / cycle / "pacs.parquet"

    if pacs_raw_path.exists() and pacs_clean_path.exists():
        try:
            # Use DuckDB to sum amounts directly from raw CSV (column 5 = Amount)
            pacs_raw_result = con.execute(f"""
                SELECT CAST(column4 AS FLOAT) as amount FROM read_csv_auto(
                    '{pacs_raw_path}',
                    DELIM=',',
                    QUOTE='|',
                    HEADER=False,
                    IGNORE_ERRORS=True,
                    ALL_VARCHAR=True
                )
                WHERE TRY_CAST(column4 AS FLOAT) IS NOT NULL
            """).fetchall()

            pacs_raw_amount = sum(row[0] for row in pacs_raw_result) if pacs_raw_result else 0

            # Read clean PACs parquet and sum amounts
            pacs_clean_result = con.execute(f"SELECT SUM(CAST(Amount AS FLOAT)) FROM read_parquet('{pacs_clean_path}')").fetchone()
            pacs_clean_amount = pacs_clean_result[0] if pacs_clean_result[0] is not None else 0

            # Calculate discrepancy
            pacs_diff_pct = round(
                100 * abs(pacs_raw_amount - pacs_clean_amount) / pacs_raw_amount
                if pacs_raw_amount != 0 else 0, 3
            )

            result['pacs_raw_total'] = round(pacs_raw_amount, 2)
            result['pacs_clean_total'] = round(pacs_clean_amount, 2)
            result['pacs_total_diff_pct'] = pacs_diff_pct
            result['pacs_status'] = 'PASS' if pacs_diff_pct < 0.1 else 'FAIL'
        except Exception as e:
            result['pacs_status'] = f'ERROR: {str(e)[:50]}'
    else:
        result['pacs_status'] = 'MISSING'

    # Individuals validation
    indivs_raw_path = raw_dir / f"CampaignFin{cycle}" / f"indivs{cycle}.txt"
    indivs_clean_path = clean_dir / cycle / "indivs.parquet"

    if indivs_raw_path.exists() and indivs_clean_path.exists():
        try:
            # For large files, use DuckDB to avoid memory issues
            try:
                indivs_raw_result = con.execute(f"""
                    SELECT CAST(column9 AS FLOAT) as amount FROM read_csv_auto(
                        '{indivs_raw_path}',
                        DELIM=',',
                        QUOTE='|',
                        HEADER=False,
                        IGNORE_ERRORS=True,
                        ALL_VARCHAR=True,
                        SAMPLE_SIZE=-1
                    )
                    WHERE TRY_CAST(column9 AS FLOAT) IS NOT NULL
                """).fetchall()

                indivs_raw_amount = sum(row[0] for row in indivs_raw_result) if indivs_raw_result else 0
            except:
                # Fallback: try pandas with chunking for memory efficiency
                indivs_raw_amount = 0
                for chunk in pd.read_csv(
                    indivs_raw_path,
                    header=None,
                    delimiter=',',
                    quotechar='|',
                    on_bad_lines='skip',
                    encoding='latin-1',
                    dtype=str,
                    chunksize=100000
                ):
                    indivs_raw_amount += pd.to_numeric(chunk.iloc[:, 9], errors='coerce').sum()

            # Read clean indivs parquet and sum amounts
            indivs_clean_result = con.execute(f"SELECT SUM(CAST(Amount AS FLOAT)) FROM read_parquet('{indivs_clean_path}')").fetchone()
            indivs_clean_amount = indivs_clean_result[0] if indivs_clean_result[0] is not None else 0

            # Calculate discrepancy
            indivs_diff_pct = round(
                100 * abs(indivs_raw_amount - indivs_clean_amount) / indivs_raw_amount
                if indivs_raw_amount != 0 else 0, 3
            )

            result['indivs_raw_total'] = round(indivs_raw_amount, 2)
            result['indivs_clean_total'] = round(indivs_clean_amount, 2)
            result['indivs_total_diff_pct'] = indivs_diff_pct
            result['indivs_status'] = 'PASS' if indivs_diff_pct < 0.1 else 'FAIL'
        except Exception as e:
            result['indivs_status'] = f'ERROR: {str(e)[:50]}'
    else:
        result['indivs_status'] = 'MISSING'

    con.close()
    return result

def main():
    """Run validation across all available cycles."""
    print("\n" + "="*80)
    print("STAGE 1 VALIDATION: Raw -> Clean Data Integrity Check")
    print("="*80 + "\n")

    cycles = _discover_cycles()
    if not cycles:
        print("No cycles found in data/clean/")
        return

    print(f"Found {len(cycles)} cycles: {', '.join(cycles)}\n")

    rows = []
    for cycle in cycles:
        print(f"Validating cycle {cycle}...", end=" ")
        result = validate_cycle(cycle)
        rows.append(result)

        # Print status
        pacs_status = result.get('pacs_status', 'N/A')
        indivs_status = result.get('indivs_status', 'N/A')
        print(f"PACs: {pacs_status}, Individuals: {indivs_status}")

    # Write results to CSV
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / "stage1_validation.csv"

    results_df = pd.DataFrame(rows)
    results_df.to_csv(output_path, index=False)
    print(f"\nResults saved to {output_path}\n")

    # Print summary
    pacs_errors = sum(1 for r in rows if isinstance(r.get('pacs_status'), str) and r['pacs_status'].startswith('ERROR'))
    indivs_errors = sum(1 for r in rows if isinstance(r.get('indivs_status'), str) and r['indivs_status'].startswith('ERROR'))
    pacs_failures = sum(1 for r in rows if r.get('pacs_status') == 'FAIL')
    indivs_failures = sum(1 for r in rows if r.get('indivs_status') == 'FAIL')

    print("SUMMARY:")
    print(f"  PACs:         {len(cycles) - pacs_errors - pacs_failures}/{len(cycles)} passed")
    if pacs_errors:
        print(f"                ({pacs_errors} errors, {pacs_failures} discrepancies > 0.1%)")
    if pacs_failures:
        print(f"                ({pacs_failures} discrepancies > 0.1%)")

    print(f"  Individuals:  {len(cycles) - indivs_errors - indivs_failures}/{len(cycles)} passed")
    if indivs_errors:
        print(f"                ({indivs_errors} errors, {indivs_failures} discrepancies > 0.1%)")
    if indivs_failures:
        print(f"                ({indivs_failures} discrepancies > 0.1%)")

    # Overall result
    all_passed = (pacs_errors == 0 and pacs_failures == 0 and
                  indivs_errors == 0 and indivs_failures == 0)

    if all_passed:
        print("\n[PASS] All cycles passed Stage 1 validation!")
    else:
        print("\n[FAIL] Some cycles failed Stage 1 validation. Review details above.")

    print("="*80 + "\n")
    return output_path

if __name__ == "__main__":
    main()
