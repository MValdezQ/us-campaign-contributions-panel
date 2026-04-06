"""
Apply NAICS industry assignments to contribution parquets.

Two-step strategy for each contribution:
  1. PRIMARY: RealCode (indivs) or PrimCode (PACs) -> catcode_naics_candidates -> best NAICS
     Uses highest freq_share candidate when a catcode maps to multiple NAICS-3.
     Covers ~95-100% of business-coded contributions.

  2. OVERRIDE (top-13k orgs): Orgname -> org_aliases -> org_id -> org_naics_assignment
     For the top-13k organizations resolved by entity resolution, the LLM-disambiguated NAICS
     replaces the freq_share best-candidate. This provides high-confidence mapping for 
     ambiguous industrial categories.

Pipeline Stage: 6 (Industry Mapping)
"""

import argparse
import sys
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
LOOKUPS_DIR = DATA_DIR / "lookups"
DERIVED_DIR = DATA_DIR / "derived"
CLEAN_DIR = DATA_DIR / "clean"

NAICS_ASSIGNMENT_PATH = LOOKUPS_DIR / "org_naics_assignment.csv"
ORG_ALIASES_PATH = LOOKUPS_DIR / "org_aliases_top_13k_enhanced.parquet"
CATCODE_NAICS_PATH = LOOKUPS_DIR / "catcode_naics_candidates.csv"

# All even-year cycles 1990-2022 (stored as 2-digit: 90, 92, ..., 0, 2, ..., 22)
ALL_CYCLES = [90, 92, 94, 96, 98, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]


def validate_inputs():
    for path, label in [
        (NAICS_ASSIGNMENT_PATH, "org_naics_assignment.csv"),
        (ORG_ALIASES_PATH, "org_aliases_top_13k_enhanced.parquet"),
        (CATCODE_NAICS_PATH, "catcode_naics_candidates.csv"),
    ]:
        if not path.exists():
            print(f"ERROR: {label} not found. Run Stage 5 first.")
            sys.exit(1)
    print("[OK] All required lookup files found")


def map_indivs(cycle: int) -> bool:
    """
    Enrich indivs_agg with NAICS via RealCode + org-level override.

    Primary:  i.RealCode -> catcode_naics_candidates (best freq_share candidate) -> naics3
    Override: i.Orgname  -> org_aliases -> org_id   -> org_naics_assignment      -> naics3
    """
    indivs_path = DERIVED_DIR / f"{cycle:02d}" / "indivs_agg.parquet"
    output_path = DERIVED_DIR / f"{cycle:02d}" / "indivs_agg_enriched.parquet"

    if not indivs_path.exists():
        print(f"  [SKIP] indivs_agg.parquet not found")
        return False

    naics_path = str(NAICS_ASSIGNMENT_PATH).replace("\\", "/")
    org_aliases_path = str(ORG_ALIASES_PATH).replace("\\", "/")
    catcode_path = str(CATCODE_NAICS_PATH).replace("\\", "/")
    in_path = str(indivs_path).replace("\\", "/")
    out_path = str(output_path).replace("\\", "/")

    con = duckdb.connect()

    con.execute(f"""
        COPY (
            WITH
            -- Step 1: best NAICS candidate per catcode (by freq_share)
            -- Filter out invalid NAICS codes (< 100 are not real NAICS-3 codes)
            best_catcode AS (
                SELECT catcode, CAST(naics3 AS VARCHAR) as naics3, naics3_name,
                       ROW_NUMBER() OVER (PARTITION BY catcode ORDER BY freq_share DESC) as rn
                FROM read_csv('{catcode_path}')
                WHERE TRY_CAST(naics3 AS INTEGER) >= 100
            ),
            -- Step 2: org-level NAICS overrides from entity resolution
            -- PRINTF to avoid DOUBLE->VARCHAR producing "523.0" instead of "523"
            org_naics AS (
                SELECT org_id,
                       PRINTF('%d', CAST(assigned_naics3 AS INTEGER)) as assigned_naics3,
                       assigned_naics3_name
                FROM read_csv('{naics_path}')
                WHERE assigned_naics3 IS NOT NULL
                  AND TRY_CAST(assigned_naics3 AS INTEGER) >= 100
            ),
            -- Step 3: name -> org_id mapping (deduplicated, REJECT entries excluded)
            aliases AS (
                SELECT DISTINCT LOWER(orgname_raw) as orgname_norm, org_id
                FROM read_parquet('{org_aliases_path}')
                WHERE cluster_decision != 'REJECT'
            )
            SELECT
                i.*,
                -- org-level resolution
                al.org_id,
                -- primary NAICS: catcode-level (realcode)
                bc.naics3 as realcode_naics3,
                bc.naics3_name as realcode_naics3_name,
                -- override: org-level NAICS (for top-13k)
                on_.assigned_naics3 as org_naics3,
                on_.assigned_naics3_name as org_naics3_name,
                -- final: prefer org-level, fallback to realcode
                COALESCE(on_.assigned_naics3, bc.naics3) as naics3,
                COALESCE(on_.assigned_naics3_name, bc.naics3_name) as naics3_name,
                CASE
                    WHEN on_.assigned_naics3 IS NOT NULL THEN 'org_level'
                    WHEN bc.naics3 IS NOT NULL THEN 'realcode'
                    ELSE NULL
                END as naics_source
            FROM read_parquet('{in_path}') i
            -- org-level override lookup
            LEFT JOIN aliases al
                ON LOWER(i.Orgname) = al.orgname_norm
            LEFT JOIN org_naics on_
                ON al.org_id = on_.org_id
            -- catcode-level fallback
            LEFT JOIN best_catcode bc
                ON UPPER(i.RealCode) = UPPER(bc.catcode) AND bc.rn = 1
        ) TO '{out_path}' (FORMAT PARQUET)
    """)

    # Summary stats
    stats = con.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(CASE WHEN org_id IS NOT NULL THEN 1 END) as org_matched,
            COUNT(CASE WHEN naics3 IS NOT NULL THEN 1 END) as naics_assigned,
            SUM(total_amount) as total_dollars,
            SUM(CASE WHEN naics3 IS NOT NULL THEN total_amount END) as naics_dollars,
            COUNT(CASE WHEN naics_source = 'org_level' THEN 1 END) as org_level_count,
            COUNT(CASE WHEN naics_source = 'realcode'  THEN 1 END) as realcode_count
        FROM read_parquet('{out_path}')
    """).fetch_df()

    s = stats.iloc[0]
    print(f"  Individuals (cycle {cycle}): {s['total_rows']:>12,.0f} rows  ${s['total_dollars']:>14,.0f}")
    print(f"    - org_id matched:  {s['org_matched']:>12,.0f} ({100*s['org_matched']/s['total_rows']:>5.1f}%)")
    print(f"    - naics3 assigned: {s['naics_assigned']:>12,.0f} ({100*s['naics_assigned']/s['total_rows']:>5.1f}%)")
    print(f"        via org_level: {s['org_level_count']:>12,.0f}")
    print(f"        via realcode:  {s['realcode_count']:>12,.0f}")
    print(f"    - $ coverage:      {100*s['naics_dollars']/s['total_dollars']:>5.1f}%")
    print(f"    Saved: indivs_agg_enriched.parquet")
    return True


def map_pacs(cycle: int) -> bool:
    """
    Enrich pacs_agg with NAICS via PrimCode + org-level override.

    Primary:  PACID -> committees.PrimCode -> catcode_naics_candidates -> naics3
    Override: committees.PACShort -> org_aliases -> org_id -> org_naics_assignment -> naics3
    """
    pacs_path = DERIVED_DIR / f"{cycle:02d}" / "pacs_agg.parquet"
    committees_path = CLEAN_DIR / f"{cycle:02d}" / "committees.parquet"
    output_path = DERIVED_DIR / f"{cycle:02d}" / "pacs_agg_enriched.parquet"

    if not pacs_path.exists():
        print(f"  [SKIP] pacs_agg.parquet not found")
        return False
    if not committees_path.exists():
        print(f"  [SKIP] committees.parquet not found for cycle {cycle}")
        return False

    naics_path = str(NAICS_ASSIGNMENT_PATH).replace("\\", "/")
    org_aliases_path = str(ORG_ALIASES_PATH).replace("\\", "/")
    catcode_path = str(CATCODE_NAICS_PATH).replace("\\", "/")
    in_path = str(pacs_path).replace("\\", "/")
    cmte_path = str(committees_path).replace("\\", "/")
    out_path = str(output_path).replace("\\", "/")

    con = duckdb.connect()

    con.execute(f"""
        COPY (
            WITH
            best_catcode AS (
                SELECT catcode, CAST(naics3 AS VARCHAR) as naics3, naics3_name,
                       ROW_NUMBER() OVER (PARTITION BY catcode ORDER BY freq_share DESC) as rn
                FROM read_csv('{catcode_path}')
                WHERE TRY_CAST(naics3 AS INTEGER) >= 100
            ),
            org_naics AS (
                SELECT org_id,
                       PRINTF('%d', CAST(assigned_naics3 AS INTEGER)) as assigned_naics3,
                       assigned_naics3_name
                FROM read_csv('{naics_path}')
                WHERE assigned_naics3 IS NOT NULL
                  AND TRY_CAST(assigned_naics3 AS INTEGER) >= 100
            ),
            aliases AS (
                SELECT DISTINCT LOWER(orgname_raw) as orgname_norm, org_id
                FROM read_parquet('{org_aliases_path}')
                WHERE cluster_decision != 'REJECT'
            )
            SELECT
                p.*,
                c.PrimCode,
                c.PACShort,
                c.UltOrg,
                al.org_id,
                -- primary: PrimCode catcode mapping
                bc.naics3 as primcode_naics3,
                bc.naics3_name as primcode_naics3_name,
                -- override: org-level
                on_.assigned_naics3 as org_naics3,
                on_.assigned_naics3_name as org_naics3_name,
                -- final
                COALESCE(on_.assigned_naics3, bc.naics3) as naics3,
                COALESCE(on_.assigned_naics3_name, bc.naics3_name) as naics3_name,
                CASE
                    WHEN on_.assigned_naics3 IS NOT NULL THEN 'org_level'
                    WHEN bc.naics3 IS NOT NULL THEN 'primcode'
                    ELSE NULL
                END as naics_source
            FROM read_parquet('{in_path}') p
            -- join to get PrimCode and PAC name from committees
            LEFT JOIN read_parquet('{cmte_path}') c
                ON UPPER(p.PACID) = c.CmteID
            -- org-level override via PAC name
            LEFT JOIN aliases al
                ON LOWER(COALESCE(c.UltOrg, c.PACShort)) = al.orgname_norm
            LEFT JOIN org_naics on_
                ON al.org_id = on_.org_id
            -- catcode fallback via PrimCode
            LEFT JOIN best_catcode bc
                ON UPPER(c.PrimCode) = UPPER(bc.catcode) AND bc.rn = 1
        ) TO '{out_path}' (FORMAT PARQUET)
    """)

    stats = con.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(CASE WHEN PrimCode IS NOT NULL THEN 1 END) as has_primcode,
            COUNT(CASE WHEN org_id IS NOT NULL THEN 1 END) as org_matched,
            COUNT(CASE WHEN naics3 IS NOT NULL THEN 1 END) as naics_assigned,
            SUM(total_amount) as total_dollars,
            SUM(CASE WHEN naics3 IS NOT NULL THEN total_amount END) as naics_dollars,
            COUNT(CASE WHEN naics_source = 'org_level' THEN 1 END) as org_level_count,
            COUNT(CASE WHEN naics_source = 'primcode'  THEN 1 END) as primcode_count
        FROM read_parquet('{out_path}')
    """).fetch_df()

    s = stats.iloc[0]
    print(f"  PACs (cycle {cycle}): {s['total_rows']:>12,.0f} rows  ${s['total_dollars']:>14,.0f}")
    print(f"    - committees matched: {s['has_primcode']:>10,.0f}")
    print(f"    - org_id matched:     {s['org_matched']:>10,.0f}")
    print(f"    - naics3 assigned:    {s['naics_assigned']:>10,.0f} ({100*s['naics_assigned']/s['total_rows']:>5.1f}%)")
    print(f"        via org_level:    {s['org_level_count']:>10,.0f}")
    print(f"        via primcode:     {s['primcode_count']:>10,.0f}")
    print(f"    - $ coverage:         {100*s['naics_dollars']/s['total_dollars']:>5.1f}%")
    print(f"    Saved: pacs_agg_enriched.parquet")
    return True


def main():
    parser = argparse.ArgumentParser(description="Apply NAICS to contribution parquets")
    parser.add_argument(
        "--cycles", type=str,
        default=",".join(str(c) for c in ALL_CYCLES),
        help="Comma-separated cycles (default: all available)"
    )
    args = parser.parse_args()
    cycles = [int(c.strip()) for c in args.cycles.split(",")]

    print("=" * 70)
    print("STAGE 6: Industry Mapping - Apply NAICS to All Contributions")
    print("=" * 70)
    print("\nValidating inputs...")
    validate_inputs()

    naics_path = str(NAICS_ASSIGNMENT_PATH)
    print(f"\nProcessing {len(cycles)} cycles: {cycles}")

    for cycle in cycles:
        print(f"\nCycle {cycle}:")
        map_indivs(cycle)
        map_pacs(cycle)

    print("\n" + "=" * 70)
    print("Industry mapping complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
