from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = PROJECT_ROOT / 'reports' / 'v1_000_baseline'
LOOKUPS_DIR = PROJECT_ROOT / 'data' / 'lookups'
OUTPUT_DIR = PROJECT_ROOT / 'output'
DERIVED_GLOB = str((PROJECT_ROOT / 'data' / 'derived' / '*' / 'indivs_agg_enriched.parquet').as_posix())
ORG_ASSIGN_PATH = str((LOOKUPS_DIR / 'org_naics_assignment.csv').as_posix())
CONTRIB_PATH = str((OUTPUT_DIR / 'contributions.parquet').as_posix())
ZCTA_PATH = str((LOOKUPS_DIR / 'zcta_county_rel_2020' / 'tab20_zcta520_county20_natl.txt').as_posix())


def cycle_to_year(cycle: int) -> int:
    return 1900 + cycle if cycle >= 90 else 2000 + cycle


def write_csv(df: pd.DataFrame, name: str) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / name
    df.to_csv(path, index=False)
    return path


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    org_counts = con.execute(
        f"""
        SELECT
            assignment_method,
            COUNT(*) AS org_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS org_share_pct
        FROM read_csv_auto('{ORG_ASSIGN_PATH}')
        GROUP BY 1
        ORDER BY org_count DESC
        """
    ).df()
    write_csv(org_counts, 'assignment_method_org_counts.csv')

    assignment_dollars = con.execute(
        f"""
        WITH org_assign AS (
            SELECT CAST(org_id AS VARCHAR) AS org_id, assignment_method
            FROM read_csv_auto('{ORG_ASSIGN_PATH}')
        ), base AS (
            SELECT CAST(org_id AS VARCHAR) AS org_id, total_amount
            FROM '{CONTRIB_PATH}'
            WHERE naics_source = 'org_level' AND org_id IS NOT NULL
        ), joined AS (
            SELECT a.assignment_method, b.total_amount
            FROM base b
            JOIN org_assign a USING (org_id)
        ), totals AS (
            SELECT
                SUM(total_amount) AS total_org_level_dollars,
                (SELECT SUM(total_amount) FROM '{CONTRIB_PATH}') AS total_panel_dollars
            FROM joined
        )
        SELECT
            assignment_method,
            COUNT(*) AS contribution_rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * SUM(total_amount) / MAX(total_org_level_dollars), 4) AS org_level_dollar_share_pct,
            ROUND(100.0 * SUM(total_amount) / MAX(total_panel_dollars), 4) AS total_panel_dollar_share_pct
        FROM joined, totals
        GROUP BY 1
        ORDER BY dollars DESC
        """
    ).df()
    write_csv(assignment_dollars, 'assignment_method_dollar_shares.csv')

    llm_summary = con.execute(
        f"""
        WITH org_assign AS (
            SELECT
                CAST(org_id AS VARCHAR) AS org_id,
                COALESCE(NULLIF(llm_consensus, ''), '<NA>') AS llm_consensus,
                assignment_method
            FROM read_csv_auto('{ORG_ASSIGN_PATH}')
        ), org_counts AS (
            SELECT llm_consensus, COUNT(*) AS org_count
            FROM org_assign
            WHERE assignment_method = 'llm_assigned'
            GROUP BY 1
        ), dollar_counts AS (
            SELECT
                oa.llm_consensus,
                COUNT(*) AS contribution_rows,
                SUM(c.total_amount) AS dollars
            FROM '{CONTRIB_PATH}' c
            JOIN org_assign oa ON CAST(c.org_id AS VARCHAR) = oa.org_id
            WHERE oa.assignment_method = 'llm_assigned'
              AND c.naics_source = 'org_level'
            GROUP BY 1
        )
        SELECT
            o.llm_consensus,
            o.org_count,
            COALESCE(d.contribution_rows, 0) AS contribution_rows,
            COALESCE(d.dollars, 0) AS dollars,
            ROUND(100.0 * o.org_count / SUM(o.org_count) OVER (), 4) AS org_share_pct,
            ROUND(100.0 * COALESCE(d.dollars, 0) / NULLIF(SUM(COALESCE(d.dollars, 0)) OVER (), 0), 4) AS dollar_share_pct
        FROM org_counts o
        LEFT JOIN dollar_counts d USING (llm_consensus)
        ORDER BY CASE o.llm_consensus
            WHEN 'UNANIMOUS' THEN 1
            WHEN 'MAJORITY' THEN 2
            WHEN 'MAJORITY_R2' THEN 3
            ELSE 9
        END
        """
    ).df()
    write_csv(llm_summary, 'llm_consensus_summary.csv')

    county_overall = con.execute(
        f"""
        WITH all_rows AS (
            SELECT total_amount, LPAD(CAST(Zip AS VARCHAR), 5, '0') AS zip5
            FROM read_parquet('{DERIVED_GLOB}')
        ), zip_map_raw AS (
            SELECT GEOID_ZCTA5_20 AS zipcode, GEOID_COUNTY_20 AS fips, CAST(AREALAND_PART AS BIGINT) AS area
            FROM read_csv_auto('{ZCTA_PATH}', delim='|', header=true)
        ), zip_map AS (
            SELECT zipcode
            FROM (
                SELECT zipcode, fips, area,
                       ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY area DESC) AS rn
                FROM zip_map_raw
            )
            WHERE rn = 1
        ), tagged AS (
            SELECT
                CASE
                    WHEN zip5 IS NULL OR TRIM(zip5) = '' THEN 'blank_or_null'
                    WHEN zip5 = '00000' THEN '00000'
                    WHEN EXISTS (SELECT 1 FROM zip_map z WHERE z.zipcode = all_rows.zip5) THEN 'mapped'
                    ELSE 'unmatched_nonzero'
                END AS zip_status,
                total_amount
            FROM all_rows
        )
        SELECT
            zip_status,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            AVG(total_amount) AS avg_amount,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS row_share_pct,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 4) AS dollar_share_pct
        FROM tagged
        GROUP BY 1
        ORDER BY rows DESC
        """
    ).df()
    write_csv(county_overall, 'county_zip_status_overall.csv')

    county_by_cycle = con.execute(
        f"""
        WITH all_rows AS (
            SELECT
                regexp_extract(filename, '([0-9]{{2}})', 1) AS cycle,
                total_amount,
                LPAD(CAST(Zip AS VARCHAR), 5, '0') AS zip5
            FROM read_parquet('{DERIVED_GLOB}', filename=true)
        ), zip_map_raw AS (
            SELECT GEOID_ZCTA5_20 AS zipcode, GEOID_COUNTY_20 AS fips, CAST(AREALAND_PART AS BIGINT) AS area
            FROM read_csv_auto('{ZCTA_PATH}', delim='|', header=true)
        ), zip_map AS (
            SELECT zipcode
            FROM (
                SELECT zipcode, fips, area,
                       ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY area DESC) AS rn
                FROM zip_map_raw
            )
            WHERE rn = 1
        ), tagged AS (
            SELECT
                cycle,
                CASE
                    WHEN zip5 IS NULL OR TRIM(zip5) = '' THEN 'blank_or_null'
                    WHEN zip5 = '00000' THEN '00000'
                    WHEN EXISTS (SELECT 1 FROM zip_map z WHERE z.zipcode = all_rows.zip5) THEN 'mapped'
                    ELSE 'unmatched_nonzero'
                END AS zip_status,
                total_amount
            FROM all_rows
        )
        SELECT
            cycle,
            zip_status,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            AVG(total_amount) AS avg_amount,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cycle), 4) AS row_share_pct,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (PARTITION BY cycle), 4) AS dollar_share_pct
        FROM tagged
        GROUP BY 1, 2
        ORDER BY cycle, rows DESC
        """
    ).df()
    county_by_cycle['cycle'] = county_by_cycle['cycle'].astype(int)
    county_by_cycle['year'] = county_by_cycle['cycle'].map(cycle_to_year)
    county_by_cycle = county_by_cycle[['cycle', 'year', 'zip_status', 'rows', 'dollars', 'avg_amount', 'row_share_pct', 'dollar_share_pct']]
    write_csv(county_by_cycle, 'county_zip_status_by_cycle.csv')

    drift = con.execute(
        f"""
        WITH industry_cycle AS (
          SELECT cycle,
                 CAST(naics3 AS VARCHAR) AS naics3,
                 SUM(CASE WHEN recip_party='D' THEN total_amount ELSE 0 END) AS dem,
                 SUM(CASE WHEN recip_party='R' THEN total_amount ELSE 0 END) AS rep,
                 SUM(total_amount) AS total_dollars
          FROM '{CONTRIB_PATH}'
          WHERE nature = 'B'
            AND recip_party IN ('D', 'R')
            AND naics3 IS NOT NULL
          GROUP BY 1, 2
        ), endpoints AS (
          SELECT
              naics3,
              MAX(CASE WHEN cycle = 90 THEN 100.0 * dem / NULLIF(dem + rep, 0) END) AS dshare_1990,
              MAX(CASE WHEN cycle = 22 THEN 100.0 * dem / NULLIF(dem + rep, 0) END) AS dshare_2022,
              SUM(total_dollars) AS lifetime_dollars
          FROM industry_cycle
          GROUP BY 1
        )
        SELECT
            naics3,
            ROUND(dshare_1990, 4) AS dshare_1990,
            ROUND(dshare_2022, 4) AS dshare_2022,
            ROUND(dshare_2022 - dshare_1990, 4) AS delta_pp,
            lifetime_dollars,
            ROUND(lifetime_dollars / 1000000000.0, 6) AS lifetime_dollars_bil
        FROM endpoints
        WHERE dshare_1990 IS NOT NULL AND dshare_2022 IS NOT NULL
        ORDER BY delta_pp DESC, lifetime_dollars DESC
        """
    ).df()
    write_csv(drift, 'industry_drift_leaderboard.csv')

    summary = {
        'generated_at_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'version_label': 'v1.000-baseline',
        'source_files': {
            'org_assignment': str(Path(ORG_ASSIGN_PATH).relative_to(PROJECT_ROOT).as_posix()),
            'contributions': str(Path(CONTRIB_PATH).relative_to(PROJECT_ROOT).as_posix()),
            'indivs_agg_enriched_glob': 'data/derived/*/indivs_agg_enriched.parquet',
            'zcta_crosswalk': str(Path(ZCTA_PATH).relative_to(PROJECT_ROOT).as_posix()),
        },
        'headline_metrics': {
            'org_assignment_total': int(org_counts['org_count'].sum()),
            'panel_total_dollars': float(con.execute(f"SELECT SUM(total_amount) FROM '{CONTRIB_PATH}'").fetchone()[0]),
            'mapped_county_row_share_pct': float(county_overall.loc[county_overall['zip_status'] == 'mapped', 'row_share_pct'].iloc[0]),
            'mapped_county_dollar_share_pct': float(county_overall.loc[county_overall['zip_status'] == 'mapped', 'dollar_share_pct'].iloc[0]),
            'largest_positive_drift_naics3': drift.iloc[0]['naics3'] if not drift.empty else None,
            'largest_negative_drift_naics3': drift.sort_values('delta_pp').iloc[0]['naics3'] if not drift.empty else None,
        },
        'output_files': sorted(path.name for path in REPORT_DIR.iterdir() if path.is_file()),
    }

    with open(REPORT_DIR / 'release_summary.json', 'w', encoding='utf-8') as fh:
        json.dump(summary, fh, indent=2)

    print('Baseline artifacts written to', REPORT_DIR)
    for path in sorted(REPORT_DIR.iterdir()):
        if path.is_file():
            print('-', path.name)


if __name__ == '__main__':
    main()
