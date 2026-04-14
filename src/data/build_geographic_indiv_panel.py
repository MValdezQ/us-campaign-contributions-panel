from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / 'data'
DERIVED_DIR = DATA_DIR / 'derived'
CLEAN_DIR = DATA_DIR / 'clean'
LOOKUPS_DIR = DATA_DIR / 'lookups'
OUTPUT_DIR = PROJECT_ROOT / 'output'
REPORT_DIR = PROJECT_ROOT / 'reports' / 'v1_001'

ALL_CYCLES = ['90', '92', '94', '96', '98', '00', '02', '04', '06', '08', '10', '12', '14', '16', '18', '20', '22']


def normalize_county_name(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .str.replace(' county', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace("'", '', regex=False)
        .str.replace('-', ' ', regex=False)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )


def build_zip_fips_map_v1001() -> pd.DataFrame:
    census_path = LOOKUPS_DIR / 'zcta_county_rel_2020' / 'tab20_zcta520_county20_natl.txt'
    alt_zip_path = LOOKUPS_DIR / 'zip_county_fips.csv'
    alt_crosswalk_path = LOOKUPS_DIR / 'zip_county_crosswalk_2025.csv'
    fips_master_path = LOOKUPS_DIR / 'fips_master.csv'

    if not census_path.exists():
        raise FileNotFoundError(f'Missing Census crosswalk: {census_path}')
    if not fips_master_path.exists():
        raise FileNotFoundError(f'Missing FIPS master: {fips_master_path}')

    fips_master = pd.read_csv(fips_master_path, dtype={'fips': str}, encoding='latin-1')
    fips_master['state_abbr'] = fips_master['state_abbr'].astype(str).str.upper()
    fips_master['county_key'] = normalize_county_name(fips_master['county_name'])
    fips_lookup = fips_master[['fips', 'state_abbr', 'county_key']].drop_duplicates()

    census = pd.read_csv(census_path, sep='|', dtype=str)
    census = census[['GEOID_ZCTA5_20', 'GEOID_COUNTY_20', 'AREALAND_PART']].copy()
    census.columns = ['zipcode', 'fips', 'area']
    census['zipcode'] = census['zipcode'].astype(str).str.zfill(5)
    census['fips'] = census['fips'].astype(str).str.zfill(5)
    census['area'] = pd.to_numeric(census['area'], errors='coerce').fillna(0)
    census['zip_total_area'] = census.groupby('zipcode')['area'].transform('sum')
    census['dominance_share'] = census['area'] / census['zip_total_area'].where(census['zip_total_area'] != 0, 1)
    census = census.sort_values(['zipcode', 'area'], ascending=[True, False]).drop_duplicates('zipcode', keep='first')
    census['assignment_method'] = 'census_zcta_landmax'
    census['assignment_confidence'] = 'high'
    census['n_candidate_counties'] = 1
    census['zip_issue_type'] = 'mapped'
    census['source_name'] = 'tab20_zcta520_county20_natl.txt'
    census = census[['zipcode', 'fips', 'assignment_method', 'assignment_confidence', 'n_candidate_counties', 'dominance_share', 'zip_issue_type', 'source_name']]

    resolved_zips = set(census['zipcode'])
    frames = [census]

    if alt_zip_path.exists():
        alt_zip = pd.read_csv(alt_zip_path, dtype=str)
        alt_zip['zipcode'] = alt_zip['zipcode'].astype(str).str.zfill(5)
        alt_zip['state_abbr'] = alt_zip['state_abbr'].astype(str).str.upper()
        alt_zip['county_key'] = normalize_county_name(alt_zip['county'])
        alt_zip = alt_zip.merge(fips_lookup, on=['state_abbr', 'county_key'], how='left')
        alt_zip = alt_zip.dropna(subset=['fips'])
        alt_zip = alt_zip.groupby('zipcode').filter(lambda g: g['fips'].nunique() == 1)
        alt_zip = alt_zip[['zipcode', 'fips']].drop_duplicates()
        alt_zip = alt_zip[~alt_zip['zipcode'].isin(resolved_zips)].copy()
        alt_zip['assignment_method'] = 'zip_county_fips_unique'
        alt_zip['assignment_confidence'] = 'medium'
        alt_zip['n_candidate_counties'] = 1
        alt_zip['dominance_share'] = pd.NA
        alt_zip['zip_issue_type'] = 'mapped'
        alt_zip['source_name'] = 'zip_county_fips.csv'
        frames.append(alt_zip)
        resolved_zips |= set(alt_zip['zipcode'])

    if alt_crosswalk_path.exists():
        alt_crosswalk = pd.read_csv(alt_crosswalk_path, dtype=str)
        alt_crosswalk['zipcode'] = alt_crosswalk['zipcode'].astype(str).str.zfill(5)
        alt_crosswalk['state_abbr'] = alt_crosswalk['state_code'].astype(str).str.upper()
        alt_crosswalk['county_key'] = normalize_county_name(alt_crosswalk['county_name'])
        alt_crosswalk = alt_crosswalk.merge(fips_lookup, on=['state_abbr', 'county_key'], how='left')
        alt_crosswalk = alt_crosswalk.dropna(subset=['fips'])
        alt_crosswalk = alt_crosswalk.groupby('zipcode').filter(lambda g: g['fips'].nunique() == 1)
        alt_crosswalk = alt_crosswalk[['zipcode', 'fips']].drop_duplicates()
        alt_crosswalk = alt_crosswalk[~alt_crosswalk['zipcode'].isin(resolved_zips)].copy()
        alt_crosswalk['assignment_method'] = 'zip_county_crosswalk_2025_unique'
        alt_crosswalk['assignment_confidence'] = 'medium'
        alt_crosswalk['n_candidate_counties'] = 1
        alt_crosswalk['dominance_share'] = pd.NA
        alt_crosswalk['zip_issue_type'] = 'mapped'
        alt_crosswalk['source_name'] = 'zip_county_crosswalk_2025.csv'
        frames.append(alt_crosswalk)

    lookup = pd.concat(frames, ignore_index=True)
    lookup = lookup.drop_duplicates('zipcode', keep='first').sort_values('zipcode').reset_index(drop=True)

    out_path = LOOKUPS_DIR / 'zip_to_county_assignment_v1001.parquet'
    lookup.to_parquet(out_path, index=False)
    print(f'Built ZIP lookup: {out_path} ({len(lookup):,} ZIPs)')
    return lookup


def run_geo_panel(lookup_df: pd.DataFrame, input_suffix: str, output_name: str) -> None:
    con = duckdb.connect()
    con.register('zip_lookup', lookup_df)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    out_path = OUTPUT_DIR / output_name
    dfs = []

    for cycle in ALL_CYCLES:
        in_path = DERIVED_DIR / cycle / f'indivs_agg_enriched{input_suffix}.parquet'
        cmte_path = CLEAN_DIR / cycle / 'committees.parquet'
        cand_path = CLEAN_DIR / cycle / 'candidates.parquet'
        if not in_path.exists():
            continue

        print(f'Processing cycle {cycle}...')
        query = f"""
            WITH cand_deduped AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY CandID ORDER BY CandID) AS rn
                    FROM read_parquet('{str(cand_path).replace('\\', '/')}')
                ) WHERE rn = 1
            ), enriched_indivs AS (
                SELECT
                    i.*,
                    CASE
                        WHEN i.Zip IS NULL OR TRIM(CAST(i.Zip AS VARCHAR)) = '' THEN NULL
                        WHEN LPAD(CAST(i.Zip AS VARCHAR), 5, '0') = '00000' THEN NULL
                        ELSE z.fips
                    END AS donor_county_fips,
                    z.assignment_method AS county_assignment_method
                FROM read_parquet('{str(in_path).replace('\\', '/')}') i
                LEFT JOIN zip_lookup z
                    ON LPAD(CAST(i.Zip AS VARCHAR), 5, '0') = z.zipcode
            )
            SELECT
                CAST({cycle} AS INTEGER) AS cycle_id,
                e.naics3,
                e.donor_county_fips,
                e.CmteID AS recip_id,
                COALESCE(cnd.Party, c.Party) AS recip_party,
                cnd.DistIDCurr AS recip_seat,
                cnd.CRPICO AS recip_incumbent,
                SUM(e.total_amount) AS total_amount
            FROM enriched_indivs e
            LEFT JOIN read_parquet('{str(cmte_path).replace('\\', '/')}') c
                ON UPPER(TRIM(e.CmteID)) = UPPER(TRIM(c.CmteID))
            LEFT JOIN cand_deduped cnd
                ON UPPER(TRIM(c.RecipID)) = UPPER(TRIM(cnd.CandID))
            GROUP BY cycle_id, e.naics3, donor_county_fips, recip_id, recip_party, recip_seat, recip_incumbent
        """
        df_cycle = con.execute(query).fetch_df()
        dfs.append(df_cycle)
        print(f"  rows={len(df_cycle):,} null_fips={df_cycle['donor_county_fips'].isna().sum():,}")

    if not dfs:
        raise RuntimeError('No cycles processed for geographic panel build.')

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_parquet(out_path, compression='snappy', index=False)
    print(f'Saved geographic panel: {out_path} ({len(final_df):,} rows)')


def build_diagnostics(input_suffix: str) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    lookup_path = str((LOOKUPS_DIR / 'zip_to_county_assignment_v1001.parquet').as_posix())
    input_glob = str((DERIVED_DIR / '*' / f'indivs_agg_enriched{input_suffix}.parquet').as_posix())

    base_cte = f"""
        WITH all_rows AS (
            SELECT
                regexp_extract(filename, '([0-9]{{2}})', 1) AS cycle,
                total_amount,
                CAST(Zip AS VARCHAR) AS raw_zip,
                CASE
                    WHEN Zip IS NULL THEN NULL
                    WHEN TRIM(CAST(Zip AS VARCHAR)) = '' THEN NULL
                    ELSE LPAD(CAST(Zip AS VARCHAR), 5, '0')
                END AS zip5
            FROM read_parquet('{input_glob}', filename=true)
        ), tagged AS (
            SELECT
                cycle,
                total_amount,
                raw_zip,
                zip5,
                l.fips,
                l.assignment_method,
                CASE
                    WHEN raw_zip IS NULL OR TRIM(raw_zip) = '' THEN 'blank_or_null'
                    WHEN zip5 = '00000' THEN '00000'
                    WHEN TRY_CAST(raw_zip AS BIGINT) IS NULL THEN 'non_numeric'
                    WHEN l.fips IS NOT NULL THEN 'mapped'
                    ELSE 'unmatched_numeric'
                END AS zip_status
            FROM all_rows a
            LEFT JOIN read_parquet('{lookup_path}') l
                ON a.zip5 = l.zipcode
        )
    """

    overall = con.execute(base_cte + """
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
    """).df()
    overall.to_csv(REPORT_DIR / 'county_mapping_overall.csv', index=False)

    by_cycle = con.execute(base_cte + """
        SELECT
            cycle,
            zip_status,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cycle), 4) AS row_share_pct,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (PARTITION BY cycle), 4) AS dollar_share_pct
        FROM tagged
        GROUP BY 1, 2
        ORDER BY cycle, rows DESC
    """).df()
    by_cycle.to_csv(REPORT_DIR / 'county_mapping_by_cycle.csv', index=False)

    by_method = con.execute(base_cte + """
        SELECT
            COALESCE(assignment_method, 'unresolved') AS assignment_method,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 4) AS dollar_share_pct
        FROM tagged
        GROUP BY 1
        ORDER BY dollars DESC
    """).df()
    by_method.to_csv(REPORT_DIR / 'county_mapping_by_method.csv', index=False)

    unmatched_leaderboard = con.execute(base_cte + """
        SELECT
            COALESCE(zip5, '<null>') AS zip5,
            zip_status,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            AVG(total_amount) AS avg_amount
        FROM tagged
        WHERE zip_status IN ('unmatched_numeric', 'non_numeric')
        GROUP BY 1, 2
        ORDER BY dollars DESC, rows DESC
        LIMIT 200
    """).df()
    unmatched_leaderboard.to_csv(REPORT_DIR / 'unmatched_zip_leaderboard.csv', index=False)

    taxonomy = con.execute(base_cte + """
        SELECT
            zip_status AS issue_type,
            COUNT(*) AS rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 4) AS dollar_share_pct
        FROM tagged
        GROUP BY 1
        ORDER BY dollars DESC
    """).df()
    taxonomy.to_csv(REPORT_DIR / 'unmatched_zip_taxonomy.csv', index=False)

    print('Wrote county diagnostics to', REPORT_DIR)


def main() -> None:
    parser = argparse.ArgumentParser(description='Build v1.001 geographic individual panel and county diagnostics.')
    parser.add_argument('--input-suffix', default='', help='Optional suffix for Stage 6 enriched inputs, e.g. _v1001')
    parser.add_argument('--output-name', default='indiv_geography_panel_v1001.parquet', help='Output parquet filename under output/')
    args = parser.parse_args()

    lookup_df = build_zip_fips_map_v1001()
    run_geo_panel(lookup_df, input_suffix=args.input_suffix, output_name=args.output_name)
    build_diagnostics(input_suffix=args.input_suffix)


if __name__ == '__main__':
    main()
