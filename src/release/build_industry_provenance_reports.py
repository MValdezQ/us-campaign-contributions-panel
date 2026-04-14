from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = PROJECT_ROOT / 'reports' / 'v1_001'
ORG_ASSIGN_PATH = str((PROJECT_ROOT / 'data' / 'lookups' / 'org_naics_assignment_validated_v1001.csv').as_posix())
ORG_ASSIGN_ENV_VAR = 'ORG_NAICS_ASSIGNMENT_PATH'


def write_csv(df: pd.DataFrame, name: str) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(REPORT_DIR / name, index=False)


def resolve_assignment_path(path_value: str | None) -> str:
    if not path_value:
        return ORG_ASSIGN_PATH
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate.resolve().as_posix()


def main(contrib_name: str = 'contributions.parquet', assignment_path: str = ORG_ASSIGN_PATH) -> None:
    con = duckdb.connect()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    contrib_path = str((PROJECT_ROOT / 'output' / contrib_name).as_posix())

    org_summary = con.execute(
        f"""
        SELECT
            assignment_method,
            COUNT(*) AS org_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS org_share_pct,
            SUM(CASE WHEN assignment_method = 'llm_assigned' THEN 1 ELSE 0 END) OVER () AS total_llm_orgs
        FROM read_csv_auto('{assignment_path}')
        GROUP BY 1
        ORDER BY org_count DESC
        """
    ).df()
    write_csv(org_summary, 'industry_assignment_org_summary.csv')

    dollar_summary = con.execute(
        f"""
        WITH panel AS (
            SELECT * FROM '{contrib_path}'
        ), totals AS (
            SELECT
                SUM(total_amount) AS panel_total_dollars,
                SUM(CASE WHEN naics_source = 'org_level' THEN total_amount ELSE 0 END) AS org_level_total_dollars
            FROM panel
        )
        SELECT
            COALESCE(org_assignment_method, '<null>') AS org_assignment_method,
            COUNT(*) AS contribution_rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * SUM(total_amount) / MAX(panel_total_dollars), 4) AS panel_dollar_share_pct,
            ROUND(100.0 * SUM(total_amount) / NULLIF(MAX(org_level_total_dollars), 0), 4) AS org_level_dollar_share_pct,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS row_share_pct
        FROM panel, totals
        WHERE naics_source = 'org_level'
        GROUP BY 1
        ORDER BY dollars DESC
        """
    ).df()
    write_csv(dollar_summary, 'industry_assignment_dollar_summary.csv')

    llm_dollar_summary = con.execute(
        f"""
        SELECT
            COALESCE(org_llm_consensus, '<null>') AS org_llm_consensus,
            COUNT(*) AS contribution_rows,
            SUM(total_amount) AS dollars,
            ROUND(100.0 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 4) AS dollar_share_pct,
            ROUND(AVG(total_amount), 4) AS avg_amount
        FROM '{contrib_path}'
        WHERE org_assignment_method = 'llm_assigned'
        GROUP BY 1
        ORDER BY dollars DESC
        """
    ).df()
    write_csv(llm_dollar_summary, 'llm_consensus_dollar_summary.csv')

    special_code_breakdown = con.execute(
        f"""
        SELECT
            CASE
                WHEN is_ideology THEN 'ideology'
                WHEN is_party THEN 'party'
                WHEN is_labor_union THEN 'labor_union'
                WHEN is_public_employee THEN 'public_employee'
                WHEN is_nonprofit THEN 'nonprofit'
                WHEN is_lobbyist THEN 'lobbyist'
                WHEN no_realcode THEN 'no_realcode'
                ELSE 'unclassified_special'
            END AS special_bucket,
            COUNT(*) AS org_count,
            COUNT(CASE WHEN assigned_naics3 IS NOT NULL THEN 1 END) AS orgs_with_naics,
            ROUND(AVG(top_realcode_share), 6) AS avg_top_realcode_share
        FROM read_csv_auto('{assignment_path}')
        WHERE assignment_method IN ('special_code', 'no_realcode', 'unknown')
        GROUP BY 1
        ORDER BY org_count DESC
        """
    ).df()
    write_csv(special_code_breakdown, 'special_code_breakdown.csv')

    print('Industry provenance reports written to', REPORT_DIR)
    for name in [
        'industry_assignment_org_summary.csv',
        'industry_assignment_dollar_summary.csv',
        'llm_consensus_dollar_summary.csv',
        'special_code_breakdown.csv',
    ]:
        print('-', name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build v1.001 industry provenance reports.')
    parser.add_argument('--contrib-name', default='contributions.parquet', help='Parquet file under output/ to summarize.')
    parser.add_argument(
        '--assignment-path',
        default=os.environ.get(ORG_ASSIGN_ENV_VAR, ''),
        help=(
            'Optional org assignment CSV path. '
            f'Defaults to data/lookups/org_naics_assignment_validated_v1001.csv or ${ORG_ASSIGN_ENV_VAR} if set.'
        ),
    )
    args = parser.parse_args()
    main(contrib_name=args.contrib_name, assignment_path=resolve_assignment_path(args.assignment_path))
