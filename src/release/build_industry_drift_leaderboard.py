from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = PROJECT_ROOT / 'reports' / 'v1_001'
OUT_PATH = REPORT_DIR / 'industry_drift_leaderboard.csv'


def main(contrib_name: str = 'contributions_v1001.parquet') -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    contrib_path = str((PROJECT_ROOT / 'output' / contrib_name).as_posix())
    df = con.execute(
        f"""
        WITH industry_cycle AS (
            SELECT
                cycle,
                CAST(naics3 AS VARCHAR) AS naics3,
                ANY_VALUE(naics3_name) AS naics3_name,
                SUM(CASE WHEN recip_party = 'D' THEN total_amount ELSE 0 END) AS dem_dollars,
                SUM(CASE WHEN recip_party = 'R' THEN total_amount ELSE 0 END) AS rep_dollars,
                SUM(total_amount) AS total_dollars
            FROM '{contrib_path}'
            WHERE nature = 'B'
              AND recip_party IN ('D', 'R')
              AND naics3 IS NOT NULL
            GROUP BY 1, 2
        ), endpoints AS (
            SELECT
                naics3,
                ANY_VALUE(naics3_name) AS naics3_name,
                MAX(CASE WHEN cycle = 90 THEN 100.0 * dem_dollars / NULLIF(dem_dollars + rep_dollars, 0) END) AS dshare_1990,
                MAX(CASE WHEN cycle = 22 THEN 100.0 * dem_dollars / NULLIF(dem_dollars + rep_dollars, 0) END) AS dshare_2022,
                SUM(total_dollars) AS lifetime_dollars,
                AVG(total_dollars) AS avg_cycle_dollars,
                COUNT(*) AS observed_cycles
            FROM industry_cycle
            GROUP BY 1
        )
        SELECT
            naics3,
            naics3_name,
            ROUND(dshare_1990, 6) AS dshare_1990,
            ROUND(dshare_2022, 6) AS dshare_2022,
            ROUND(dshare_2022 - dshare_1990, 6) AS delta_pp,
            lifetime_dollars,
            ROUND(lifetime_dollars / 1000000000.0, 6) AS lifetime_dollars_bil,
            avg_cycle_dollars,
            observed_cycles,
            CASE WHEN lifetime_dollars >= 100000000 THEN TRUE ELSE FALSE END AS meets_100m_threshold
        FROM endpoints
        WHERE dshare_1990 IS NOT NULL AND dshare_2022 IS NOT NULL
        ORDER BY delta_pp DESC, lifetime_dollars DESC
        """
    ).df()
    df.to_csv(OUT_PATH, index=False)
    print(f'Saved {OUT_PATH}')
    print(df.head(10).to_string(index=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build v1.001 industry drift leaderboard.')
    parser.add_argument('--contrib-name', default='contributions_v1001.parquet', help='Parquet file under output/ to summarize.')
    args = parser.parse_args()
    main(contrib_name=args.contrib_name)
