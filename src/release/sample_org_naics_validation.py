from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ORG_ASSIGN_PATH = str((PROJECT_ROOT / 'data' / 'lookups' / 'org_naics_assignment.csv').as_posix())
OUT_DIR = PROJECT_ROOT / 'data' / 'validation'
OUT_PATH = OUT_DIR / 'org_naics_validation_sample_v1001.csv'
SEED = 1001

TARGETS = {
    'manual_override': None,
    'hardcoded': 50,
    'deterministic': 75,
    'llm_assigned:UNANIMOUS': 75,
    'llm_assigned:MAJORITY': 50,
    'llm_assigned:MAJORITY_R2': 25,
    'special_code': 40,
}


def weighted_pick(df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    if n is None or n >= len(df):
        return df.copy()
    weights = np.log1p(df['org_level_dollars'].fillna(0))
    weights = weights + 1.0
    return df.sample(n=n, random_state=seed, weights=weights, replace=False)


def build_special_code_stratum(df: pd.DataFrame) -> pd.DataFrame:
    flag_columns = [
        ('is_ideology', 'ideology'),
        ('is_party', 'party'),
        ('is_labor_union', 'labor_union'),
        ('is_public_employee', 'public_employee'),
        ('is_nonprofit', 'nonprofit'),
        ('is_lobbyist', 'lobbyist'),
    ]
    tagged = df.copy()
    tagged['special_code_type'] = 'special_other'
    for col, label in flag_columns:
        tagged.loc[tagged[col] == True, 'special_code_type'] = label
    pieces = []
    per_group = max(TARGETS['special_code'] // max(tagged['special_code_type'].nunique(), 1), 1)
    for idx, (_, grp) in enumerate(tagged.groupby('special_code_type')):
        pieces.append(weighted_pick(grp, min(per_group, len(grp)), seed=SEED + idx))
    sampled = pd.concat(pieces, ignore_index=True).drop_duplicates('org_id')
    remaining = TARGETS['special_code'] - len(sampled)
    if remaining > 0:
        leftovers = tagged[~tagged['org_id'].isin(sampled['org_id'])]
        if not leftovers.empty:
            sampled = pd.concat([sampled, weighted_pick(leftovers, min(remaining, len(leftovers)), seed=SEED + 99)], ignore_index=True)
    return sampled


def main(contrib_name: str = 'contributions_v1001.parquet') -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    contrib_path = str((PROJECT_ROOT / 'output' / contrib_name).as_posix())
    dollars = con.execute(
        f"""
        SELECT
            CAST(org_id AS VARCHAR) AS org_id,
            COUNT(*) AS org_level_rows,
            SUM(total_amount) AS org_level_dollars
        FROM '{contrib_path}'
        WHERE naics_source = 'org_level' AND org_id IS NOT NULL
        GROUP BY 1
        """
    ).df()

    frame = pd.read_csv(ORG_ASSIGN_PATH)
    frame['org_id'] = frame['org_id'].astype(str)
    frame = frame.merge(dollars, on='org_id', how='left')
    frame['org_level_rows'] = frame['org_level_rows'].fillna(0).astype(int)
    frame['org_level_dollars'] = frame['org_level_dollars'].fillna(0.0)
    frame['llm_consensus'] = frame['llm_consensus'].fillna('')
    frame['sampling_stratum'] = frame['assignment_method']
    frame.loc[frame['assignment_method'] == 'llm_assigned', 'sampling_stratum'] = (
        frame['assignment_method'] + ':' + frame['llm_consensus'].replace('', 'UNSPECIFIED')
    )

    samples = []
    samples.append(frame[frame['assignment_method'] == 'manual_override'].copy())
    samples.append(weighted_pick(frame[frame['assignment_method'] == 'hardcoded'].copy(), TARGETS['hardcoded'], SEED + 1))
    samples.append(weighted_pick(frame[frame['assignment_method'] == 'deterministic'].copy(), TARGETS['deterministic'], SEED + 2))

    for idx, consensus in enumerate(['UNANIMOUS', 'MAJORITY', 'MAJORITY_R2'], start=10):
        grp = frame[(frame['assignment_method'] == 'llm_assigned') & (frame['llm_consensus'] == consensus)].copy()
        samples.append(weighted_pick(grp, TARGETS[f'llm_assigned:{consensus}'], SEED + idx))

    special = frame[frame['assignment_method'] == 'special_code'].copy()
    if not special.empty:
        samples.append(build_special_code_stratum(special))

    sample = pd.concat(samples, ignore_index=True).drop_duplicates('org_id').copy()
    sample = sample.sort_values(['assignment_method', 'org_level_dollars'], ascending=[True, False]).reset_index(drop=True)
    sample['sample_id'] = [f'V1001-{i:04d}' for i in range(1, len(sample) + 1)]
    sample['validated_naics3'] = ''
    sample['validated_naics2'] = ''
    sample['validation_status'] = 'pending_review'
    sample['evidence_source_1'] = ''
    sample['evidence_source_2'] = ''
    sample['adjudicator'] = ''
    sample['review_date'] = ''
    sample['review_notes'] = ''

    columns = [
        'sample_id', 'org_id', 'orgname', 'assignment_method', 'llm_consensus', 'sampling_stratum',
        'assigned_naics3', 'assigned_naics3_name', 'top_realcode', 'top_realcode_name', 'top_realcode_share',
        'org_level_rows', 'org_level_dollars', 'is_ideology', 'is_party', 'is_labor_union',
        'is_public_employee', 'is_nonprofit', 'is_lobbyist', 'validated_naics3', 'validated_naics2',
        'validation_status', 'evidence_source_1', 'evidence_source_2', 'adjudicator', 'review_date', 'review_notes'
    ]
    sample[columns].to_csv(OUT_PATH, index=False)
    print(f'Saved {OUT_PATH} ({len(sample):,} sampled orgs)')
    print(sample.groupby('assignment_method').size().to_string())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create v1.001 industry-validation sample.')
    parser.add_argument('--contrib-name', default='contributions_v1001.parquet', help='Parquet file under output/ to summarize.')
    args = parser.parse_args()
    main(contrib_name=args.contrib_name)
