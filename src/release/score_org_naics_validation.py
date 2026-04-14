from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SAMPLE = PROJECT_ROOT / 'data' / 'validation' / 'org_naics_validation_sample_v1001.csv'
OUT_DIR = PROJECT_ROOT / 'reports' / 'v1_001' / 'validation'
METRICS_PATH = OUT_DIR / 'org_naics_validation_metrics.csv'
CONFUSION_PATH = OUT_DIR / 'org_naics_validation_confusion.csv'
SUMMARY_PATH = OUT_DIR / 'org_naics_validation_summary.md'
CLUSTER_PATH = OUT_DIR / 'org_naics_validation_cluster_issues.csv'


def normalize_code(value: object, digits: int) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ''
    text = str(value).strip()
    if not text:
        return ''
    if text.endswith('.0'):
        text = text[:-2]
    return text[:digits]


def build_metrics(df: pd.DataFrame, group_cols: list[str], label: str) -> pd.DataFrame:
    rows = []
    for keys, grp in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        record = {col: key for col, key in zip(group_cols, keys)}
        record['scope'] = label
        record['n_reviewed'] = len(grp)
        record['exact_3digit_match_rate'] = round(100.0 * grp['match_3digit'].mean(), 4)
        record['exact_2digit_match_rate'] = round(100.0 * grp['match_2digit'].mean(), 4)
        record['weighted_3digit_match_rate'] = round(100.0 * (grp['match_3digit'] * grp['org_level_dollars'].clip(lower=1.0)).sum() / grp['org_level_dollars'].clip(lower=1.0).sum(), 4)
        record['weighted_2digit_match_rate'] = round(100.0 * (grp['match_2digit'] * grp['org_level_dollars'].clip(lower=1.0)).sum() / grp['org_level_dollars'].clip(lower=1.0).sum(), 4)
        rows.append(record)
    return pd.DataFrame(rows)


def main(sample_path: Path = DEFAULT_SAMPLE) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(sample_path)
    for col in ['assigned_naics3', 'validated_naics3', 'validated_naics2', 'llm_consensus']:
        if col not in df.columns:
            raise ValueError(f'Missing required column: {col}')

    cluster_issues = df[df['validation_status'].fillna('') == 'cluster_split_required'].copy()
    reviewable = df[df['validation_status'].fillna('') != 'cluster_split_required'].copy()
    reviewed = reviewable[
        reviewable['validation_status'].fillna('').ne('pending_review')
        | reviewable['validated_naics3'].fillna('').astype(str).str.strip().ne('')
    ].copy()

    cluster_cols = [
        col for col in [
            'sample_id', 'org_id', 'orgname', 'assignment_method', 'llm_consensus',
            'assigned_naics3', 'assigned_naics3_name', 'validation_status',
            'evidence_source_1', 'evidence_source_2', 'adjudicator', 'review_date', 'review_notes'
        ] if col in df.columns
    ]
    cluster_issues[cluster_cols].to_csv(CLUSTER_PATH, index=False)

    if reviewed.empty:
        pd.DataFrame(columns=['scope', 'n_reviewed', 'exact_3digit_match_rate', 'exact_2digit_match_rate']).to_csv(METRICS_PATH, index=False)
        pd.DataFrame(columns=['assigned_naics3', 'validated_naics3', 'n']).to_csv(CONFUSION_PATH, index=False)
        summary = [
            '# Validation summary',
            '',
            'No reviewed rows were available yet.',
            '',
            f'- Cluster-split issues logged: {len(cluster_issues)}',
            f'- Cluster issue file: `{CLUSTER_PATH.relative_to(PROJECT_ROOT).as_posix()}`',
        ]
        SUMMARY_PATH.write_text('\n'.join(summary) + '\n', encoding='utf-8')
        print('No adjudicated rows yet; wrote empty validation outputs.')
        return

    reviewed['assigned_naics3_norm'] = reviewed['assigned_naics3'].map(lambda v: normalize_code(v, 3))
    reviewed['validated_naics3_norm'] = reviewed['validated_naics3'].map(lambda v: normalize_code(v, 3))
    reviewed['assigned_naics2_norm'] = reviewed['assigned_naics3_norm'].str[:2]
    reviewed['validated_naics2_norm'] = reviewed['validated_naics2'].map(lambda v: normalize_code(v, 2))
    reviewed.loc[reviewed['validated_naics2_norm'] == '', 'validated_naics2_norm'] = reviewed['validated_naics3_norm'].str[:2]
    reviewed['match_3digit'] = (reviewed['assigned_naics3_norm'] == reviewed['validated_naics3_norm']).astype(int)
    reviewed['match_2digit'] = (reviewed['assigned_naics2_norm'] == reviewed['validated_naics2_norm']).astype(int)

    overall = pd.DataFrame([{
        'scope': 'overall',
        'n_reviewed': len(reviewed),
        'exact_3digit_match_rate': round(100.0 * reviewed['match_3digit'].mean(), 4),
        'exact_2digit_match_rate': round(100.0 * reviewed['match_2digit'].mean(), 4),
        'weighted_3digit_match_rate': round(100.0 * (reviewed['match_3digit'] * reviewed['org_level_dollars'].clip(lower=1.0)).sum() / reviewed['org_level_dollars'].clip(lower=1.0).sum(), 4),
        'weighted_2digit_match_rate': round(100.0 * (reviewed['match_2digit'] * reviewed['org_level_dollars'].clip(lower=1.0)).sum() / reviewed['org_level_dollars'].clip(lower=1.0).sum(), 4),
    }])
    by_method = build_metrics(reviewed, ['assignment_method'], 'by_assignment_method')
    llm_only = reviewed[reviewed['assignment_method'] == 'llm_assigned'].copy()
    by_consensus = build_metrics(llm_only, ['llm_consensus'], 'by_llm_consensus') if not llm_only.empty else pd.DataFrame()

    metrics = pd.concat([overall, by_method, by_consensus], ignore_index=True, sort=False)
    metrics.to_csv(METRICS_PATH, index=False)

    confusion = (
        reviewed.groupby(['assigned_naics3_norm', 'validated_naics3_norm'])
        .size()
        .reset_index(name='n')
        .rename(columns={'assigned_naics3_norm': 'assigned_naics3', 'validated_naics3_norm': 'validated_naics3'})
        .sort_values(['n', 'assigned_naics3', 'validated_naics3'], ascending=[False, True, True])
    )
    confusion.to_csv(CONFUSION_PATH, index=False)

    summary = [
        '# Validation summary',
        '',
        f'- Reviewed rows: {len(reviewed)}',
        f'- Cluster-split issues excluded from scoring: {len(cluster_issues)}',
        f"- Exact 3-digit match: {overall.iloc[0]['exact_3digit_match_rate']}%",
        f"- Exact 2-digit match: {overall.iloc[0]['exact_2digit_match_rate']}%",
        f"- Weighted 3-digit match: {overall.iloc[0]['weighted_3digit_match_rate']}%",
        f"- Weighted 2-digit match: {overall.iloc[0]['weighted_2digit_match_rate']}%",
        '',
        '## Files',
        f'- Metrics: `{METRICS_PATH.relative_to(PROJECT_ROOT).as_posix()}`',
        f'- Confusion: `{CONFUSION_PATH.relative_to(PROJECT_ROOT).as_posix()}`',
        f'- Cluster issues: `{CLUSTER_PATH.relative_to(PROJECT_ROOT).as_posix()}`',
    ]
    SUMMARY_PATH.write_text('\n'.join(summary) + '\n', encoding='utf-8')
    print(f'Saved {METRICS_PATH}')
    print(f'Saved {CONFUSION_PATH}')
    print(f'Saved {SUMMARY_PATH}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Score adjudicated org-level NAICS validation sample.')
    parser.add_argument('--sample-path', type=Path, default=DEFAULT_SAMPLE, help='Validation sample CSV to score.')
    args = parser.parse_args()
    main(sample_path=args.sample_path)
