"""Integrity QA checks for campaign finance data."""
#%%
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _validate_identifier(df: pd.DataFrame, field: str) -> Dict[str, Any]:
    missing = df[field].isna().sum() if field in df.columns else None
    duplicates = df[field].duplicated().sum() if field in df.columns else None
    return {
        "check": f"identifier_{field}",
        "missing": missing,
        "duplicates": duplicates,
    }


def _validate_amount(df: pd.DataFrame) -> Dict[str, Any]:
    amounts = pd.to_numeric(df.get("Amount"), errors="coerce")
    invalid = amounts.isna().sum()
    negative = (amounts < 0).sum()
    return {"check": "amount", "invalid": int(invalid), "negative": int(negative)}


def _validate_required_fields(df: pd.DataFrame, fields: List[str]) -> List[Dict[str, Any]]:
    results = []
    for f in fields:
        if f not in df.columns:
            results.append({"check": f"missing_column_{f}", "value": True})
        else:
            missing = df[f].isna().sum()
            results.append({"check": f"missing_values_{f}", "count": int(missing)})
    return results


def run_integrity_qa(cycle: str) -> Path:
    """Run QA checks on cleaned Parquet files for a cycle.

    Parameters
    ----------
    cycle:
        Election cycle (e.g., "1990").

    Returns
    -------
    Path
        Path to the generated CSV report.
    """
    cycle = cycle[-2:]
    clean_dir = PROJECT_ROOT / "data" / "clean" / cycle
    report_path = REPORT_DIR / f"qa_{cycle}.csv"
    results: List[Dict[str, Any]] = []

    for name in ["indivs", "pacs"]:
        file_path = clean_dir / f"{name}.parquet"
        if not file_path.exists():
            continue
        df = pd.read_parquet(file_path)
        results.append(_validate_amount(df))
        if name == "indivs":
            id_fields = ["ContribID"]
            required = ["Cycle", "Amount", "ContribID"]
        else:
            id_fields = ["PACID"]
            required = ["Cycle", "Amount", "PACID"]
        for f in id_fields:
            results.append(_validate_identifier(df, f))
        results.extend(_validate_required_fields(df, required))

    if results:
        pd.DataFrame(results).to_csv(report_path, index=False)
    return report_path

