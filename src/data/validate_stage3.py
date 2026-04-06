"""Validate Stage 3 (Pre-aggregation) outputs across all cycles.

This script checks that aggregated Parquet files exist for all cycles and
spot-checks aggregation correctness by comparing row counts and total amounts.

Usage (from repo root):
    python -m src.data.validate_stage3
"""
#%%
from __future__ import annotations

from pathlib import Path

import pandas as pd
#%%
# Two levels up from src/data/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
DERIVED_DIR = PROJECT_ROOT / "data" / "derived"


def _discover_cycles(clean_dir: Path) -> list[str]:
    """Find all 2-digit cycle directories under data/clean/."""
    if not clean_dir.exists():
        return []
    cycles: list[str] = []
    for child in clean_dir.iterdir():
        if child.is_dir() and child.name.isdigit() and len(child.name) == 2:
            cycles.append(child.name)
    return sorted(cycles)


def validate_cycle(cycle: str) -> dict[str, object]:
    """Validate aggregated outputs for a single cycle."""
    clean_cycle = CLEAN_DIR / cycle
    derived_cycle = DERIVED_DIR / cycle
    
    indivs_clean = clean_cycle / "indivs.parquet"
    pacs_clean = clean_cycle / "pacs.parquet"
    indivs_agg = derived_cycle / "indivs_agg.parquet"
    pacs_agg = derived_cycle / "pacs_agg.parquet"
    
    result: dict[str, object] = {"cycle": cycle}
    
    # Check file existence
    result["has_clean_indivs"] = indivs_clean.exists()
    result["has_clean_pacs"] = pacs_clean.exists()
    result["has_agg_indivs"] = indivs_agg.exists()
    result["has_agg_pacs"] = pacs_agg.exists()
    
    # Validate indivs
    if indivs_clean.exists() and indivs_agg.exists():
        df_clean = pd.read_parquet(indivs_clean)
        df_agg = pd.read_parquet(indivs_agg)
        
        result["indivs_clean_rows"] = len(df_clean)
        result["indivs_agg_rows"] = len(df_agg)
        result["indivs_reduction_pct"] = round(
            100 * (1 - len(df_agg) / len(df_clean)) if len(df_clean) > 0 else 0, 1
        )
        
        # Compare total amounts (clean Amount may be string, convert)
        clean_amount = pd.to_numeric(df_clean["Amount"], errors="coerce").sum()
        agg_amount = df_agg["total_amount"].sum()
        result["indivs_clean_total"] = round(clean_amount, 2)
        result["indivs_agg_total"] = round(agg_amount, 2)
        result["indivs_total_diff_pct"] = round(
            100 * abs(clean_amount - agg_amount) / clean_amount if clean_amount != 0 else 0, 3
        )
    
    # Validate pacs
    if pacs_clean.exists() and pacs_agg.exists():
        df_clean = pd.read_parquet(pacs_clean)
        df_agg = pd.read_parquet(pacs_agg)
        
        result["pacs_clean_rows"] = len(df_clean)
        result["pacs_agg_rows"] = len(df_agg)
        result["pacs_reduction_pct"] = round(
            100 * (1 - len(df_agg) / len(df_clean)) if len(df_clean) > 0 else 0, 1
        )
        
        clean_amount = pd.to_numeric(df_clean["Amount"], errors="coerce").sum()
        agg_amount = df_agg["total_amount"].sum()
        result["pacs_clean_total"] = round(clean_amount, 2)
        result["pacs_agg_total"] = round(agg_amount, 2)
        result["pacs_total_diff_pct"] = round(
            100 * abs(clean_amount - agg_amount) / clean_amount if clean_amount != 0 else 0, 3
        )
    
    return result


def main() -> int:
    """Validate all Stage 3 outputs and write summary."""
    cycles = _discover_cycles(CLEAN_DIR)
    
    if not cycles:
        print(f"No cycles found under: {CLEAN_DIR}")
        return 1
    
    print(f"Validating Stage 3 outputs for {len(cycles)} cycles")
    
    rows = [validate_cycle(c) for c in cycles]
    df = pd.DataFrame(rows)
    
    # Write summary
    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_csv = reports_dir / "stage3_validation.csv"
    df.to_csv(out_csv, index=False)
    
    print(f"\nWrote: {out_csv}")
    
    # Flag issues
    missing_agg = df[(df["has_clean_indivs"] == True) & (df["has_agg_indivs"] == False)]["cycle"].tolist()
    missing_agg += df[(df["has_clean_pacs"] == True) & (df["has_agg_pacs"] == False)]["cycle"].tolist()
    
    if missing_agg:
        print(f"\n⚠️  Cycles missing aggregated outputs: {', '.join(set(missing_agg))}")
    else:
        print("\n✓ All clean cycles have corresponding aggregated outputs")
    
    # Check for large discrepancies in totals (>0.1%)
    issues = []
    for _, row in df.iterrows():
        if row.get("indivs_total_diff_pct", 0) > 0.1:
            issues.append(f"{row['cycle']} (indivs: {row['indivs_total_diff_pct']}% diff)")
        if row.get("pacs_total_diff_pct", 0) > 0.1:
            issues.append(f"{row['cycle']} (pacs: {row['pacs_total_diff_pct']}% diff)")
    
    if issues:
        print(f"\n⚠️  Cycles with >0.1% total amount discrepancy: {', '.join(issues)}")
    else:
        print("✓ All aggregated totals match clean totals (within 0.1%)")
    
    # Show preview
    print("\nPreview (first 10 cycles):")
    preview_cols = [
        "cycle", "has_agg_indivs", "has_agg_pacs",
        "indivs_reduction_pct", "pacs_reduction_pct",
        "indivs_total_diff_pct", "pacs_total_diff_pct"
    ]
    preview_cols = [c for c in preview_cols if c in df.columns]
    
    with pd.option_context("display.max_columns", 20, "display.width", 140):
        print(df[preview_cols].head(10).to_string(index=False))
    
    return 0

#%%
if __name__ == "__main__":
    raise SystemExit(main())
