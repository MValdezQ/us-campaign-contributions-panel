from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOOKUPS_DIR = PROJECT_ROOT / "data" / "lookups"
VALIDATION_DIR = PROJECT_ROOT / "data" / "validation"

ASSIGNMENT_PATH = LOOKUPS_DIR / "org_naics_assignment.csv"
OVERRIDE_PATH = VALIDATION_DIR / "org_naics_validation_overrides_v1001_production_safe_v2.csv"
OUT_PATH = VALIDATION_DIR / "org_naics_assignment_validated_v1001_preview_v2.csv"


def normalize_code(value: object) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def apply_overrides(assignments: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    corrected = assignments.copy()
    corrected["assigned_naics3"] = corrected["assigned_naics3"].map(normalize_code)

    override_view = overrides[
        [
            "org_id",
            "validated_naics3",
            "validated_naics3_name",
            "override_assignment_method",
            "override_assignment_confidence",
        ]
    ].copy()
    duplicate_orgs = override_view[override_view["org_id"].duplicated(keep=False)]
    if not duplicate_orgs.empty:
        dupes = ", ".join(sorted(duplicate_orgs["org_id"].unique())[:10])
        raise ValueError("Override artifact contains duplicate org_id values: " + dupes)

    merged = corrected.merge(override_view, on="org_id", how="left", validate="one_to_one")
    has_override = merged["validated_naics3"].fillna("").map(normalize_code).ne("")

    merged.loc[has_override, "assigned_naics3"] = merged.loc[has_override, "validated_naics3"].map(normalize_code)
    merged.loc[has_override, "assigned_naics3_name"] = merged.loc[has_override, "validated_naics3_name"].fillna("")
    merged.loc[has_override, "assignment_method"] = merged.loc[has_override, "override_assignment_method"].fillna("validation_override")
    merged.loc[has_override, "assignment_confidence"] = merged.loc[has_override, "override_assignment_confidence"].fillna("manual_validation_complete")
    merged.loc[has_override, "llm_consensus"] = ""
    if "llm_rounds" in merged.columns:
        merged.loc[has_override, "llm_rounds"] = ""

    return merged[assignments.columns.tolist()]


def main(assignment_path: Path = ASSIGNMENT_PATH, override_path: Path = OVERRIDE_PATH, out_path: Path = OUT_PATH) -> None:
    assignments = pd.read_csv(assignment_path, dtype=str).fillna("")
    overrides = pd.read_csv(override_path, dtype=str).fillna("")
    corrected = apply_overrides(assignments=assignments, overrides=overrides)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    corrected.to_csv(out_path, index=False)
    applied = int((corrected["assignment_method"] == "validation_override").sum())
    print(f"Saved {out_path} ({applied} rows tagged as validation_override)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply validated org-NAICS overrides to produce a preview assignment table.")
    parser.add_argument("--assignment-path", type=Path, default=ASSIGNMENT_PATH, help="Current org_naics_assignment.csv baseline.")
    parser.add_argument("--override-path", type=Path, default=OVERRIDE_PATH, help="Validated override artifact CSV.")
    parser.add_argument("--out-path", type=Path, default=OUT_PATH, help="Output CSV path for the preview assignment table.")
    args = parser.parse_args()
    main(assignment_path=args.assignment_path, override_path=args.override_path, out_path=args.out_path)