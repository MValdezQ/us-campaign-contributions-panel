from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOOKUPS_DIR = PROJECT_ROOT / "data" / "lookups"
VALIDATION_DIR = PROJECT_ROOT / "data" / "validation"

ASSIGNMENT_PATH = LOOKUPS_DIR / "org_naics_assignment.csv"
VALIDATION_SAMPLE_PATH = VALIDATION_DIR / "org_naics_validation_sample_v1001.csv"
CANDIDATES_PATH = LOOKUPS_DIR / "catcode_naics_candidates.csv"
OVERRIDE_OUT_PATH = VALIDATION_DIR / "org_naics_validation_overrides_v1001_production_safe.csv"
CORRECTED_ASSIGNMENT_OUT_PATH = LOOKUPS_DIR / "org_naics_assignment_validated_v1001.csv"

EXCLUDED_SET_CODE_STATUSES = {
    "pending_review",
    "insufficient_public_evidence",
    "cluster_split_required",
}
SPECIAL_CODE_STATUSES = {"special_code_confirmed", "special_code_revised"}

MANUAL_NAICS3_NAMES = {
    "211": "Oil and Gas Extraction",
    "212": "Mining (except Oil and Gas)",
    "213": "Support Activities for Mining",
    "221": "Utilities",
    "311": "Food Manufacturing",
    "321": "Wood Product Manufacturing",
    "322": "Paper Manufacturing",
    "323": "Printing and Related Support Activities",
    "325": "Chemical Manufacturing",
    "326": "Plastics and Rubber Products Manufacturing",
    "327": "Nonmetallic Mineral Product Manufacturing",
    "332": "Fabricated Metal Product Manufacturing",
    "333": "Machinery Manufacturing",
    "334": "Computer and Electronic Product Manufacturing",
    "335": "Electrical Equipment, Appliance, and Component Manufacturing",
    "336": "Transportation Equipment Manufacturing",
    "337": "Furniture and Related Product Manufacturing",
    "339": "Miscellaneous Manufacturing",
    "423": "Merchant Wholesalers, Durable Goods",
    "424": "Merchant Wholesalers, Nondurable Goods",
    "442": "Furniture and Home Furnishings Stores",
    "443": "Electronics and Appliance Stores",
    "446": "Health and Personal Care Stores",
    "448": "Clothing and Clothing Accessories Stores",
    "451": "Sporting Goods, Hobby, Book, and Music Stores",
    "484": "Truck Transportation",
    "485": "Transit and Ground Passenger Transportation",
    "487": "Scenic and Sightseeing Transportation",
    "511": "Publishing Industries",
    "512": "Motion Picture and Sound Recording Industries",
    "517": "Telecommunications",
    "518": "Data Processing, Hosting, and Related Services",
    "522": "Credit Intermediation and Related Activities",
    "523": "Securities, Commodity Contracts, and Other Financial Investments",
    "525": "Funds, Trusts, and Other Financial Vehicles",
    "531": "Real Estate",
    "541": "Professional, Scientific, and Technical Services",
    "551": "Management of Companies and Enterprises",
    "561": "Administrative and Support Services",
    "611": "Educational Services",
    "621": "Ambulatory Health Care Services",
    "623": "Nursing and Residential Care Facilities",
    "721": "Accommodation",
    "722": "Food Services and Drinking Places",
    "812": "Personal and Laundry Services",
    "813": "Religious, Grantmaking, Civic, Professional, and Similar Organizations",
}


def normalize_code(value: object) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def build_naics_name_lookup(candidates_path: Path = CANDIDATES_PATH) -> dict[str, str]:
    lookup = dict(MANUAL_NAICS3_NAMES)
    candidates = pd.read_csv(candidates_path, dtype=str).fillna("")
    candidates["naics3"] = candidates["naics3"].map(normalize_code)
    for _, row in candidates.iterrows():
        code = row["naics3"]
        name = row.get("naics3_name", "").strip()
        if code and name and code not in lookup:
            lookup[code] = name
    return lookup


def load_inputs(sample_path: Path, assignment_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    sample = pd.read_csv(sample_path, dtype=str).fillna("")
    assignments = pd.read_csv(assignment_path, dtype=str).fillna("")
    return sample, assignments


def prepare_sample(sample: pd.DataFrame) -> pd.DataFrame:
    prepared = sample.copy()
    prepared["sample_assigned_naics3"] = prepared["assigned_naics3"].map(normalize_code)
    prepared["validated_naics3"] = prepared["validated_naics3"].map(normalize_code)
    prepared["validated_naics2"] = prepared["validated_naics2"].map(normalize_code)
    mismatch_mask = prepared["validated_naics3"].ne("") & prepared["validated_naics2"].ne(prepared["validated_naics3"].str[:2])
    if mismatch_mask.any():
        bad_ids = ", ".join(prepared.loc[mismatch_mask, "sample_id"].tolist())
        print(f"[WARN] Normalizing validated_naics2 to match validated_naics3 for: {bad_ids}")
        prepared.loc[mismatch_mask, "validated_naics2"] = prepared.loc[mismatch_mask, "validated_naics3"].str[:2]
    prepared.loc[prepared["validated_naics2"].eq(""), "validated_naics2"] = prepared["validated_naics3"].str[:2]
    prepared["org_level_dollars"] = pd.to_numeric(prepared["org_level_dollars"], errors="coerce").fillna(0.0)
    prepared["org_level_rows"] = pd.to_numeric(prepared["org_level_rows"], errors="coerce").fillna(0).astype(int)
    return prepared


def prepare_assignments(assignments: pd.DataFrame) -> pd.DataFrame:
    prepared = assignments.copy()
    prepared["current_assigned_naics3"] = prepared["assigned_naics3"].map(normalize_code)
    return prepared


def build_override_rows(sample: pd.DataFrame, assignments: pd.DataFrame, naics_name_lookup: dict[str, str]) -> pd.DataFrame:
    assignment_view = assignments[
        [
            "org_id",
            "orgname",
            "current_assigned_naics3",
            "assigned_naics3_name",
            "assignment_method",
            "assignment_confidence",
            "llm_consensus",
            "llm_rounds",
        ]
    ].rename(
        columns={
            "orgname": "current_orgname",
            "assigned_naics3_name": "current_assigned_naics3_name",
            "assignment_method": "current_assignment_method",
            "assignment_confidence": "current_assignment_confidence",
            "llm_consensus": "current_llm_consensus",
            "llm_rounds": "current_llm_rounds",
        }
    )

    merged = sample.merge(assignment_view, on="org_id", how="left", validate="one_to_one")
    merged["current_assigned_naics3"] = merged["current_assigned_naics3"].map(normalize_code)
    merged["current_assigned_naics3_name"] = merged["current_assigned_naics3_name"].fillna("")

    drift = merged[
        merged["current_assigned_naics3"].ne("")
        & merged["sample_assigned_naics3"].ne("")
        & merged["current_assigned_naics3"].ne(merged["sample_assigned_naics3"])
    ]
    if not drift.empty:
        sample_ids = ", ".join(drift["sample_id"].head(10))
        raise ValueError(
            "Validation sample assignments drifted from data/lookups/org_naics_assignment.csv "
            f"for: {sample_ids}"
        )

    set_code_mask = (
        merged["validation_status"].ne("")
        & ~merged["validation_status"].isin(EXCLUDED_SET_CODE_STATUSES)
        & merged["validated_naics3"].ne("")
        & merged["validated_naics3"].ne(merged["current_assigned_naics3"])
    )
    clear_special_mask = (
        merged["validation_status"].isin(SPECIAL_CODE_STATUSES)
        & (
            merged["current_assigned_naics3"].ne("")
            | merged["current_assignment_method"].ne("special_code")
        )
    )

    overrides = merged[set_code_mask | clear_special_mask].copy()
    duplicate_orgs = overrides[overrides["org_id"].duplicated(keep=False)]
    if not duplicate_orgs.empty:
        dupes = ", ".join(sorted(duplicate_orgs["org_id"].unique())[:10])
        raise ValueError(f"Override artifact would contain duplicate org_id values: {dupes}")

    overrides["validated_naics3_name"] = overrides["validated_naics3"].map(lambda code: naics_name_lookup.get(code, ""))
    overrides["override_action"] = "set_validated_code"
    overrides.loc[overrides["current_assigned_naics3"].eq(""), "override_action"] = "fill_missing_naics3"
    overrides.loc[overrides["validation_status"].isin(SPECIAL_CODE_STATUSES), "override_action"] = "clear_to_special_code"
    overrides["override_source"] = "org_naics_validation_sample_v1001"
    overrides["override_assignment_method"] = "validation_override"
    overrides["override_assignment_confidence"] = "HIGH"
    overrides["override_reason"] = "validated_naics3_differs_from_current_assignment"
    overrides["apply_override"] = True

    special_mask = overrides["override_action"].eq("clear_to_special_code")
    overrides.loc[special_mask, "validated_naics3"] = ""
    overrides.loc[special_mask, "validated_naics3_name"] = ""
    overrides.loc[special_mask, "override_assignment_method"] = "special_code"
    overrides.loc[special_mask, "override_assignment_confidence"] = ""
    overrides.loc[special_mask, "override_reason"] = "validated_special_code_no_naics"

    missing_names = overrides[
        overrides["override_action"].ne("clear_to_special_code")
        & overrides["validated_naics3_name"].eq("")
    ]
    if not missing_names.empty:
        sample_ids = ", ".join(missing_names["sample_id"].head(10))
        raise ValueError(f"Missing validated NAICS3 names for override rows: {sample_ids}")

    export = overrides[
        [
            "org_id",
            "sample_id",
            "orgname",
            "current_orgname",
            "current_assignment_method",
            "current_assignment_confidence",
            "current_llm_consensus",
            "current_llm_rounds",
            "current_assigned_naics3",
            "current_assigned_naics3_name",
            "sample_assigned_naics3",
            "assigned_naics3_name",
            "validated_naics3",
            "validated_naics3_name",
            "validated_naics2",
            "override_action",
            "override_source",
            "override_assignment_method",
            "override_assignment_confidence",
            "override_reason",
            "apply_override",
            "validation_status",
            "adjudicator",
            "review_date",
            "evidence_source_1",
            "evidence_source_2",
            "review_notes",
            "assignment_method",
            "llm_consensus",
            "sampling_stratum",
            "org_level_rows",
            "org_level_dollars",
        ]
    ].rename(
        columns={
            "assigned_naics3_name": "sample_assigned_naics3_name",
            "assignment_method": "sample_assignment_method",
            "llm_consensus": "sample_llm_consensus",
        }
    )
    return export.sort_values(["org_level_dollars", "sample_id"], ascending=[False, True]).reset_index(drop=True)


def build_corrected_assignment(assignments: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    corrected = assignments.copy()
    base_columns = corrected.columns.tolist()
    if overrides.empty:
        return corrected[base_columns]

    mergeable = overrides[
        [
            "org_id",
            "validated_naics3",
            "validated_naics3_name",
            "override_assignment_method",
            "override_assignment_confidence",
            "apply_override",
        ]
    ].rename(
        columns={
            "validated_naics3": "target_assigned_naics3",
            "validated_naics3_name": "target_assigned_naics3_name",
            "override_assignment_method": "target_assignment_method",
            "override_assignment_confidence": "target_assignment_confidence",
        }
    )
    corrected = corrected.merge(mergeable, on="org_id", how="left")
    apply_mask = corrected["apply_override"].map(normalize_bool)
    corrected.loc[apply_mask, "assigned_naics3"] = corrected.loc[apply_mask, "target_assigned_naics3"]
    corrected.loc[apply_mask, "assigned_naics3_name"] = corrected.loc[apply_mask, "target_assigned_naics3_name"]
    corrected.loc[apply_mask, "assignment_method"] = corrected.loc[apply_mask, "target_assignment_method"]
    corrected.loc[apply_mask, "assignment_confidence"] = corrected.loc[apply_mask, "target_assignment_confidence"]
    corrected.loc[apply_mask, "llm_consensus"] = ""
    corrected.loc[apply_mask, "llm_rounds"] = ""
    return corrected[base_columns]


def main(
    sample_path: Path = VALIDATION_SAMPLE_PATH,
    assignment_path: Path = ASSIGNMENT_PATH,
    out_path: Path = OVERRIDE_OUT_PATH,
    corrected_assignment_out_path: Path = CORRECTED_ASSIGNMENT_OUT_PATH,
    write_corrected_assignment: bool = True,
) -> None:
    sample, assignments = load_inputs(sample_path=sample_path, assignment_path=assignment_path)
    sample = prepare_sample(sample)
    assignments = prepare_assignments(assignments)
    naics_name_lookup = build_naics_name_lookup()
    overrides = build_override_rows(sample=sample, assignments=assignments, naics_name_lookup=naics_name_lookup)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    overrides.to_csv(out_path, index=False)
    print(f"Saved {out_path} ({len(overrides):,} rows)")
    print(f"Validation statuses included: {overrides['validation_status'].value_counts().to_dict()}")
    print(f"Override actions included: {overrides['override_action'].value_counts().to_dict()}")

    if write_corrected_assignment:
        corrected_assignment_out_path.parent.mkdir(parents=True, exist_ok=True)
        corrected = build_corrected_assignment(assignments, overrides)
        corrected.to_csv(corrected_assignment_out_path, index=False)
        print(f"Saved {corrected_assignment_out_path}")

    print("Selection rule: apply concrete validated NAICS changes plus reviewed special-code resets; exclude pending_review, insufficient_public_evidence, and cluster_split_required.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build production-safe validated org-NAICS override artifacts.")
    parser.add_argument("--sample-path", type=Path, default=VALIDATION_SAMPLE_PATH, help="Completed validation sample CSV.")
    parser.add_argument("--assignment-path", type=Path, default=ASSIGNMENT_PATH, help="Current org_naics_assignment.csv baseline.")
    parser.add_argument("--out-path", type=Path, default=OVERRIDE_OUT_PATH, help="Override artifact CSV output path.")
    parser.add_argument(
        "--corrected-assignment-out-path",
        type=Path,
        default=CORRECTED_ASSIGNMENT_OUT_PATH,
        help="Full corrected assignment CSV output path.",
    )
    parser.add_argument(
        "--no-corrected-assignment",
        action="store_true",
        help="Only write the override artifact and skip the corrected assignment export.",
    )
    args = parser.parse_args()
    main(
        sample_path=args.sample_path,
        assignment_path=args.assignment_path,
        out_path=args.out_path,
        corrected_assignment_out_path=args.corrected_assignment_out_path,
        write_corrected_assignment=not args.no_corrected_assignment,
    )
