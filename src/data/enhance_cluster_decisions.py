"""Enhance cluster decision outputs with resolution types and RealCode distributions.

This script:
1) Reclassifies REJECT clusters into four resolution types
2) Adds data quality flags
3) Builds RealCode distributions per org_id

Inputs
------
- data/lookups/ready_to_review_top_13k_with_decisions.csv
- data/derived/<cycle>/indivs_agg.parquet
- data/derived/<cycle>/pacs_agg.parquet
- data/clean/<cycle>/committees.parquet

Outputs
-------
- data/lookups/org_aliases_top_13k_enhanced.csv
- data/lookups/org_aliases_top_13k_enhanced.parquet
- data/lookups/org_id_realcode_distribution_top_13k.csv
- data/lookups/org_id_realcode_distribution_top_13k.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DERIVED_DIR = PROJECT_ROOT / "data" / "derived"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
LOOKUPS_DIR = PROJECT_ROOT / "data" / "lookups"

# Trade association keywords for detecting real entities
TRADE_ASSOCIATION_KEYWORDS = [
    'assn', 'association', 'federation', 'union', 'council', 'coalition',
    'chamber', 'society', 'institute', 'foundation', 'committee', 'league',
    'alliance', 'consortium', 'corp', 'inc', 'llc', 'ltd', 'company', 'group'
]

# Pure occupation terms that are INDUSTRY_ONLY (individuals, not organizations)
OCCUPATION_TERMS = {
    'homemaker', 'physician', 'accountant', 'actor', 'musician', 'businessman',
    'ceo', 'veterinarian', 'pharmacist', 'businesswoman', 'electrician',
    'salesman', 'plumber', 'dentist', 'rancher', 'retired'
}

# Generic industry terms that are INDUSTRY_ONLY when they don't have specific entities
GENERIC_INDUSTRY_TERMS = {
    'attorney', 'lawyer', 'consultant', 'investor', 'writer', 'artist', 'farmer',
    'engineer', 'entrepreneur', 'sales', 'finance', 'architect', 'executive',
    'marketing', 'teacher', 'student', 'property manager', 'manager', 'filmmaker',
    'registered nurse', 'nurse', 'trader', 'broker', 'director', 'investment analyst',
    'analyst', 'president', 'education'
}

# Resolution mapping rules: (pattern_in_reasoning, resolution_type, flags)
RESOLUTION_RULES: List[Tuple[str, str, List[str]]] = [
    # Data quality issues: form codes and placeholders
    (
        "generic/placeholder: none",
        "DATA_QUALITY_ISSUE",
        ["placeholder_value", "missing_orgname"],
    ),
    (
        "generic/placeholder: [candidate contribution]",
        "DATA_QUALITY_ISSUE",
        ["form_code", "24k_contribution_type"],
    ),
    (
        "generic/placeholder: [24t contribution]",
        "DATA_QUALITY_ISSUE",
        ["form_code", "24t_contribution_type"],
    ),
    (
        "generic/placeholder: [24i contribution]",
        "DATA_QUALITY_ISSUE",
        ["form_code", "24i_contribution_type"],
    ),
    (
        "generic/placeholder: self-employed",
        "DATA_QUALITY_ISSUE",
        ["placeholder_value", "employment_status"],
    ),
    (
        "generic/placeholder: retired",
        "DATA_QUALITY_ISSUE",
        ["placeholder_value", "employment_status"],
    ),
    # Large false positives: originally NEEDS_SUBCLUSTERING, downgraded to INDUSTRY_ONLY
    # Decision (2026-02-21): strip org_id and treat as sector-level contributions.
    # These 2 clusters (investments=109 firms, construction=70 firms, $247M total)
    # represent unrelated companies incorrectly grouped by a generic term.
    # Proper subclustering deferred to future work (see SUBCLUSTERING_DEFERRED.md).
    (
        "large false positive: 109 unrelated companies",
        "INDUSTRY_ONLY",
        ["large_false_positive", "unrelated_investment_firms", "org_id_suppressed"],
    ),
    (
        "large false positive: 70 unrelated companies",
        "INDUSTRY_ONLY",
        ["large_false_positive", "unrelated_construction_firms", "org_id_suppressed"],
    ),
    # Occupation categories (individuals, not organizations)
    (
        "occupation category, not organization",
        "INDUSTRY_ONLY",
        ["occupation_category", "individual_contributor"],
    ),
    # Generic terms mixed with specific companies -> needs splitting
    (
        "generic term mixed with specific companies",
        "INDUSTRY_ONLY",
        ["generic_industry_term", "may_contain_trade_associations"],
    ),
]


def _normalize_orgname(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def _parse_currency(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace("", "0")
        .astype(float)
    )


def _parse_percent(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace("", "0")
        .astype(float)
        / 100.0
    )


def load_decisions(decisions_path: Path) -> pd.DataFrame:
    df = pd.read_csv(decisions_path)
    df["orgname_raw"] = _normalize_orgname(df["orgname_raw"])
    df["cluster_decision"] = df["cluster_decision"].fillna("").astype(str).str.upper()
    df["decision_reasoning"] = df["decision_reasoning"].fillna("").astype(str)

    if "total_amount" in df.columns:
        df["total_amount_numeric"] = _parse_currency(df["total_amount"])
    if "confidence_est" in df.columns:
        df["confidence_est_numeric"] = _parse_percent(df["confidence_est"])

    return df


def _is_trade_association(orgname: str) -> bool:
    """Detect if an organization name contains trade association keywords."""
    orgname_lower = str(orgname).lower()
    return any(keyword in orgname_lower for keyword in TRADE_ASSOCIATION_KEYWORDS)


def _is_occupation(orgname: str) -> bool:
    """Check if orgname is a pure occupation term."""
    orgname_lower = str(orgname).lower().strip()
    return orgname_lower in OCCUPATION_TERMS


def _is_generic_industry(orgname: str) -> bool:
    """Check if orgname is a generic industry term."""
    orgname_lower = str(orgname).lower().strip()
    return orgname_lower in GENERIC_INDUSTRY_TERMS


def classify_resolution(df: pd.DataFrame) -> pd.DataFrame:
    """Classify each cluster into resolution types based on 4-tier strategy."""
    resolution_type: List[str] = []
    flags: List[str] = []
    is_trade_association: List[bool] = []
    is_occupation_category: List[bool] = []
    is_generic_industry: List[bool] = []
    suggested_action: List[str] = []

    for _, row in df.iterrows():
        decision = row.get("cluster_decision", "")
        reasoning = str(row.get("decision_reasoning", "")).lower()
        orgname = str(row.get("orgname_raw", ""))

        # Determine if trade association, occupation, or generic industry
        is_trade_assn = _is_trade_association(orgname)
        is_occ = _is_occupation(orgname)
        is_generic = _is_generic_industry(orgname)

        if decision == "REJECT":
            matched = False
            for needle, res_type, res_flags in RESOLUTION_RULES:
                if needle in reasoning:
                    resolution_type.append(res_type)
                    flags.append(json.dumps(res_flags))

                    # Add metadata
                    is_trade_association.append(is_trade_assn)
                    is_occupation_category.append(is_occ or "occupation_category" in res_flags)
                    is_generic_industry.append(is_generic or res_type == "INDUSTRY_ONLY")

                    # Suggested actions
                    if res_type == "NEEDS_SUBCLUSTERING":
                        suggested_action.append("Consider splitting by similarity or RealCode subtype")
                    elif res_type == "DATA_QUALITY_ISSUE":
                        suggested_action.append("Flag in analysis; investigate data extraction issues")
                    elif res_type == "INDUSTRY_ONLY" and "may_contain_trade_associations" in res_flags:
                        suggested_action.append("Review for trade associations; may need entity extraction")
                    else:
                        suggested_action.append("Use for sector-level analysis; no org_id assignment")

                    matched = True
                    break

            if not matched:
                resolution_type.append("INDUSTRY_ONLY")
                flags.append(json.dumps(["unspecified_reject"]))
                is_trade_association.append(is_trade_assn)
                is_occupation_category.append(is_occ)
                is_generic_industry.append(is_generic)
                suggested_action.append("Review classification")

        elif decision == "REVIEW":
            # REVIEW entries are real org names from the new entity resolution run
            # that haven't been manually confirmed yet.  Treat as provisional
            # ENTITY_RESOLVED so they flow through Tier 1/2/3 classification normally.
            # The pending_review flag marks them for Stage 5E manual confirmation.
            resolution_type.append("ENTITY_RESOLVED")
            flags.append(json.dumps(["pending_review"]))
            is_trade_association.append(is_trade_assn)
            is_occupation_category.append(is_occ)
            is_generic_industry.append(is_generic)
            suggested_action.append("Pending manual review; provisionally treated as entity")

        elif decision == "APPROVE":
            # All approved clusters are real entities
            resolution_type.append("ENTITY_RESOLVED")
            flags.append(json.dumps([]))
            is_trade_association.append(is_trade_assn)
            is_occupation_category.append(False)
            is_generic_industry.append(False)
            suggested_action.append("Use org_id for entity-level analysis")

        else:
            resolution_type.append("ENTITY_RESOLVED")
            flags.append(json.dumps(["missing_decision"]))
            is_trade_association.append(is_trade_assn)
            is_occupation_category.append(is_occ)
            is_generic_industry.append(is_generic)
            suggested_action.append("Review decision status")

    df = df.copy()
    df["resolution_type"] = resolution_type
    df["data_quality_flags"] = flags
    df["is_trade_association"] = is_trade_association
    df["is_occupation_category"] = is_occupation_category
    df["is_generic_industry"] = is_generic_industry
    df["suggested_action"] = suggested_action

    return df


def _discover_cycles(cycles: Iterable[str] | None) -> List[str]:
    if cycles:
        return list(cycles)
    return sorted([d.name for d in DERIVED_DIR.iterdir() if d.is_dir() and d.name.isdigit()])


def _load_indivs(cycle: str, orgnames: set[str]) -> pd.DataFrame:
    indiv_file = DERIVED_DIR / cycle / "indivs_agg.parquet"
    if not indiv_file.exists():
        return pd.DataFrame(columns=["orgname_raw", "RealCode", "total_amount", "record_count", "source"])

    df = pd.read_parquet(indiv_file, columns=["Orgname", "RealCode", "total_amount", "record_count"])
    df["orgname_raw"] = _normalize_orgname(df["Orgname"])
    df = df[df["orgname_raw"].isin(orgnames)].copy()
    if df.empty:
        return pd.DataFrame(columns=["orgname_raw", "RealCode", "total_amount", "record_count", "source"])

    df["RealCode"] = df["RealCode"].fillna("UNKNOWN").astype(str)
    df["source"] = "indiv"
    return df[["orgname_raw", "RealCode", "total_amount", "record_count", "source"]]


def _load_pacs(cycle: str, orgnames: set[str]) -> pd.DataFrame:
    pac_file = DERIVED_DIR / cycle / "pacs_agg.parquet"
    if not pac_file.exists():
        return pd.DataFrame(columns=["orgname_raw", "RealCode", "total_amount", "record_count", "source"])

    pacs = pd.read_parquet(pac_file, columns=["PACID", "RealCode", "total_amount", "record_count"])
    cmte_file = CLEAN_DIR / cycle / "committees.parquet"

    if cmte_file.exists():
        committees = pd.read_parquet(cmte_file, columns=["CmteID", "UltOrg"])
        # Normalize CmteID to lowercase to match PACID (lowercased by pre_aggregate)
        committees["CmteID"] = committees["CmteID"].str.lower()
        committees["UltOrg"] = _normalize_orgname(committees["UltOrg"])
        pacs = pacs.merge(committees, left_on="PACID", right_on="CmteID", how="left")
        pacs["orgname_raw"] = pacs["UltOrg"].fillna(pacs["PACID"]).astype(str).str.strip().str.lower()
    else:
        pacs["orgname_raw"] = pacs["PACID"].astype(str).str.strip().str.lower()

    pacs = pacs[pacs["orgname_raw"].isin(orgnames)].copy()
    if pacs.empty:
        return pd.DataFrame(columns=["orgname_raw", "RealCode", "total_amount", "record_count", "source"])

    pacs["RealCode"] = pacs["RealCode"].fillna("UNKNOWN").astype(str)
    pacs["source"] = "pac"
    return pacs[["orgname_raw", "RealCode", "total_amount", "record_count", "source"]]


def build_realcode_distribution(
    decisions: pd.DataFrame,
    cycles: Iterable[str] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    orgnames = set(decisions["orgname_raw"].dropna().astype(str))
    cycles_list = _discover_cycles(cycles)

    frames: List[pd.DataFrame] = []
    for cycle in cycles_list:
        frames.append(_load_indivs(cycle, orgnames))
        frames.append(_load_pacs(cycle, orgnames))

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        return pd.DataFrame(), pd.DataFrame()

    combined = combined.merge(
        decisions[["orgname_raw", "org_id"]],
        on="orgname_raw",
        how="left",
    )
    combined = combined.dropna(subset=["org_id"]).copy()

    grouped = (
        combined.groupby(["org_id", "RealCode"], as_index=False)
        .agg({"total_amount": "sum", "record_count": "sum"})
        .rename(columns={"RealCode": "realcode"})
    )
    grouped["org_total_amount"] = grouped.groupby("org_id")["total_amount"].transform("sum")
    grouped["org_total_records"] = grouped.groupby("org_id")["record_count"].transform("sum")
    grouped["share_amount"] = grouped["total_amount"] / grouped["org_total_amount"]
    grouped["share_records"] = grouped["record_count"] / grouped["org_total_records"]

    distribution_rows = []
    for org_id, chunk in grouped.groupby("org_id"):
        ordered = chunk.sort_values("share_amount", ascending=False)
        top = ordered.head(10)
        other_share = max(0.0, 1.0 - top["share_amount"].sum())
        entries = [
            {
                "realcode": row["realcode"],
                "amount": float(row["total_amount"]),
                "share": float(row["share_amount"]),
            }
            for _, row in top.iterrows()
        ]
        if other_share > 0:
            entries.append({"realcode": "OTHER", "amount": None, "share": other_share})

        distribution_rows.append(
            {
                "org_id": org_id,
                "realcode_top": top.iloc[0]["realcode"] if not top.empty else None,
                "realcode_top_share": float(top.iloc[0]["share_amount"]) if not top.empty else None,
                "realcode_distribution": json.dumps(entries),
            }
        )

    distribution = pd.DataFrame(distribution_rows)
    return grouped, distribution


def enhance_decisions(
    decisions_path: Path,
    output_prefix: str,
    cycles: Iterable[str] | None = None,
) -> Tuple[Path, Path, Path, Path]:
    decisions = load_decisions(decisions_path)
    decisions = classify_resolution(decisions)

    # Suppress org_id for clusters whose flags include "org_id_suppressed".
    # This prevents false-entity org_ids (large false positives) from leaking
    # into downstream joins. These rows are kept as INDUSTRY_ONLY with RealCode.
    suppressed_mask = decisions["data_quality_flags"].str.contains("org_id_suppressed", na=False)
    decisions.loc[suppressed_mask, "org_id"] = None

    realcode_long, realcode_summary = build_realcode_distribution(decisions, cycles=cycles)

    enhanced = decisions.merge(realcode_summary, on="org_id", how="left")

    enhanced_csv = LOOKUPS_DIR / f"{output_prefix}.csv"
    enhanced_parquet = LOOKUPS_DIR / f"{output_prefix}.parquet"
    realcode_csv = LOOKUPS_DIR / f"{output_prefix}_realcode_distribution.csv"
    realcode_parquet = LOOKUPS_DIR / f"{output_prefix}_realcode_distribution.parquet"

    enhanced.to_csv(enhanced_csv, index=False)
    enhanced.to_parquet(enhanced_parquet, index=False)

    if not realcode_long.empty:
        realcode_long.to_csv(realcode_csv, index=False)
        realcode_long.to_parquet(realcode_parquet, index=False)
    else:
        pd.DataFrame().to_csv(realcode_csv, index=False)
        pd.DataFrame().to_parquet(realcode_parquet, index=False)

    return enhanced_csv, enhanced_parquet, realcode_csv, realcode_parquet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enhance cluster decisions with resolution types and RealCode distributions."
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        default=LOOKUPS_DIR / "ready_to_review_top_13k_with_decisions.csv",
        help="Path to decisions CSV",
    )
    parser.add_argument(
        "--output-prefix",
        default="org_aliases_top_13k_enhanced",
        help="Output file prefix under data/lookups",
    )
    parser.add_argument(
        "--cycles",
        nargs="*",
        default=None,
        help="Optional list of cycles to include (e.g., 00 02 04)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    enhance_decisions(args.decisions, args.output_prefix, cycles=args.cycles)


if __name__ == "__main__":
    main()
