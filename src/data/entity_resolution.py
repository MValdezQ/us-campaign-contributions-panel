"""Entity Resolution (Stage 4) for campaign finance data.

Clusters organization name variations into stable org_ids using Deezymatch.
Supports iterative scope expansion (top_200 -> top_500 -> all).

Usage:
    python -m src.data.entity_resolution --scope top_200
    python -m src.data.entity_resolution --scope all --dry_run
"""

import argparse
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Tuple

import pandas as pd
import numpy as np
import duckdb

from .name_cleaning import clean_org_name, clean_batch

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
DERIVED_DIR = PROJECT_ROOT / "data" / "derived"
LOOKUPS_DIR = PROJECT_ROOT / "data" / "lookups"
LOOKUPS_DIR.mkdir(parents=True, exist_ok=True)

# Scope configuration
SCOPE_CONFIG = {
    "top_200": {
        "n_orgs": 200,
        "description": "top 200 organizations by total contribution $",
        "coverage_pct_est": 50,
    },
    "top_500": {
        "n_orgs": 500,
        "description": "top 500 organizations by total contribution $",
        "coverage_pct_est": 50,
    },
    "top_13k": {
        "n_orgs": 13876,
        "description": "top ~13,876 organizations (80% of total contribution $)",
        "coverage_pct_est": 80,
    },
    "all": {
        "n_orgs": None,
        "description": "all organizations (100% coverage)",
        "coverage_pct_est": 100,
    },
}


def load_aggregated_data(cycles: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load aggregated individuals and PACs data across all cycles.

    Parameters
    ----------
    cycles : List[str], optional
        Specific cycles to load. If None, loads all available cycles.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (indivs_agg, pacs_agg) concatenated across cycles
    """
    if cycles is None:
        cycles = sorted([d.name for d in DERIVED_DIR.iterdir() if d.is_dir() and d.name.isdigit()])

    indivs_list = []
    pacs_list = []

    for cycle in cycles:
        cycle_dir = DERIVED_DIR / cycle
        indiv_file = cycle_dir / "indivs_agg.parquet"
        pac_file = cycle_dir / "pacs_agg.parquet"

        if indiv_file.exists():
            # Only load the columns aggregate_org_totals actually uses
            df = pd.read_parquet(indiv_file, columns=["Orgname", "total_amount", "record_count"])
            indivs_list.append(df)

        if pac_file.exists():
            # Only load the columns needed for the committee join and aggregation
            df = pd.read_parquet(pac_file, columns=["Cycle", "PACID", "total_amount", "record_count"])
            pacs_list.append(df)

    indivs_agg = pd.concat(indivs_list, ignore_index=True) if indivs_list else pd.DataFrame()
    pacs_agg = pd.concat(pacs_list, ignore_index=True) if pacs_list else pd.DataFrame()

    return indivs_agg, pacs_agg


def aggregate_org_totals(indivs_agg: pd.DataFrame, pacs_agg: pd.DataFrame, cycles: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Aggregate contributions by unique organization name across all cycles.

    For PACs, joins to committees to get UltOrg. For individuals, uses Orgname.

    Parameters
    ----------
    indivs_agg : pd.DataFrame
        Aggregated individual contributions
    pacs_agg : pd.DataFrame
        Aggregated PAC contributions
    cycles : List[str], optional
        Cycles to process (used for loading committees)

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: orgname_raw, total_amount, record_count, source_types
        Sorted by total_amount descending
    """
    org_totals = []

    # Individuals: group by Orgname
    if not indivs_agg.empty:
        indiv_grouped = indivs_agg.groupby("Orgname").agg({
            "total_amount": "sum",
            "record_count": "sum"
        }).reset_index()
        indiv_grouped["source"] = "indiv"
        org_totals.append(indiv_grouped.rename(columns={"Orgname": "orgname_raw"}))

    # PACs: must join to committees to get UltOrg
    if not pacs_agg.empty:
        if cycles is None:
            cycles = sorted([d.name for d in DERIVED_DIR.iterdir() if d.is_dir() and d.name.isdigit()])

        # Load all committees and join to PACs
        committees_list = []
        for cycle in cycles:
            cmte_file = CLEAN_DIR / cycle / "committees.parquet"
            if cmte_file.exists():
                df = pd.read_parquet(cmte_file)
                df["cycle"] = cycle
                committees_list.append(df)

        if committees_list:
            committees = pd.concat(committees_list, ignore_index=True)

            # Committee IDs are globally unique stable identifiers; UltOrg is a
            # property of the committee, not of a specific cycle.  Joining on
            # cycle too causes a mismatch because pacs_agg stores 4-digit years
            # (e.g. "2018") while the folder-name cycle is 2-digit (e.g. "18").
            # Build a deduplicated CmteID → UltOrg lookup across all cycles;
            # keep the last non-null UltOrg per committee (most recent cycle).
            cmte_lookup = committees[["CmteID", "UltOrg"]].copy()
            cmte_lookup["CmteID"] = cmte_lookup["CmteID"].str.lower()
            cmte_lookup = (
                cmte_lookup[cmte_lookup["UltOrg"].notna()]
                .drop_duplicates(subset=["CmteID"], keep="last")
            )

            # Join PACs to committees on PACID == CmteID only
            pacs_with_org = pacs_agg.merge(
                cmte_lookup,
                left_on="PACID",
                right_on="CmteID",
                how="left"
            )

            # Fill UltOrg with PACID if not found (fallback)
            pacs_with_org["UltOrg"] = pacs_with_org["UltOrg"].fillna(pacs_with_org["PACID"])

            # Group by UltOrg
            pac_grouped = pacs_with_org.groupby("UltOrg").agg({
                "total_amount": "sum",
                "record_count": "sum"
            }).reset_index()
            pac_grouped["source"] = "pac"
            org_totals.append(pac_grouped.rename(columns={"UltOrg": "orgname_raw"}))

    if org_totals:
        combined = pd.concat(org_totals, ignore_index=True)
        # Aggregate across sources if org appears in both
        combined = combined.groupby("orgname_raw").agg({
            "total_amount": "sum",
            "record_count": "sum",
            "source": lambda x: ",".join(sorted(set(x)))
        }).reset_index()
    else:
        combined = pd.DataFrame(columns=["orgname_raw", "total_amount", "record_count", "source"])

    # Sort by total amount descending
    combined = combined.sort_values("total_amount", ascending=False).reset_index(drop=True)
    combined["rank"] = range(1, len(combined) + 1)

    return combined


def filter_by_scope(org_totals: pd.DataFrame, scope: str) -> pd.DataFrame:
    """
    Filter organization list by scope (top_200, top_500, or all).

    Parameters
    ----------
    org_totals : pd.DataFrame
        Full organization totals (sorted by amount)
    scope : str
        Scope name: "top_200", "top_500", "all"

    Returns
    -------
    pd.DataFrame
        Filtered org_totals
    """
    config = SCOPE_CONFIG.get(scope)
    if not config:
        raise ValueError(f"Unknown scope: {scope}. Must be one of {list(SCOPE_CONFIG.keys())}")

    n_orgs = config["n_orgs"]
    if n_orgs is None:
        return org_totals

    return org_totals.head(n_orgs).copy()


def cluster_with_rapidfuzz(
    org_names: List[str],
    threshold: float = 95,
    verbose: bool = False
) -> Dict[str, str]:
    """
    Cluster organization names using RapidFuzz similarity matching.

    Parameters
    ----------
    org_names : List[str]
        Cleaned organization names to cluster
    threshold : float
        Minimum similarity score for grouping (0-100, default 95)
    verbose : bool
        Print progress

    Returns
    -------
    Dict[str, str]
        Mapping from cleaned_name -> org_id (UUID)
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        raise ImportError("rapidfuzz not installed. Install with: pip install rapidfuzz")

    if verbose:
        print(f"Clustering {len(org_names)} organization names with RapidFuzz (threshold={threshold})")

    # Remove duplicates while preserving order for faster processing
    unique_names = list(dict.fromkeys(org_names))
    if verbose:
        print(f"  - Unique names to cluster: {len(unique_names)}")

    # Greedy clustering: assign each name to first matching cluster
    name_to_org_id = {}
    cluster_representatives = {}  # org_id -> representative name
    cluster_count = 0

    for i, name in enumerate(unique_names):
        if name in name_to_org_id:
            continue

        # Check if this name matches any existing cluster
        matched = False
        for org_id, rep_name in cluster_representatives.items():
            # Use token_set_ratio for better handling of partial matches
            similarity = fuzz.token_set_ratio(name, rep_name)
            if similarity >= threshold:
                name_to_org_id[name] = org_id
                matched = True
                break

        # Create new cluster if no match found
        if not matched:
            org_id = str(uuid.uuid4())[:8]
            name_to_org_id[name] = org_id
            cluster_representatives[org_id] = name
            cluster_count += 1

        if verbose and (i + 1) % 1000 == 0:
            print(f"  - Processed {i + 1}/{len(unique_names)} names, {len(set(name_to_org_id.values()))} clusters so far...")

    if verbose:
        print(f"  - Clusters created: {len(set(name_to_org_id.values()))}")

    # Map all original names (including duplicates) to their org_id
    final_mapping = {}
    for name in org_names:
        final_mapping[name] = name_to_org_id[name]

    return final_mapping


def build_org_aliases(
    org_totals_filtered: pd.DataFrame,
    name_to_org_id: Dict[str, str],
    scope: str,
) -> pd.DataFrame:
    """
    Build org_aliases table with version tracking.

    Parameters
    ----------
    org_totals_filtered : pd.DataFrame
        Filtered organization totals
    name_to_org_id : Dict[str, str]
        Cleaned name -> org_id mapping from Deezymatch
    scope : str
        Scope used (for tracking)

    Returns
    -------
    pd.DataFrame
        org_aliases with columns:
        - orgname_raw: original name
        - orgname_cleaned: cleaned name
        - org_id: assigned cluster ID
        - total_amount: aggregate dollars
        - record_count: total contributions
        - version_added: when org was first clustered
        - scope: which scope it was added in
        - approved: pending manual review
    """
    org_totals_filtered["orgname_cleaned"] = org_totals_filtered["orgname_raw"].apply(clean_org_name)
    org_totals_filtered["org_id"] = org_totals_filtered["orgname_cleaned"].map(name_to_org_id)
    org_totals_filtered["version_added"] = datetime.utcnow().isoformat()
    org_totals_filtered["scope"] = scope
    org_totals_filtered["approved"] = False  # Pending manual review
    org_totals_filtered["reviewer_notes"] = ""

    return org_totals_filtered[[
        "rank",
        "orgname_raw",
        "orgname_cleaned",
        "org_id",
        "total_amount",
        "record_count",
        "source",
        "version_added",
        "scope",
        "approved",
        "reviewer_notes"
    ]]


def generate_review_file(org_aliases: pd.DataFrame, scope: str) -> Path:
    """
    Generate human-friendly CSV for manual review.

    Parameters
    ----------
    org_aliases : pd.DataFrame
        Organization aliases with clustering results
    scope : str
        Scope name

    Returns
    -------
    Path
        Path to generated CSV
    """
    # Calculate confidence by grouping org_id and checking uniqueness
    org_id_counts = org_aliases.groupby("org_id").size()
    org_aliases["cluster_size"] = org_aliases["org_id"].map(org_id_counts)

    # Confidence: high if small cluster (good match), low if large (diverse)
    org_aliases["confidence_est"] = (1 - (org_aliases["cluster_size"] - 1) / org_aliases["cluster_size"].max()).round(3)

    # Sort by confidence (descending) then by amount (descending)
    review_df = org_aliases.sort_values(["confidence_est", "total_amount"], ascending=[False, False])

    # Select columns for review
    review_cols = [
        "rank",
        "orgname_raw",
        "total_amount",
        "record_count",
        "org_id",
        "cluster_size",
        "confidence_est",
        "source",
        "reviewer_notes"
    ]
    review_df = review_df[review_cols].copy()

    # Format for readability
    review_df["total_amount"] = review_df["total_amount"].apply(lambda x: f"${x:,.0f}")
    review_df["confidence_est"] = review_df["confidence_est"].apply(lambda x: f"{x:.1%}")

    # Output
    out_path = LOOKUPS_DIR / f"ready_to_review_{scope}.csv"
    review_df.to_csv(out_path, index=False)

    print(f"\nReview file generated: {out_path}")
    print(f"Instructions:")
    print(f"  1. Open the CSV in Excel or your editor")
    print(f"  2. Review orgname_raw and cluster_size (how many names in this cluster)")
    print(f"  3. If the cluster is WRONG (names don't belong together):")
    print(f"     - Add comment in reviewer_notes column")
    print(f"     - Mark confidence_est as 'REJECT'")
    print(f"  4. If the cluster is CORRECT:")
    print(f"     - Leave reviewer_notes empty or add notes")
    print(f"     - Confidence_est already set based on cluster size")
    print(f"  5. Save and send back")

    return out_path


def run_entity_resolution(
    scope: str = "top_200",
    cycles: Optional[List[str]] = None,
    threshold: float = 0.95,
    dry_run: bool = False,
    verbose: bool = False,
) -> Path:
    """
    Main entity resolution orchestrator.

    Parameters
    ----------
    scope : str
        "top_200", "top_500", or "all"
    cycles : List[str], optional
        Specific cycles to process. If None, auto-discovers.
    threshold : float
        RapidFuzz similarity threshold (0-100, default 95)
    dry_run : bool
        If True, don't write outputs (for testing)
    verbose : bool
        Verbose output

    Returns
    -------
    Path
        Path to generated org_aliases file
    """
    print("\n" + "=" * 80)
    print(f"STAGE 4: ENTITY RESOLUTION (Scope: {scope})")
    print("=" * 80)

    # 1. Load data
    print(f"\n[1/5] Loading aggregated data across {len(cycles or [])} cycles...")
    indivs_agg, pacs_agg = load_aggregated_data(cycles)
    print(f"  - Individuals: {len(indivs_agg):,} records")
    print(f"  - PACs: {len(pacs_agg):,} records")

    # 2. Aggregate by org
    print(f"\n[2/5] Aggregating contributions by organization...")
    org_totals = aggregate_org_totals(indivs_agg, pacs_agg, cycles=cycles)
    print(f"  - Unique organizations: {len(org_totals):,}")
    print(f"  - Total $ across all: ${org_totals['total_amount'].sum():,.0f}")

    # 3. Filter by scope
    print(f"\n[3/5] Filtering by scope: {scope}")
    config = SCOPE_CONFIG[scope]
    org_totals_filtered = filter_by_scope(org_totals, scope)
    pct_coverage = (org_totals_filtered["total_amount"].sum() / org_totals["total_amount"].sum() * 100)
    print(f"  - Organizations to cluster: {len(org_totals_filtered):,}")
    print(f"  - Estimated $ coverage: {pct_coverage:.1f}%")

    # 4. RapidFuzz clustering
    # Convert threshold from 0-1 scale to 0-100 scale for RapidFuzz
    rapidfuzz_threshold = threshold * 100 if threshold <= 1.0 else threshold
    print(f"\n[4/5] Clustering with RapidFuzz (threshold={rapidfuzz_threshold})...")
    cleaned_names = org_totals_filtered["orgname_raw"].apply(clean_org_name).tolist()
    name_to_org_id = cluster_with_rapidfuzz(cleaned_names, threshold=rapidfuzz_threshold, verbose=verbose)
    print(f"  - Clusters created: {len(set(name_to_org_id.values())):,}")

    # 5. Build org_aliases and outputs
    print(f"\n[5/5] Building org_aliases and generating review file...")
    org_aliases = build_org_aliases(org_totals_filtered, name_to_org_id, scope)

    if not dry_run:
        # Save org_aliases
        out_path = LOOKUPS_DIR / "org_aliases.parquet"
        org_aliases.to_parquet(out_path, compression="snappy", index=False)
        print(f"  - Saved: {out_path}")

        # Generate review file
        review_path = generate_review_file(org_aliases, scope)

        print("\n" + "=" * 80)
        print(f"READY FOR MANUAL REVIEW")
        print("=" * 80)
        print(f"Next steps:")
        print(f"  1. Open: {review_path}")
        print(f"  2. Review flagged clusters (sorted by confidence)")
        print(f"  3. Once approved, re-run with --scope {scope} to finalize")

        return out_path
    else:
        print("  [DRY RUN] No files written")
        return Path()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Entity Resolution (Stage 4) for campaign finance data"
    )
    parser.add_argument(
        "--scope",
        choices=list(SCOPE_CONFIG.keys()),
        default="top_200",
        help="Scope of organizations to cluster"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Deezymatch similarity threshold (0-1)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test run without writing files"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )

    args = parser.parse_args()

    try:
        run_entity_resolution(
            scope=args.scope,
            threshold=args.threshold,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
