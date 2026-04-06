"""
Stage 5 — Steps 2-4: Org-level NAICS assignment with 3-model consensus.

Usage:
  python src/data/assign_org_naics.py          # Tier 1 + 3 auto-assign
  python src/data/assign_org_naics.py --llm    # Run LLM batch for Tier 2
  python src/data/assign_org_naics.py --review # Interactive review of Tier 2

Inputs:
  - data/lookups/org_aliases_top_13k_enhanced.csv (alias-level org data)
  - data/lookups/org_aliases_top_13k_enhanced_realcode_distribution.parquet
  - data/lookups/catcode_naics_candidates.csv (crosswalk)
  - data/raw/political/CRP_Categories.txt (catcode metadata)
  - openrouter_api_key.txt (for --llm flag)

Outputs:
  - data/lookups/org_naics_assignment.csv (main output, all 12,530 ENTITY_RESOLVED orgs)
"""

import pandas as pd
import json
import sys
import time
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from openai import OpenAI

# ============================================================================
# CONFIG
# ============================================================================

ORG_CSV_PATH      = Path("data/lookups/org_aliases_top_13k_enhanced.csv")
RC_DIST_PATH      = Path("data/lookups/org_aliases_top_13k_enhanced_realcode_distribution.parquet")
CANDIDATES_PATH   = Path("data/lookups/catcode_naics_candidates.csv")
CRP_PATH          = Path("data/raw/political/CRP_Categories.txt")
API_KEY_PATH      = Path("openrouter_api_key.txt")
OUT_PATH          = Path("data/lookups/org_naics_assignment.csv")

# LLM models for 3-model consensus
LLM_MODELS = [
    "anthropic/claude-haiku-4.5",
    "deepseek/deepseek-v3.2",
    "google/gemini-3-flash-preview",
]

# Hardcoded overrides: catcode -> (naics3 or None, {flags})
HARDCODED_OVERRIDES = {
    "K1000": ("541", {}),
    "K1100": ("541", {}),
    "K1200": ("541", {}),
    "K2000": (None, {"is_lobbyist": True}),
    "K2100": (None, {"is_lobbyist": True}),
    "X3000": (None, {"is_public_employee": True}),
    "X3100": (None, {"is_public_employee": True}),
    "X3500": ("611", {"is_public_employee": True}),
    "X3700": ("491", {"is_public_employee": True}),
    "X4100": (None, {"is_nonprofit": True}),
    "X4110": (None, {"is_nonprofit": True}),
    "X7000": ("813", {"is_ideology": True}),
    "X8000": ("928", {}),
    "G2800": ("312", {}),
}

# NAICS-3 names for hardcoded codes
NAICS3_NAMES = {
    "312": "Beverage Manufacturing",
    "491": "Postal Service",
    "541": "Professional Services",
    "611": "Education",
    "813": "Religious/Civic Organizations",
    "928": "International Organizations",
}

# ============================================================================
# DATA LOADING
# ============================================================================

def load_data():
    """Load all input data."""
    print("Loading input data...")
    orgs = pd.read_csv(ORG_CSV_PATH)
    dist = pd.read_parquet(RC_DIST_PATH)
    cands = pd.read_csv(CANDIDATES_PATH)
    crp = pd.read_csv(CRP_PATH, sep="\t", encoding="latin-1", dtype=str)
    crp.columns = crp.columns.str.strip()
    crp["Catcode"] = crp["Catcode"].str.strip().str.upper()

    print(f"  Orgs CSV: {len(orgs)} rows, {orgs['org_id'].nunique()} unique org_ids")
    print(f"  RC distribution: {len(dist)} rows, {dist['org_id'].nunique()} orgs")
    print(f"  Candidates: {len(cands)} pairs")
    print(f"  CRP categories: {len(crp)} catcodes")

    return orgs, dist, cands, crp


def build_org_universe(orgs: pd.DataFrame, crp: pd.DataFrame, cands: pd.DataFrame) -> pd.DataFrame:
    """
    Build the org universe:
    - Dedup by org_id (keep max total_amount row = most canonical alias)
    - Include ENTITY_RESOLVED and INDUSTRY_ONLY
    - Exclude DATA_QUALITY_ISSUE
    - Classify into tiers
    """
    # Filter to resolutions we want
    valid = orgs[orgs["resolution_type"].isin(["ENTITY_RESOLVED", "INDUSTRY_ONLY"])].copy()

    # Dedup by org_id (keep max total_amount row)
    universe = valid.sort_values("total_amount_numeric", ascending=False).drop_duplicates(
        "org_id", keep="first"
    ).copy()

    # Normalize realcode
    universe["rc"] = universe["realcode_top"].fillna("").str.upper().str.strip()
    universe["rc1"] = universe["rc"].str[:1]

    # Build catcode lookup sets
    unambig_set = set(cands[cands["is_only_candidate"]]["catcode"])
    ambig_set   = set(cands[~cands["is_only_candidate"]]["catcode"])
    crp_set     = set(crp["Catcode"])

    # Classify into tiers
    def classify_tier(row):
        rc, rc1 = row["rc"], row["rc1"]

        # Hardcoded override takes precedence
        if rc in HARDCODED_OVERRIDES:
            return "TIER1_HARDCODED"

        # Labor unions go to Tier 2 (LLM)
        if rc1 == "L":
            return "TIER2_LABOR"

        # Ambiguous catcodes go to Tier 2
        if rc in ambig_set:
            return "TIER2_AMBIG"

        # Deterministic (1 NAICS-3) go to Tier 1
        if rc in unambig_set:
            return "TIER1"

        # Special codes (J/Z/X/Y) go to Tier 3
        if rc1 in ("J", "Z", "X", "Y"):
            return "TIER3_SPECIAL"

        # Empty realcode goes to Tier 3
        if rc == "":
            return "TIER3_EMPTY"

        # Unknown catcodes
        return "TIER3_UNKNOWN"

    universe["tier"] = universe.apply(classify_tier, axis=1)

    print(f"\nTier breakdown (deduped org_ids):")
    print(universe["tier"].value_counts().sort_index())

    return universe


# ============================================================================
# REPRESENTATIVE ORGNAME (TOP-3 ALIASES)
# ============================================================================

def build_orgname_lookup(orgs: pd.DataFrame) -> Dict[str, str]:
    """
    For each org_id, collect top-3 aliases by total_amount.
    Return dict: org_id -> "alias1; alias2; alias3"
    """
    orgnames = {}
    for oid, grp in orgs.dropna(subset=["org_id"]).groupby("org_id"):
        top3 = grp.nlargest(3, "total_amount_numeric")["orgname_raw"].tolist()
        orgnames[oid] = "; ".join(top3)
    return orgnames


# ============================================================================
# TIER 1 & 3 AUTO-ASSIGNMENT
# ============================================================================

def assign_tier1_and_tier3(
    universe: pd.DataFrame,
    cands: pd.DataFrame,
    crp: pd.DataFrame,
    orgnames: Dict[str, str],
) -> pd.DataFrame:
    """
    Auto-assign NAICS for Tier 1 deterministic and Tier 3 special codes.
    Return DataFrame with assignment columns filled.
    """
    # Filter to valid org_ids only
    universe = universe.dropna(subset=["org_id"]).copy()
    results = []

    # Build candidate lookup: catcode -> list of (naics3, naics3_name)
    cand_dict = {}
    for _, row in cands.iterrows():
        cat = row["catcode"]
        naics3 = row["naics3"]
        name = row["naics3_name"]
        if cat not in cand_dict:
            cand_dict[cat] = []
        cand_dict[cat].append((naics3, name))

    # Build catname lookup
    catname_dict = cands.drop_duplicates("catcode")[["catcode", "catname"]].set_index("catcode")["catname"].to_dict()

    for _, row in universe.iterrows():
        org_id = row["org_id"]
        rc = row["rc"]
        tier = row["tier"]
        orgname = orgnames[org_id]

        # Initialize result dict
        result = {
            "org_id": org_id,
            "orgname": orgname,
            "resolution_type": row["resolution_type"],
            "top_realcode": row["realcode_top"],
            "top_realcode_share": row["realcode_top_share"],
            "top_realcode_name": catname_dict.get(rc, ""),
            "assigned_naics3": None,
            "assigned_naics3_name": "",
            "assignment_method": None,
            "assignment_confidence": None,
            "n_candidates": 0,
            "candidates_list": "",
            "is_ideology": False,
            "is_party": False,
            "is_labor_union": False,
            "is_public_employee": False,
            "is_nonprofit": False,
            "is_lobbyist": False,
            "no_realcode": False,
        }

        # Tier 1: Hardcoded override
        if tier == "TIER1_HARDCODED":
            naics3, flags = HARDCODED_OVERRIDES[rc]
            result["assigned_naics3"] = naics3
            result["assigned_naics3_name"] = NAICS3_NAMES.get(naics3, "") if naics3 else ""
            result["assignment_method"] = "hardcoded"
            result["assignment_confidence"] = "HIGH"
            result.update(flags)

        # Tier 1: Deterministic (one candidate)
        elif tier == "TIER1":
            candidates = cand_dict.get(rc, [])
            if candidates:
                naics3, name = candidates[0]
                result["assigned_naics3"] = naics3
                result["assigned_naics3_name"] = name
                result["n_candidates"] = 1
                result["assignment_method"] = "deterministic"
                result["assignment_confidence"] = "HIGH"

        # Tier 2: Labor unions (skip for now, LLM will handle)
        elif tier == "TIER2_LABOR":
            result["is_labor_union"] = True
            result["assignment_method"] = "llm_pending"
            candidates = cand_dict.get(rc, [])
            if candidates:
                result["n_candidates"] = len(candidates)
                result["candidates_list"] = "|".join([str(c[0]) for c in candidates])

        # Tier 2: Ambiguous (skip for now, LLM will handle)
        elif tier == "TIER2_AMBIG":
            result["assignment_method"] = "llm_pending"
            candidates = cand_dict.get(rc, [])
            if candidates:
                result["n_candidates"] = len(candidates)
                result["candidates_list"] = "|".join([str(c[0]) for c in candidates])

        # Tier 3: Special codes (J/Z/X/Y prefix rules)
        elif tier == "TIER3_SPECIAL":
            result["assignment_method"] = "special_code"
            result["assigned_naics3"] = None

            if rc.startswith("J"):
                result["is_ideology"] = True
            elif rc.startswith("Z"):
                result["is_party"] = True
            elif rc.startswith("Y"):
                pass  # No flag, just NULL NAICS
            elif rc.startswith("X"):
                pass  # Check if specific override exists above

        # Tier 3: Empty realcode
        elif tier == "TIER3_EMPTY":
            result["assignment_method"] = "no_realcode"
            result["assigned_naics3"] = None
            result["no_realcode"] = True

        # Tier 3: Unknown (shouldn't happen much)
        elif tier == "TIER3_UNKNOWN":
            result["assignment_method"] = "unknown"
            result["assigned_naics3"] = None

        # LLM-pending: add placeholder columns (initialize as str, not float)
        if result["assignment_method"] in ("llm_pending",):
            for i in range(1, 4):
                result[f"llm_model_{i}"] = ""
                result[f"llm_naics3_{i}"] = ""
                result[f"llm_confidence_{i}"] = ""
                result[f"llm_reasoning_{i}"] = ""
            result["llm_consensus"] = ""
            result["llm_rounds"] = 0

        results.append(result)

    return pd.DataFrame(results)


# ============================================================================
# LLM BATCH PROCESSING
# ============================================================================

def run_llm_batch(assignments: pd.DataFrame, dist: pd.DataFrame, cands: pd.DataFrame) -> pd.DataFrame:
    """
    Run 3-model consensus for all Tier 2 orgs (TIER2_AMBIG + TIER2_LABOR).
    Resume-safe: save results incrementally.
    """
    print("\nInitializing OpenRouter client...")
    api_key = API_KEY_PATH.read_text().strip()
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    # Filter to Tier 2 orgs
    tier2 = assignments[assignments["assignment_method"].isin(["llm_pending"])].copy()
    total_tier2 = len(tier2)
    print(f"Processing {total_tier2} Tier 2 orgs with 3-model consensus...")

    # Convert string columns to object dtype to avoid FutureWarning
    for i in range(1, 4):
        assignments[f"llm_model_{i}"] = assignments[f"llm_model_{i}"].astype("object")
        assignments[f"llm_naics3_{i}"] = assignments[f"llm_naics3_{i}"].astype("object")
        assignments[f"llm_confidence_{i}"] = assignments[f"llm_confidence_{i}"].astype("object")
        assignments[f"llm_reasoning_{i}"] = assignments[f"llm_reasoning_{i}"].astype("object")
    assignments["llm_consensus"] = assignments["llm_consensus"].astype("object")
    assignments["assigned_naics3"] = assignments["assigned_naics3"].astype("object")
    assignments["assigned_naics3_name"] = assignments["assigned_naics3_name"].astype("object")

    # Build candidate lookup for prompt generation
    cand_dict = {}
    naics3_names = {}
    for _, row in cands.iterrows():
        cat = row["catcode"]
        naics3 = row["naics3"]
        name = row["naics3_name"]
        if cat not in cand_dict:
            cand_dict[cat] = []
        cand_dict[cat].append((naics3, name))
        naics3_names[naics3] = name

    # Process in batches (Resume-safe: check if already processed)
    processed = 0
    for idx, (_, org_row) in enumerate(tier2.iterrows()):
        org_id = org_row["org_id"]

        # Skip if already processed (resume-safety)
        if pd.notna(org_row["llm_consensus"]) and org_row["llm_consensus"] != "":
            processed += 1
            continue

        top_rc = org_row["top_realcode"]
        candidates = cand_dict.get(top_rc, [])
        cand_str = "\n  ".join([f"{c[0]}: {c[1]}" for c in candidates])

        # Build realcode distribution for this org (from parquet)
        org_dist = dist[dist["org_id"] == org_id].sort_values("share_amount", ascending=False).head(5)
        dist_str = ""
        for _, d_row in org_dist.iterrows():
            dist_str += f"\n    {d_row['realcode']}: ${d_row['total_amount']:,.0f} ({d_row['share_amount']*100:.1f}%)"

        orgname = org_row["orgname"]
        top_rc_name = org_row["top_realcode_name"]

        # Call all 3 models in Round 1
        round1_picks = []
        for model_idx, model_id in enumerate(LLM_MODELS, 1):
            prompt = f"""You are an expert in US industry classification.

Organization: {orgname}
CRP Catcode: {top_rc} ({top_rc_name})
Total contributions: ${org_row['top_realcode_share']*100:.1f}%

Top realcode distribution:{dist_str}

The dominant catcode ({top_rc}) maps to these NAICS-3 candidates:
  {cand_str}

Pick the SINGLE best NAICS-3 code for this organization.
Consider the organization NAME - it may indicate a specific sub-industry.

Respond ONLY in JSON: {{"naics3": "XXX", "confidence": "HIGH|MED|LOW", "reasoning": "one sentence"}}"""

            try:
                resp = client.chat.completions.create(
                    model=model_id,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                )
                resp_text = resp.choices[0].message.content.strip()
                # Strip markdown code fences (Haiku wraps JSON in ```json...```)
                if resp_text.startswith("```"):
                    resp_text = resp_text.split("```")[1]
                    if resp_text.startswith("json"):
                        resp_text = resp_text[4:]  # Remove "json" language identifier
                resp_text = resp_text.strip()
                pick = json.loads(resp_text)
                round1_picks.append((model_id, pick))
                print(f"  [{idx+1}/{total_tier2}] {org_id} - Model {model_idx}/3: {pick.get('naics3', '?')}")
            except Exception as e:
                print(f"  ERROR on {org_id} Model {model_idx}: {e}")
                round1_picks.append((model_id, {"naics3": None, "confidence": "ERROR", "reasoning": str(e)}))

            time.sleep(0.3)  # Rate limit

        # Determine consensus (Round 1)
        picks_r1 = [p[1]["naics3"] for p in round1_picks]
        if picks_r1[0] == picks_r1[1] == picks_r1[2]:
            consensus = "UNANIMOUS"
            final_naics3 = picks_r1[0]
            rounds = 1
        elif picks_r1[0] == picks_r1[1] or picks_r1[0] == picks_r1[2] or picks_r1[1] == picks_r1[2]:
            consensus = "MAJORITY"
            # Pick the majority
            from collections import Counter
            counts = Counter(picks_r1)
            final_naics3 = counts.most_common(1)[0][0]
            rounds = 1
        else:
            consensus = "DIVERGENT"
            final_naics3 = None
            rounds = 1

        # Update the row
        for model_idx, (model_id, pick) in enumerate(round1_picks, 1):
            assignments.loc[assignments["org_id"] == org_id, f"llm_model_{model_idx}"] = model_id
            assignments.loc[assignments["org_id"] == org_id, f"llm_naics3_{model_idx}"] = pick.get("naics3")
            assignments.loc[assignments["org_id"] == org_id, f"llm_confidence_{model_idx}"] = pick.get("confidence")
            assignments.loc[assignments["org_id"] == org_id, f"llm_reasoning_{model_idx}"] = pick.get("reasoning", "")

        assignments.loc[assignments["org_id"] == org_id, "llm_consensus"] = consensus
        assignments.loc[assignments["org_id"] == org_id, "llm_rounds"] = rounds

        # If divergent, DO Round 2 (with context from Round 1)
        if consensus == "DIVERGENT":
            context_str = "\n  ".join([
                f"Model {i+1} ({round1_picks[i][0]}): {round1_picks[i][1].get('naics3')} - {round1_picks[i][1].get('reasoning', '')}"
                for i in range(3)
            ])

            prompt_r2 = f"""You are an expert in US industry classification.

Organization: {orgname}
CRP Catcode: {top_rc} ({top_rc_name})

Three AI models were asked to classify this organization and disagreed:
  {context_str}

Reconsider carefully. Which NAICS-3 is most appropriate?

Respond ONLY in JSON: {{"naics3": "XXX", "confidence": "HIGH|MED|LOW", "reasoning": "one sentence"}}"""

            try:
                resp = client.chat.completions.create(
                    model=LLM_MODELS[0],  # Use Model 1 for tiebreaker
                    messages=[{"role": "user", "content": prompt_r2}],
                    max_tokens=200,
                )
                resp_text = resp.choices[0].message.content.strip()
                # Strip markdown code fences (Haiku wraps JSON in ```json...```)
                if resp_text.startswith("```"):
                    resp_text = resp_text.split("```")[1]
                    if resp_text.startswith("json"):
                        resp_text = resp_text[4:]  # Remove "json" language identifier
                resp_text = resp_text.strip()
                pick_r2 = json.loads(resp_text)
                final_naics3 = pick_r2.get("naics3")
                assignments.loc[assignments["org_id"] == org_id, "llm_consensus"] = "MAJORITY_R2"
                assignments.loc[assignments["org_id"] == org_id, "llm_rounds"] = 2
                print(f"    Round 2 result: {final_naics3}")
            except Exception as e:
                print(f"    Round 2 ERROR: {e}")
                assignments.loc[assignments["org_id"] == org_id, "llm_consensus"] = "UNRESOLVED"
                final_naics3 = None

            time.sleep(0.3)

        # Fill in final NAICS
        if final_naics3:
            assignments.loc[assignments["org_id"] == org_id, "assigned_naics3"] = final_naics3
            assignments.loc[assignments["org_id"] == org_id, "assigned_naics3_name"] = naics3_names.get(final_naics3, "")
            assignments.loc[assignments["org_id"] == org_id, "assignment_method"] = "llm_assigned"

        # Save every 50 orgs (resume-safety)
        if (idx + 1) % 50 == 0:
            assignments.to_csv(OUT_PATH, index=False)
            print(f"  Checkpoint saved at {idx+1}/{total_tier2}")

    # Final save
    assignments.to_csv(OUT_PATH, index=False)
    print(f"\nLLM batch complete. Results saved to {OUT_PATH}")
    return assignments


# ============================================================================
# INTERACTIVE REVIEW
# ============================================================================

def run_interactive_review(assignments: pd.DataFrame) -> pd.DataFrame:
    """
    Interactive review of all Tier 2 orgs.
    Priority: UNRESOLVED > DIVERGENT > MAJORITY > UNANIMOUS
    """
    tier2 = assignments[assignments["assignment_method"] == "llm_assigned"].copy()

    # Sort by consensus priority
    priority = {"UNRESOLVED": 0, "DIVERGENT": 1, "MAJORITY_R2": 2, "MAJORITY": 3, "UNANIMOUS": 4}
    tier2["priority"] = tier2["llm_consensus"].map(lambda x: priority.get(x, 5))
    tier2 = tier2.sort_values("priority")

    reviewed = 0
    for idx, (_, row) in enumerate(tier2.iterrows()):
        print(f"\n[{idx+1}/{len(tier2)}] {row['org_id']}")
        print(f"  Org: {row['orgname']}")
        print(f"  Catcode: {row['top_realcode']} ({row['top_realcode_name']})")
        print(f"  Consensus: {row['llm_consensus']} (Round {int(row['llm_rounds'])})")
        print(f"  Assigned: {row['assigned_naics3']} ({row['assigned_naics3_name']})")
        print(f"  Model picks:")
        for i in range(1, 4):
            print(f"    {row[f'llm_model_{i}']}: {row[f'llm_naics3_{i}']} ({row[f'llm_confidence_{i}']})")
            print(f"      {row[f'llm_reasoning_{i}']}")

        user_input = input("  Confirm? (ENTER to accept, 3-digit code to override, q to quit): ").strip()

        if user_input.lower() == "q":
            break
        elif user_input:
            # Override
            if len(user_input) in (2, 3) and user_input.isdigit():
                assignments.loc[assignments["org_id"] == row["org_id"], "assigned_naics3"] = user_input
                print(f"  -> Updated to {user_input}")

        reviewed += 1
        if reviewed % 100 == 0:
            assignments.to_csv(OUT_PATH, index=False)
            print(f"  Checkpoint saved ({reviewed} reviewed)")

    assignments.to_csv(OUT_PATH, index=False)
    print(f"\nReview complete. {reviewed} orgs reviewed. Results saved to {OUT_PATH}")
    return assignments


# ============================================================================
# MAIN
# ============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm", action="store_true", help="Run LLM batch for Tier 2")
    parser.add_argument("--review", action="store_true", help="Interactive review of Tier 2")
    args = parser.parse_args()

    # Load data
    orgs, dist, cands, crp = load_data()

    # Build universe
    universe = build_org_universe(orgs, crp, cands)
    orgnames = build_orgname_lookup(orgs)

    # Auto-assign Tier 1 & 3
    if not OUT_PATH.exists():
        print("\nAssigning Tier 1 (deterministic) and Tier 3 (special codes)...")
        assignments = assign_tier1_and_tier3(universe, cands, crp, orgnames)
        assignments.to_csv(OUT_PATH, index=False)
        print(f"Saved -> {OUT_PATH}")
    else:
        print(f"\nLoading existing assignments from {OUT_PATH}...")
        assignments = pd.read_csv(OUT_PATH)

    # Run LLM batch if requested
    if args.llm:
        assignments = run_llm_batch(assignments, dist, cands)

    # Interactive review if requested
    if args.review:
        assignments = run_interactive_review(assignments)

    print("\nDone!")


if __name__ == "__main__":
    main()
