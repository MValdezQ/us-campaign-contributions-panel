from __future__ import annotations

import argparse
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SAMPLE = PROJECT_ROOT / "data" / "validation" / "org_naics_validation_sample_v1001.csv"
DEFAULT_PRIORITY = PROJECT_ROOT / "data" / "validation" / "org_naics_validation_sample_v1001_priority_first50.csv"

ALLOWED_STATUSES = [
    "confirmed",
    "revised",
    "special_code_confirmed",
    "special_code_revised",
    "insufficient_public_evidence",
    "cluster_split_required",
    "pending_review",
]


def normalize_code(value: str) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def prompt(
    label: str,
    default: str = "",
    allow_blank: bool = True,
    validator=None,
) -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        raw = input(f"{label}{suffix}: ").strip()
        if raw == "":
            raw = default
        if raw == "" and allow_blank:
            return ""
        if validator is None:
            return raw
        ok, message = validator(raw)
        if ok:
            return raw
        print(f"  ! {message}")


def code_validator(length: int):
    def validate(value: str) -> tuple[bool, str]:
        if value == "":
            return True, ""
        if not value.isdigit():
            return False, "Use digits only."
        if len(value) != length:
            return False, f"Expected exactly {length} digits."
        return True, ""

    return validate


def status_validator(value: str) -> tuple[bool, str]:
    if value in ALLOWED_STATUSES:
        return True, ""
    return False, f"Status must be one of: {', '.join(ALLOWED_STATUSES)}"


def build_review_queue(
    sample_df: pd.DataFrame,
    priority_path: Path | None,
    only_priority: bool,
    sample_ids: list[str] | None,
    contains_text: str,
) -> pd.DataFrame:
    queue = sample_df.copy()
    if priority_path and priority_path.exists():
        priority_ids = pd.read_csv(priority_path, dtype=str)["sample_id"].astype(str).tolist()
        priority_rank = {sample_id: idx for idx, sample_id in enumerate(priority_ids)}
        queue["priority_rank"] = queue["sample_id"].map(priority_rank)
        if only_priority:
            queue = queue[queue["priority_rank"].notna()].copy()
        queue["priority_rank"] = queue["priority_rank"].fillna(10**9)
    else:
        queue["priority_rank"] = 10**9

    queue["org_level_dollars"] = pd.to_numeric(queue["org_level_dollars"], errors="coerce").fillna(0.0)
    queue["llm_consensus"] = queue["llm_consensus"].fillna("")
    queue["reviewed"] = queue["validation_status"].fillna("pending_review").ne("pending_review")

    if sample_ids:
        sample_id_set = {sample_id.strip() for sample_id in sample_ids if sample_id.strip()}
        queue = queue[queue["sample_id"].isin(sample_id_set)].copy()

    if contains_text:
        needle = contains_text.lower().strip()
        queue = queue[queue["orgname"].fillna("").str.lower().str.contains(needle, regex=False)].copy()

    queue = queue.sort_values(
        ["reviewed", "priority_rank", "assignment_method", "org_level_dollars"],
        ascending=[True, True, True, False],
    )
    return queue


def print_row_context(row: pd.Series) -> None:
    print("\n" + "=" * 88)
    print(f"{row['sample_id']} | {row['assignment_method']} | consensus={row.get('llm_consensus', '') or '-'}")
    print("-" * 88)
    print(f"Org name(s):        {row['orgname']}")
    print(f"Assigned NAICS3:    {normalize_code(row.get('assigned_naics3', ''))} | {row.get('assigned_naics3_name', '')}")
    print(f"Top realcode:       {row.get('top_realcode', '')} | {row.get('top_realcode_name', '')}")
    print(f"Top realcode share: {row.get('top_realcode_share', '')}")
    print(f"Org-level dollars:  {row.get('org_level_dollars', '')}")
    print(f"Current status:     {row.get('validation_status', '')}")
    print(f"Current notes:      {row.get('review_notes', '')}")
    print("=" * 88)


def apply_shortcut(choice: str, row: pd.Series, adjudicator: str, review_date: str) -> dict[str, str]:
    assigned = normalize_code(row.get("assigned_naics3", ""))
    auto = {
        "validated_naics3": "",
        "validated_naics2": "",
        "validation_status": "pending_review",
        "evidence_source_1": "",
        "evidence_source_2": "",
        "adjudicator": adjudicator,
        "review_date": review_date,
        "review_notes": "",
    }

    if choice == "c":
        auto["validated_naics3"] = assigned
        auto["validated_naics2"] = assigned[:2]
        auto["validation_status"] = "confirmed"
        return auto
    if choice == "s":
        auto["validation_status"] = "special_code_confirmed"
        return auto
    return auto


def review_row(row: pd.Series, adjudicator: str) -> tuple[str, dict[str, str] | None]:
    print_row_context(row)
    print("Commands: [e]dit/save  [c]onfirm assigned code  [s]pecial-code confirm  [x] cluster split required  s[k]ip  [q]uit")
    action = input("Choice: ").strip().lower()
    if action in {"q", "quit"}:
        return "quit", None
    if action in {"k", "skip"}:
        return "skip", None

    today = date.today().isoformat()
    if action in {"c", "s"}:
        values = apply_shortcut(action, row, adjudicator, today)
    elif action in {"x"}:
        values = {
            "validated_naics3": "",
            "validated_naics2": "",
            "validation_status": "cluster_split_required",
            "evidence_source_1": prompt("evidence_source_1", row.get("evidence_source_1", ""), False),
            "evidence_source_2": prompt("evidence_source_2", row.get("evidence_source_2", ""), True),
            "adjudicator": prompt("adjudicator", row.get("adjudicator", "") or adjudicator, False),
            "review_date": prompt("review_date", row.get("review_date", "") or today, False),
            "review_notes": prompt(
                "review_notes",
                row.get("review_notes", "") or "Distinct organizations appear merged in one org_id; upstream cluster split required.",
                False,
            ),
        }
    else:
        assigned = normalize_code(row.get("assigned_naics3", ""))
        default_2digit = assigned[:2] if assigned else ""
        values = {
            "validated_naics3": prompt("validated_naics3", normalize_code(row.get("validated_naics3", "")), True, code_validator(3)),
            "validated_naics2": prompt("validated_naics2", normalize_code(row.get("validated_naics2", "")) or default_2digit, True, code_validator(2)),
            "validation_status": prompt("validation_status", row.get("validation_status", "pending_review"), False, status_validator),
            "evidence_source_1": prompt("evidence_source_1", row.get("evidence_source_1", ""), False),
            "evidence_source_2": prompt("evidence_source_2", row.get("evidence_source_2", ""), True),
            "adjudicator": prompt("adjudicator", row.get("adjudicator", "") or adjudicator, False),
            "review_date": prompt("review_date", row.get("review_date", "") or today, False),
            "review_notes": prompt("review_notes", row.get("review_notes", ""), True),
        }

    if values["validation_status"] != "pending_review":
        values["adjudicator"] = values["adjudicator"] or adjudicator
        values["review_date"] = values["review_date"] or today
    return "save", values


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive reviewer for the org-level NAICS validation sample.")
    parser.add_argument("--sample-path", type=Path, default=DEFAULT_SAMPLE, help="Main validation sample CSV to update.")
    parser.add_argument("--priority-path", type=Path, default=DEFAULT_PRIORITY, help="Optional priority ordering CSV.")
    parser.add_argument("--only-priority", action="store_true", help="Restrict the queue to sample_ids listed in the priority CSV.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of rows to review this session.")
    parser.add_argument("--adjudicator", default="", help="Default adjudicator name to prefill.")
    parser.add_argument("--backup", action="store_true", help="Create a .bak copy of the main sample before editing.")
    parser.add_argument("--sample-id", action="append", default=[], help="Review a specific sample_id (repeatable).")
    parser.add_argument("--contains", default="", help="Review rows whose orgname contains this text.")
    parser.add_argument("--include-reviewed", action="store_true", help="Include rows that already have a non-pending validation status.")
    args = parser.parse_args()

    if not args.sample_path.exists():
        raise FileNotFoundError(f"Missing sample CSV: {args.sample_path}")

    sample_df = pd.read_csv(args.sample_path, dtype=str).fillna("")
    if args.backup:
        backup_path = args.sample_path.with_suffix(args.sample_path.suffix + ".bak")
        shutil.copy2(args.sample_path, backup_path)
        print(f"Backup created: {backup_path}")

    queue = build_review_queue(sample_df, args.priority_path, args.only_priority, args.sample_id, args.contains)
    if args.include_reviewed:
        pending = queue.copy()
    else:
        pending = queue[queue["validation_status"].fillna("pending_review") == "pending_review"].copy()

    if pending.empty:
        print("No matching rows found for this queue.")
        return

    total = len(pending) if args.limit <= 0 else min(len(pending), args.limit)
    print(f"Starting review session for {total} row(s).")
    print(f"Main file: {args.sample_path}")
    if args.priority_path.exists():
        print(f"Priority ordering: {args.priority_path}")

    reviewed_count = 0
    for _, row in pending.head(total).iterrows():
        action, values = review_row(row, args.adjudicator)
        if action == "quit":
            break
        if action == "skip":
            continue
        mask = sample_df["sample_id"] == row["sample_id"]
        for key, value in values.items():
            sample_df.loc[mask, key] = value
        sample_df.to_csv(args.sample_path, index=False)
        reviewed_count += 1
        print(f"Saved {row['sample_id']} ({reviewed_count}/{total})")

    print(f"Session complete. Reviewed {reviewed_count} row(s).")
    print(f"Updated file: {args.sample_path}")


if __name__ == "__main__":
    main()
