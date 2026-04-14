# Industry Validation Protocol (v1.001)

## Purpose
This protocol creates a reproducible manual-audit workflow for validating the org-level industry assignments in `data/lookups/org_naics_assignment.csv`.

## Inputs
- `data/lookups/org_naics_assignment.csv`
- `output/contributions.parquet`
- `data/validation/org_naics_validation_sample_v1001.csv` (generated sample)

## Sample design
The sample intentionally mixes method risk and practical importance.

### Included strata
- all `manual_override`
- weighted sample of `hardcoded`
- weighted sample of `deterministic`
- weighted sample of `llm_assigned:UNANIMOUS`
- weighted sample of `llm_assigned:MAJORITY`
- weighted sample of `llm_assigned:MAJORITY_R2`
- weighted sample of `special_code`

Weights are based on `log1p(org_level_dollars)` so the audit over-represents higher-dollar organizations without becoming a pure top-dollar census.

## Reviewer instructions
For each sampled organization:
1. Review the organization name and any obvious aliases.
2. Use public evidence only.
3. Assign the best 3-digit NAICS code when appropriate.
4. If 3-digit assignment is not feasible, record the best 2-digit industry family.
5. For special-code cases, confirm whether the non-business label appears correct.
6. Record at least one evidence source URL for every adjudicated row.

## Required fields to fill
- `validated_naics3`
- `validated_naics2`
- `validation_status`
- `evidence_source_1`
- optional `evidence_source_2`
- `adjudicator`
- `review_date`
- `review_notes`

## Allowed validation statuses
- `confirmed`
- `revised`
- `special_code_confirmed`
- `special_code_revised`
- `insufficient_public_evidence`
- `cluster_split_required`
- `pending_review`

## If you find a bad org cluster
Do **not** force a NAICS adjudication for a cluster that actually contains multiple distinct organizations.

Instead:
1. set `validation_status = cluster_split_required`
2. leave `validated_naics3` and `validated_naics2` blank
3. record at least one evidence URL showing why the aliases belong to different entities
4. explain the split problem in `review_notes`

These rows are treated as upstream entity-resolution errors and are excluded from NAICS accuracy scoring. The scorer writes them to a separate cluster-issues file so they can be fixed upstream.

## Scoring outputs
The scorer produces:
- exact 3-digit match rate
- exact 2-digit match rate
- weighted 3-digit match rate
- weighted 2-digit match rate
- method-level summaries
- LLM-consensus summaries
- assigned-versus-validated confusion counts
- separate cluster-split issue log for upstream entity-resolution fixes

## Commands
Generate sample:
```powershell
C:\SmartData\apps\Python-3.12\python.exe src\release\sample_org_naics_validation.py
```

Interactive review (recommended for manual adjudication):
```powershell
C:\SmartData\apps\Python-3.12\python.exe src\release\review_org_naics_validation.py --backup --only-priority --adjudicator "Your Name"
```

Target a specific row or organization text (including already reviewed rows):
```powershell
C:\SmartData\apps\Python-3.12\python.exe src\release\review_org_naics_validation.py --sample-id V1001-0076 --include-reviewed --adjudicator "Your Name"
```

```powershell
C:\SmartData\apps\Python-3.12\python.exe src\release\review_org_naics_validation.py --contains "blank rome" --include-reviewed --adjudicator "Your Name"
```

Score adjudicated sample:
```powershell
C:\SmartData\apps\Python-3.12\python.exe src\release\score_org_naics_validation.py
```
