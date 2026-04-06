# Entity Resolution: Methodology and Decisions

# Campaign Finance Entity Resolution: Cluster Review Guide

## Overview
This guide explains how to review and validate organization name clustering for the campaign finance pipeline. The clustering groups similar organization names into clusters assigned a single `org_id`.

## What You're Reviewing

**File:** `data/lookups/ready_to_review_*.csv`

Each row represents ONE organization name. Rows with the same `org_id` belong to the SAME cluster and should represent the same real-world entity.

### Key Columns

| Column | Meaning | What to Look For |
|--------|---------|------------------|
| **rank** | Top-N organization by $ | Context: larger $ orgs need more scrutiny |
| **orgname_raw** | Original name from source data | The actual text to review |
| **total_amount** | Total $ attributed to this name | High $ = more impact if wrong |
| **record_count** | Number of contribution records | More records = longer history |
| **org_id** | Cluster identifier | All rows with same ID should be equivalent |
| **cluster_size** | How many names in this cluster | See below for interpretation |
| **confidence_est** | Estimated cluster quality | Higher = more likely correct |
| **source** | Where name came from (indiv/pac) | Useful for understanding context |
| **reviewer_notes** | Your feedback column | Add reasons for REJECT |

## Understanding Confidence Scores

**Formula:** `1 - (cluster_size - 1) / max_cluster_size`

- **100% confidence** = Single-member cluster (no other names)
  - âœ“ Safest to approve
  - Usually unique identifiers (FEC committee IDs, rare names)

- **90-99% confidence** = Small cluster (2-10 members)
  - Review to ensure names are truly similar
  - Example: "Boeing Co", "Boeing Company", "Boeing Inc"

- **70-89% confidence** = Medium cluster (10-100+ members)
  - âš ï¸ **Highest review priority**
  - May contain false positives
  - Example: Large "investments" cluster with 109+ members

- **<70% confidence** = Large cluster (100+ members)
  - ðŸš¨ **Requires careful review**
  - High risk of false positives
  - May need to split into sub-clusters

## What Makes a Good Cluster?

### âœ“ Clusters to APPROVE

1. **Obvious Company Variants**
   ```
   "apple inc"
   "apple inc."
   "apple, inc"
   "apple incorporated"
   â†’ All refer to Apple Inc - APPROVE
   ```

2. **Standardized Format Names** (FEC committee IDs, PAC names)
   ```
   "c00000935" â†’ FEC committee ID (always unique)
   â†’ APPROVE as single cluster
   ```

3. **Professional Categories** (if intentionally grouped)
   ```
   "retired" (109 different retirees as one occupation)
   "attorney" (multiple attorneys grouped by profession)
   â†’ These may be intentional OR errors - depends on context
   ```

4. **Known Business Name Variations**
   ```
   "mckinsey & co"
   "mckinsey and company"
   "mckinsey co"
   â†’ Same consulting firm - APPROVE
   ```

### âœ— Clusters to REJECT

1. **Different Companies with Similar Names**
   ```
   Cluster contains:
   - "microsoft corporation"
   - "microsoft research" (non-profit research division)
   â†’ REJECT - they have different EINs/purposes
   ```

2. **Generic Terms Bundled Together**
   ```
   Cluster contains:
   - "investments inc"
   - "smith investments"
   - "jones investments"
   - "tiger investments"
   â†’ These are DIFFERENT investment firms!
   â†’ REJECT with note: "Too many unrelated investment companies"
   ```

3. **Professional Category Over-Grouping**
   ```
   Cluster contains 500+ entries of:
   - "homemaker", "retired", "self-employed", "businessman"
   â†’ These are occupations, not organizations
   â†’ REJECT with note: "Occupation category, not organization"
   ```

4. **Clear Typos or Formatting Issues**
   ```
   - "3m company" vs "3M company" (case sensitivity)
   - "ppl corp" vs "pp l corp" (spacing)
   â†’ If different companies: REJECT
   â†’ If same company: APPROVE
   ```

5. **Geographic/Subsidiary Confusion**
   ```
   - "Microsoft Inc USA"
   - "Microsoft Inc Canada"
   - "Microsoft EMEA"
   â†’ These are regional offices, same company? Or different legal entities?
   â†’ NEED CONTEXT: Check if separate tax IDs
   â†’ If yes: REJECT ("Regional offices, different entities")
   ```

## Automated Validation Tools

You can run automated checks BEFORE manual review:

### 1. **Quick Statistical Check** (included in code)
```python
# Identifies outliers
python -c "
import pandas as pd
df = pd.read_csv('ready_to_review_top_13k.csv')

# Flag huge clusters
huge = df[df['cluster_size'] > 100]
print(f'âš ï¸  Clusters >100 members: {len(huge.groupby(\"org_id\"))}')

# Flag high-variance clusters (same $ from very different names)
suspicious = df.groupby('org_id').agg({
    'orgname_raw': lambda x: (x.nunique()),
    'total_amount': 'sum'
}).query('orgname_raw > 50')
print(f'âš ï¸  Clusters with {len(suspicious)} disparate names: {len(suspicious)}')
"
```

### 2. **FEC Committee ID Validation** (auto-pass)
```python
# Committee IDs (c + 8 digits) are official FEC identifiers
# They should NEVER be clustered with non-committee names
# âœ“ Safe to auto-approve all single-member committee ID clusters
```

### 3. **Occupation Categorization Detection**
Automatically flag clusters where ALL members are generic occupations:
```python
occupations = ['attorney', 'physician', 'homemaker', 'retired', 'retired',
               'self-employed', 'investor', 'businessman']
# If cluster contains only these: mark for review/rejection
```

### 4. **Semantic Similarity Validation**
Run a second-pass similarity check on flagged clusters:
```python
# For clusters with >5 members, compute pairwise similarity
# If any pair below 0.85 token_set_ratio: flag as "outlier present"
# You then decide: fix or remove?
```

## Review Workflow

### Step 1: Filter by Risk Level
Sort CSV by `confidence_est` (lowest first). Focus on:
- **< 80% confidence:** MUST review
- **80-95% confidence:** Spot-check (sample 30%)
- **> 95% confidence:** Quick scan or approve all

### Step 2: Group Review
Review in batches:
- All single-member clusters: Batch approve âœ“
- All FEC committee IDs: Batch approve âœ“
- All 2-3 member clusters: Detailed review
- All 4+ member clusters: Detailed review with caution

### Step 3: Add Notes to CSV
In `reviewer_notes` column:

**For APPROVAL:**
```
[leave blank] OR
"verified - checked against dun & bradstreet"
"common variations - same corp"
```

**For REJECTION:**
```
"Different legal entities - separate EINs"
"Generic occupation, not organization"
"Too many unrelated companies"
"Needs regional entity clarification"
```

### Step 4: Finalize
Save CSV with your notes. Send back for reprocessing with corrections.

## Automation Strategy

### What CAN be automated (safe):
1. âœ“ FEC Committee ID single-member clusters (100% auto-approve)
2. âœ“ Obvious typos/formatting (whitespace, case) â†’ merge
3. âœ“ Known corporate subsidiaries list â†’ pre-cluster
4. âœ“ High-confidence pairs (similarity > 0.99) â†’ auto-merge
5. âœ“ Generic occupation filtering â†’ flag for manual removal

### What REQUIRES manual review (complex):
1. âœ— Similar-sounding companies (could be unrelated)
2. âœ— Geographic entity disambiguation
3. âœ— Corporate restructuring/acquisitions
4. âœ— Business vs. personal name boundaries
5. âœ— Typos vs. intentionally different names

## Special Cases to Watch

### 1. **Committee Clusters**
- FEC IDs (c + 8 digits) are unique and official
- Should be single-member clusters
- Safe to auto-approve

### 2. **Large Investment Company Clusters**
Example: 109-member cluster of all "*investments" companies
- **Problem:** These are DIFFERENT investment management firms
- **Action:** Consider lowering threshold or manual splitting
- **Decision:** Does the business question need this level of detail?

### 3. **Occupation Labels**
- "retired", "attorney", "physician", etc.
- These represent PEOPLE, not organizations
- **Decision:** Should these be single clusters per occupation, or merged?
- Depends on your analysis goal

### 4. **PAC vs. Individual Name Confusion**
- Some rows marked `source: indiv` are actually business entities
- Some marked `source: pac` are committee names
- **Action:** Cross-reference with FEC database if critical

## Recommended Review Approach

### Quick Path (80% confidence):
1. Auto-approve: 100% confidence + FEC IDs (~40% of clusters)
2. Spot-check: Random sample of 50 from 90-99% confidence
3. Deep review: All < 90% confidence
4. **Time:** ~2-4 hours

### Thorough Path (95% confidence):
1. Auto-approve: 100% confidence + FEC IDs
2. Review all: 80-99% confidence (manually verify each)
3. Deep review: All < 80% confidence + validation checks
4. **Time:** ~8-12 hours

### Full Audit Path (100% confidence):
1. Manual review of EVERY cluster
2. Cross-reference with external data sources (Dun & Bradstreet, SEC, IRS)
3. Verify corporate relationships and hierarchy
4. **Time:** ~40+ hours (not practical for 2.3M orgs)

## Tools to Help You

### External Data Sources:
- **FEC.gov**: Look up committee IDs â†’ verify names
- **SEC.gov (EDGAR)**: Verify public company names
- **IRS EIN Lookup**: Verify business names by EIN
- **Dun & Bradstreet**: Company name standardization reference

### Excel Functions (for your CSV review):
```
=UNIQUE(O2:O100)  â†’ Find unique names in a cluster
=COUNTIF(O:O, O2)  â†’ Count members in cluster
=AVERAGEIF(D:D, CRITERIA)  â†’ Analyze cluster patterns
```

### Python One-Liners (for automated pre-filtering):
```python
# List all 100% confidence clusters
df[df['confidence_est'] == 1.0]['org_id'].unique()

# Show all clusters with >50 members
df.groupby('org_id').filter(lambda x: len(x) > 50)['org_id'].unique()

# Find potential occupation clusters
df[df['orgname_raw'].isin(['retired', 'attorney', ...])]
```

## Questions to Ask Yourself

For each cluster:

1. **Is it a real organization?**
   - Or is it an occupation (retired, attorney)?
   - Or a generic descriptor (none, unknown)?

2. **Would these names appear in a phone book under the same business?**
   - Yes? â†’ APPROVE
   - No? â†’ REJECT

3. **Is there a legal entity (EIN, Tax ID, License)?**
   - Single ID? â†’ APPROVE
   - Multiple IDs? â†’ REJECT (different entities)
   - No ID (occupation)? â†’ Flag for decision

4. **Are the dollar amounts plausible?**
   - Similar amounts per name? â†’ Suggests real clustering
   - One dominant amount, others tiny? â†’ Suggests false positives

5. **Could a reasonable person confuse these names?**
   - "Boeing Inc" vs "Boeing Co" â†’ Easy confusion, APPROVE
   - "Smith Investments" vs "Jones Investments" â†’ No confusion, REJECT

---

## Next Steps After Review

Once you approve clusters:
1. Save CSV with your reviewer_notes
2. Mark `approved = TRUE` for accepted clusters
3. Run enrichment script: `enrich_with_org_ids.py`
4. This updates contribution records with `org_id` column
5. Proceed to Stage 5 (panel creation)

Good luck with your review! ðŸŽ¯


---

# Cluster Review Decisions - 80% Resolution (Top 13,876 Organizations)

## Executive Summary

Comprehensive review of 12,578 organization clusters from Stage 4 entity resolution.

### Decision Breakdown

| Decision | Clusters | Pct | Amount | Pct |
|----------|----------|-----|--------|-----|
| APPROVE | 12,530 | 99.6% | $26.7B | 69.2% |
| REJECT | 46 | 0.4% | $11.9B | 30.7% |
| REVIEW | 2 | 0.0% | $58M | 0.2% |
| TOTAL | 12,578 | 100.0% | $38.6B | 100.0% |

## REJECT Clusters - $11.85 Billion

### 1. Generic/Placeholder Labels (31 clusters) - $9.28B
- "none" cluster: $4.3B (missing data)
- "[candidate contribution]": $1.16B (form placeholder)
- "[24t contribution]": $1.07B (form field code)
- Other placeholders: $2.99B
- DECISION: REJECT ALL - Not real organizations

### 2. Occupation Categories (13 clusters) - $598.99M
- homemaker: $285.5M
- physician: $230M
- accountant, actor, musician, etc.
- DECISION: REJECT ALL - People, not organizations

### 3. Large False Positive Clusters (2 clusters) - $246.52M
- "construction": 70 unrelated companies, $55.9M
- "investments": 109 unrelated companies, $190.6M
- DECISION: REJECT BOTH - Fuzzy matching errors

### 4. Generic Terms Mixed With Specific Companies (25 clusters) - $1.32B
- "education" grouped with "National Education Assn": $74.8M
- "management" grouped with "Elliott Management": $61.5M
- "architect", "filmmaker", "property manager", etc.
- DECISION: REJECT ALL - Generic + specific mismatch

## APPROVE Clusters - 12,530 (99.6%)

Legitimate organizations:
- Single-member clusters (unique names)
- Company name variants (e.g., Apple Inc vs Apple Computer)
- Regional chapters (e.g., state AFL-CIO unions)
- PAC variants (e.g., Club for Growth vs Club for Growth Action)

Examples:
- amway/alticor inc: 4 members, $22.3M (legitimate variants)
- afl-cio: 8 members, $41.9M (union federation with chapters)
- boeing co: 3 members, $5.2M (company variants)

## REVIEW Clusters - 2

### 1. Dentist Cluster - $29.2M
- "dentist": $28.6M
- "pediatric dentist": $599K
- Issue: Occupation term dominant
- Decision needed: Approve or reject?

### 2. Rancher Cluster - $28.9M
- "rancher": $28M
- "cattle rancher": $871K
- Issue: Occupation term dominant
- Decision needed: Approve or reject?

## Impact

Rejecting all 46 clusters:
- Removes: $11.85B (30.7% of amount)
- Affects: 378 organizations (2.7% of records)
- Improves: Data quality by removing non-organizations

## Recommendations

1. Reject all generic/placeholder labels (not organizations)
2. Reject all occupation categories (people, not organizations)
3. Reject large false positive clusters (fuzzy matching errors)
4. Reject generic + specific mixes (prevents data confusion)
5. USER DECISION: Handle dentist/rancher occupation cases

## Next Steps

1. Confirm REJECT decisions
2. Decide the 2 REVIEW cases
3. Generate final org_aliases with approved clusters
4. Proceed to Stage 5

