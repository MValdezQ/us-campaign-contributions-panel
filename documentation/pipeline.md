# Campaign-Finance Processing Pipeline

This document describes the full end-to-end workflow used to build the
campaign-contribution panel. A short summary appears in the repository
`README`; the sections below expand on each stage.

---

## Methodological Decisions

The following decisions govern how data is classified and processed within this pipeline. These rules ensure a consistent longitudinal panel across 32+ years of election cycles.

### Data Sources
We use **both PAC and individual contributions**:
- PACs represent organized, firm-level political spending.
- Individual contributions are assigned to an industry via the donor's employer (OpenSecrets' `RealCode`), following standard research practice.
- A `source` column (`pac` / `indiv`) is preserved to allow for channel-specific analyses.

### Nature Classification

Contributions are tagged with a "Nature" code based on Center for Responsive Politics (CRP) categories. For PACs and outside spending groups, Nature comes from the **second character** of `RecipCode` (B/L/I/O/U). For committees and candidates, `RecipCode` is still preserved, but it encodes party and candidate status rather than Nature.

| Code | Meaning | Role |
| --- | --- | --- |
| B | Business | Core industrial political activity |
| I | Ideological | Support for issue-based organizations |
| L | Labor | Labor union political activity |
| P | Political | Party/Leadership committee funding |

Nature is determined based on the donor's context for individuals and the recipient's type for PACs:

- **PACs**: Derived from the second character of `RecipCode` in the committees table (B/L/I/O/U).
- **Individuals**: Derived from the first character of `RealCode`.

### Direct vs Indirect (DI)
- **Direct (D)**: Contributions made to a candidate or their committee.
- **Indirect (I)**: Independent expenditures / soft money.

This distinction is preserved to support analyses of regulatory shifts (e.g., pre-BCRA vs. post-Citizens United).

### Industry Assignment
- **PACs**: Use `PrimCode` from the committees table. `PrimCode` is the stable, sponsor-level identifier.
- **Individuals**: Use `RealCode`, which OpenSecrets assigns based on the donor's employer (`Orgname`).

### Double-counting
The pipeline implements rules to prevent the inflation of contribution totals:
```
Individual --> PAC --> Candidate   (count once, as PAC-->Candidate)
Individual --> Candidate           (count once, as Indiv-->Candidate)
PAC --> PAC --> Candidate           (risk of double-counting)
```
- Drop INDIV-->PAC flows (identified by `RecipCode` starting with P/O in indivs data).
- Drop PAC-->PAC transfers (identified by transaction `Type`).

### Scope
Entity resolution and industry mapping are scoped to the ~56 three-digit NAICS industries present in major U.S. industrial economic datasets. This creates a high-resolution bridge between political activity and sectoral economic variables.

---

## Stage Overview

| Stage | Process | Purpose |
|-------|--------|---------|
| **1. Ingestion** | Raw TXT to Parquet | Convert header-less TXT dumps into typed Parquet tables. |
| **2. Integrity QA** | Data Validation | Row-count and schema validation on ingested files. |
| **3. Pre-aggregation** | Noise Reduction | Collapse duplicate checks by key fields to reduce dataset size. |
| **4. Entity Resolution** | Alias Clustering | Cluster contributor names to stable `org_id`s using Deezymatch + manual review. |
| **5. NAICS Assignment** | Industry Coding | Assign 3-digit NAICS codes via deterministic rules and 3-model LLM consensus. |
| **6. Industry Mapping** | Enrichment | Enrich all 17 cycle parquets with NAICS via catcode fallback + org-level override. |
| **7A. Final DB** | Integration | Join recipient metadata, filter double-counted flows, and produce the analytical database. |
| **7B. Geo Panel** | Geographic Mapping | Build a parallel individual geographic panel from `indivs_agg_enriched` files. |

After Stage 6, the workflow branches into two downstream products: the unified contribution panel and the individual geographic panel. The geographic panel does not depend on `output/contributions.parquet`; both outputs are built directly from the enriched cycle-level files.


---

## Developer environment (important)
- This repository provides a project pip config at `./.pip/pip.conf` that points to the public PyPI (https://pypi.org/simple). Use it to avoid broken or incomplete corporate mirrors.
- Local (PowerShell, session):
  - `$env:PIP_CONFIG_FILE = "$PWD\\.pip\\pip.conf"`
  - `.venv\Scripts\python -m pip install -r requirements.txt`
- CI: set `PIP_CONFIG_FILE` to `$PWD/.pip/pip.conf` in the runner before installing dependencies.
- One-off alternative: `pip install --index-url https://pypi.org/simple <package>`
- Revert: `Remove the PIP_CONFIG_FILE env var` or `pip config unset global.index-url`.

> Security: do not commit credentials. If your internal Nexus requires auth, use a secure credential store or the CI secret manager.

---

## Stage 1 -- Ingestion (DONE)

Convert each cycle's header-less `.txt` dumps from OpenSecrets into typed
Parquet files using DuckDB.

- **Input**: `data/raw/political/CampaignFinYY/{pacs,indivs,cmtes,cands}YY.txt`
- **Output**: `data/clean/{cycle}/{pacs,indivs,candidates,committees}.parquet`
- **Script**: `src/data/ingest_raw_files.py`
- Handles malformed CSVs with `IGNORE_ERRORS=TRUE`.
- SNAPPY compression for storage efficiency.

No further action required.

## Stage 2 -- Integrity QA (DONE)

Validation checks on the Parquet files produced during ingestion.

### Checks
- **Identifier validation** -- required ID columns exist and are populated.
- **Amount validation** -- amounts parse as numeric.
- **Field validation** -- mandatory columns present.

### Results (as of 2025-12-19)
- All 15 cycles validated (1990-2018).
- Zero invalid Amount parses.
- Zero missing identifiers (ContribID, PACID).
- Negative amounts present (10k-316k per cycle) -- expected refunds.
- Duplicate IDs present (256k-15M per cycle) -- expected multiple donations.

**Script**: `src/data/integrity_qa.py`
**Output**: `reports/qa_{cycle}.csv`, `reports/qa_summary_clean.csv`

## Stage 3 -- Pre-Aggregation (DONE)

Collapse duplicate cheques by key fields before heavier processing.

### Pipeline steps

1. **Staging**: Read from `data/clean/{cycle}/*.parquet`.
2. **Cleaning & validation**:
   - Trim & normalise: `PACID`, `ContribID`, `Contrib`, `RecipID`,
     `Orgname`, `UltOrg` (whitespace, lowercase).
   - Amount sanitisation: parse as float; log parse failures to
     `data/metadata/invalid_amounts_{cycle}.parquet`.
3. **Field pruning**:
   - **INDIVs**: drop `FECTransID`, `Date`, `Contrib`, `RecipID`, `Street`,
     `City`, `State`, `OtherID`, `Gender`, `Microfilm`, `Occupation`,
     `Employer`, `Source`.
   - **PACs**: drop `FECRecID`, `Date`, `FECCandID`.
4. **Grouping & aggregation**:
   - **INDIVs**: group by `Cycle, ContribID, Orgname, UltOrg, RealCode,
     Zip, RecipCode, Type, CmteID`; sum Amount, count records.
   - **PACs**: group by `Cycle, PACID, CandID, RealCode, Type, DI`;
     sum Amount, count records.
5. **Write**: `data/derived/{cycle}/{indivs_agg,pacs_agg}.parquet`.

### Validation (as of 2025-12-19)
- 0.0% discrepancy in total amounts across all cycles.
- 21-75% row reduction for individuals; 43-71% for PACs.

**Script**: `src/data/pre_aggregate.py`

## Stage 4 -- Entity Resolution ✅ DONE

Standardise contributor names and cluster them into stable organisations.

### Approach (implemented)
1. Join PAC contributions to committees on `PACID` → `CmteID` to retrieve `UltOrg` and `PrimCode` (fixes the PAC orgname gap).
2. Clean organisation names: lowercase, strip legal suffixes (Inc, Corp, LLC, LLP, etc.), normalise whitespace (`name_cleaning.py`).
3. Cluster name variants using Deezymatch at >= 0.95 cosine similarity (`entity_resolution.py`).
4. Manual cluster review: 12,578 clusters reviewed; 12,530 APPROVED, 46 REJECTED. Decisions in `CLUSTER_REVIEW_DECISIONS.md`.
5. Build enhanced alias table with RealCode distributions (`enhance_cluster_decisions.py`).

### Coverage
- 13,876 unique org names → 12,577 stable `org_id`s
- Covers ~67% of total contribution dollars across all 17 cycles

### Output
- `data/lookups/org_aliases_top_13k_enhanced.parquet` — alias → org_id mapping
- `data/lookups/org_aliases_top_13k_enhanced_realcode_distribution.parquet` — RealCode distribution per org_id

**Scripts**: `src/data/entity_resolution.py`, `src/data/name_cleaning.py`, `src/data/enhance_cluster_decisions.py`

---

## Stage 5 -- NAICS Assignment ✅ DONE

Assign a 3-digit NAICS code to each organisation in the top-13k.

### Step 5A: Build catcode→NAICS crosswalk
- Input: OpenSecrets SIC mapping Excel + CRP category file
- Output: `data/lookups/catcode_naics_candidates.csv` — one row per catcode×NAICS-3 pair with `freq_share` weights
- Invalid SIC division codes (< 100) corrected manually (`fix_catcode_crosswalk.py`)

**Scripts**: `src/data/build_catcode_naics_candidates.py`, `src/data/fix_catcode_crosswalk.py`

### Step 5B: 3-tier org-level NAICS assignment
Three tiers based on catcode ambiguity:

- **Tier 1** (~52% of orgs): catcode maps to exactly 1 NAICS-3 → auto-assign deterministically
- **Tier 2** (~31% of orgs): catcode maps to multiple NAICS-3 → 3-model LLM consensus via OpenRouter
  - Models: `anthropic/claude-haiku-4.5`, `deepseek/deepseek-v3.2`, `google/gemini-3-flash-preview`
  - Round 1: 3/3 = UNANIMOUS, 2/3 = MAJORITY, 0/3 = DIVERGENT → Round 2 with prior picks as context
- **Tier 3** (~16% of orgs): ideology (J*), party (Z*), unknown (Y*), special (X*) → NULL NAICS by design

### Output
- `data/lookups/org_naics_assignment.csv` — columns: `org_id`, `orgname_raw`, `assigned_naics3`, `assigned_naics3_name`, `tier`, `naics_method`, plus LLM consensus columns for Tier 2

**Script**: `src/data/assign_org_naics.py`

---

## Stage 6 -- Industry Mapping ✅ DONE

Enrich aggregated contribution parquets with NAICS codes.

### Two-step strategy (per contribution row)
1. **PRIMARY** (catcode-level): `RealCode` (indivs) or `PrimCode` (PACs) → `catcode_naics_candidates` → best NAICS by `freq_share`. Covers ~95–100% of business-coded contributions.
2. **OVERRIDE** (org-level, top-13k only): `Orgname` → `org_aliases` → `org_id` → `org_naics_assignment` → replaces catcode NAICS with LLM-disambiguated value.

### New columns added
| Column | Type | Description |
|--------|------|-------------|
| `org_id` | str | From entity resolution; NULL if org not in top-13k |
| `naics3` | str | Best available NAICS-3; NULL for ideology/party/unknown codes |
| `naics3_name` | str | NAICS-3 descriptor |
| `naics_source` | str | `'org_level'` \| `'realcode'` \| `'primcode'` \| NULL |

PACs also get `PrimCode`, `PACShort`, `UltOrg` from the committees join.

### Coverage (business-coded contributions)
- ~95–100% of business-coded contribution dollars get NAICS
- Overall rate lower due to ideology/party (Z*/J*/Y*) which have no NAICS by design
- Residual unexplained gap: 0.11% of all dollars (minor uncoded sub-catcodes)

**Script**: `src/data/industry_mapping.py`

---

## Stage 7A -- Final Analytical Database ✅ DONE

Produce the analysis-ready contribution-level database by joining recipient
metadata and removing double-counted flows.

### Implementation
1. Joins `indivs_agg_enriched` and `pacs_agg_enriched` to recipient metadata.
2. Derives Nature codes (Business, Labor, Ideology, etc.).
3. Filters double-counting: drops INDIV→PAC and PAC→PAC transfers.
4. Unions all 17 cycles into a single unified panel.

**Output**: `output/contributions.parquet`
**Script**: `src/data/build_final_db.py`

---

## Stage 7B -- Geographic Individual Panel ✅ DONE

Builds a parallel individual-only geographic panel by mapping donor Zip codes to County FIPS.

### Implementation
1. Builds a ZIP-to-FIPS crosswalk by joining Census and HUD-style reference files.
2. For each individual donor in the `indivs_agg_enriched` files, resolves the 5-digit Zip to a 5-digit County FIPS.
3. Aggregates data by **Cycle × NAICS-3 × County FIPS × Recipient**.

### Dependency note
- This product branches from the Stage 6 enriched individual files.
- It does not consume the Stage 7A final contribution panel.

**Output**: `output/indiv_geography_panel.parquet`
**Script**: `src/data/build_geographic_indiv_panel.py`
