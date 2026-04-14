# Data Dictionary: Political Contributions Panel (1990-2022)

This document describes the schema and usage for the two primary datasets produced by this pipeline.

**Last Updated:** 2026-04-10 (v1.001 release-audit and lineage update)

**Primary coding reference:** [OpenSecrets OpenData User's Guide (local copy)](./UserGuide_OpenSecrets.pdf) and the [official OpenSecrets PDF](https://www.opensecrets.org/open-data/UserGuide.pdf)

---

## 1. Core Industry Panel (`contributions.parquet`)

**Grain:** Donor industry x recipient x cycle  
**Records:** ~46.3 million (individuals + PACs, all 17 cycles)  
**Description:** The authoritative panel of U.S. political contributions mapped to 3-digit NAICS industries.

The v1.001 private build also produces a versioned candidate file, `contributions_v1001.parquet`, with the same analytical grain and additional release-audit lineage fields.

### Schema

| Column | Type | Description | NULL Meaning |
| :--- | :--- | :--- | :--- |
| `cycle` | int | Election cycle year (for example 1990, 2022) | Never NULL |
| `source` | str | `indiv` or `pac` | Never NULL |
| `org_id` | str | Resolved organization ID from entity resolution | Unresolved org |
| `orgname` | str | Standardized organization/donor name | Rare |
| `naics3` | str | 3-digit NAICS industry code | Unclassifiable non-business flow |
| `naics3_name` | str | NAICS-3 industry descriptor | When `naics3` is NULL |
| `naics_source` | str | High-level NAICS source: `org_level`, `realcode`, `primcode` | When `naics3` is NULL |
| `org_assignment_method` | str | Org-level assignment subtype: `deterministic`, `llm_assigned`, `hardcoded`, `manual_override` | Non-`org_level` row |
| `org_assignment_confidence` | str | Confidence from org-level assignment table | Non-`org_level` row |
| `org_llm_consensus` | str | LLM consensus class (`UNANIMOUS`, `MAJORITY`, `MAJORITY_R2`) | Non-LLM row |
| `org_llm_rounds` | int | Number of LLM rounds used for the org-level assignment | Non-LLM row |
| `org_assignment_tier` | str | Assignment tier (`TIER1`, `TIER2`, `TIER3`) carried into the final output | Non-`org_level` row |
| `org_top_realcode` | str | Dominant OpenSecrets real code observed for the resolved org | No resolved org |
| `org_top_realcode_share` | float | Share of org aliases associated with the dominant real code | No resolved org |
| `naics_lineage` | str | Fine-grained lineage label such as `org_level:llm_assigned:UNANIMOUS` or `realcode:best_freq_share` | When `naics3` is NULL |
| `nature` | str | Harmonized contribution-nature code: `B`, `L`, `I`, `O`, `U`, `P` | Rare |
| `DI` | str | Direct/indirect indicator from the source data | Rare |
| `total_amount` | float | Sum of contributions in dollars (may be negative for refunds) | Never NULL |
| `recip_id` | str | FEC committee or candidate ID | Never NULL |
| `recip_name` | str | Recipient name | Rare |
| `recip_party` | str | Recipient political party (`D`, `R`, `3`) | No party affiliation |
| `recip_seat` | str | Candidate office type | Non-candidate recipient |
| `recip_incumbent` | str | Candidate status (`I`, `C`, `O`) | Non-candidate recipient |

### Notes
- `nature` is a harmonized project field, not a raw one-to-one copy from a single source table.
- `nature = O` means `Other`, not `Open Seat`.
- The lineage columns are especially important for v1.001 because they make org-level method shares observable directly in the final panel.

---

## 2. Geographic Individual Panel (`indiv_geography_panel.parquet`)

**Grain:** Donor county x industry x recipient x cycle  
**Records:** ~8.9 million (individuals only)  
**Description:** Individual contributions with county attribution.

The v1.001 private build also produces `indiv_geography_panel_v1001.parquet`, together with county-diagnostic reports and a versioned ZIP lookup.

### Schema

| Column | Type | Description | NULL Meaning |
| :--- | :--- | :--- | :--- |
| `cycle_id` | int | Election cycle year | Never NULL |
| `naics3` | str | 3-digit NAICS industry code | Non-business or unclassified row |
| `donor_county_fips` | str | 5-digit county FIPS code | Unresolved ZIP / non-geocodable record |
| `recip_id` | str | FEC committee or candidate ID | Never NULL |
| `recip_party` | str | Recipient party | No party affiliation |
| `recip_seat` | str | Office type | Committee recipient |
| `recip_incumbent` | str | Candidate status | Committee recipient |
| `total_amount` | float | Sum of contributions | Never NULL |

### Geography notes
The v1.001 workflow adds supporting geography artifacts outside the parquet itself:
- `data/lookups/zip_to_county_assignment_v1001.parquet`
- `reports/v1_001/county_mapping_overall.csv`
- `reports/v1_001/county_mapping_by_cycle.csv`
- `reports/v1_001/county_mapping_by_method.csv`
- `reports/v1_001/unmatched_zip_leaderboard.csv`
- `reports/v1_001/unmatched_zip_taxonomy.csv`

The current v1.001 diagnostics map **96.8605% of rows** and **91.9004% of dollars** in the individual-enriched geography frame.

---

## 3. Validation and release-audit artifacts

The public codebase now includes release-audit utilities and documentation for:
- baseline freeze metrics
- industry provenance summaries
- stratified validation sample generation
- validation scoring after manual adjudication
- benchmark matrices
- reproducible industry-drift leaderboards

See:
- [pipeline.md](pipeline.md)
- [industry_validation_protocol.md](industry_validation_protocol.md)
- `src/release/`
- `data/validation/org_naics_validation_sample_v1001.csv`

---

## 4. Caveats

1. The validation sample and scoring workflow now exist, but substantive validation accuracy still depends on manual adjudication.
2. County coverage is high but not exhaustive; use the county diagnostics rather than assuming perfect geographic exhaustiveness.
3. Lineage columns improve transparency, but they do not by themselves certify correctness; they identify how each assignment entered the final panel.
4. Versioned candidate outputs (`*_v1001.parquet`) are release-build artifacts, while archival distribution remains through Zenodo.
