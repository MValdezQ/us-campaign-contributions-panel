# Data Dictionary: Political Contributions Panel (1990–2022)

This document describes the schema and usage for the two primary datasets produced by this pipeline.

**Last Updated:** 2026-03-31 (Stage 8 Geographic Panel Fix)

---

## 1. Core Industry Panel (`contributions.parquet`)

**Grain:** Donor (Industry) × Recipient × Cycle
**Records:** ~46.3 million (individuals + PACs, all 17 cycles: 1990, 1992, ..., 2022)
**Total Dollar Value:** ~$58.5 billion
**Description:** The authoritative panel of U.S. political contributions mapped to 3-digit NAICS industries.

### Schema

| Column | Type | Description | NULL Meaning |
| :--- | :--- | :--- | :--- |
| `cycle` | int | Election year (e.g., 1990, 2022) | Never NULL |
| `source` | str | `indiv` (Individual/employee) or `pac` (PAC/organization) | Never NULL |
| `org_id` | str | Resolved Organization ID from entity resolution (Stage 4) | Unresolved org (rare) |
| `orgname` | str | Standardized organization/donor name | Never NULL |
| `naics3` | str | 3-digit NAICS industry code | Unclassifiable (ideology, party, lobbying) |
| `naics3_name` | str | NAICS-3 industry descriptor | When naics3 is NULL |
| `naics_source` | str | How NAICS was assigned: `org_level` (entity resolution) or `realcode` (donation category) | When naics3 is NULL |
| `nature` | str | Donor type: `B` (Business), `L` (Labor Union), `I` (Ideology/PAC), `P` (Party) | Rare |
| `DI` | str | Individual/PAC indicator for FEC source | Rare |
| `total_amount` | float | Sum of contributions to this recipient in $dollars (may be negative = refund) | Never NULL |
| `recip_id` | str | FEC Committee or Candidate ID | Never NULL |
| `recip_name` | str | Recipient Name | Never NULL |
| `recip_party` | str | Recipient political party: `D` (Dem), `R` (Rep), `3` (Other/Independent) | No party affiliation |
| `recip_seat` | str | Candidate office type: `F` (Federal), `S` (State), `L` (Local), `P` (Presidential) | Non-candidate recipients (committees) |
| `recip_incumbent` | str | Candidate status: `I` (Incumbent), `C` (Challenger), `O` (Open Seat) | Non-candidate recipients |

### Key Observations
- **NAICS Coverage:** ~67% of records have valid NAICS-3 codes. Remaining 33% are contributions to ideology orgs, party committees, lobbying groups, etc.
- **Negative Amounts:** Occasional refunds appear as negative contributions.
- **Industry Mapping:** Individuals auto-classified by "real code" (FEC donation category). Top 13K organizations via LLM consensus (Stage 5).

---

## 2. Geographic Individual Panel (`indiv_geography_panel.parquet`)

**Grain:** Donor County × Industry × Recipient × Cycle
**Records:** ~8.9 million (individuals only, all 17 cycles)
**Total Dollar Value:** ~$57.3 billion
**Description:** Individual contributions with geographic attribution. Excludes PACs. ZIP codes mapped to county FIPS via Census 2020 ZCTA crosswalk.

### Schema

| Column | Type | Description | NULL Meaning |
| :--- | :--- | :--- | :--- |
| `cycle_id` | int | Election year (1990–2022) | Never NULL |
| `naics3` | str | 3-digit NAICS industry code | Tier 3 org (ideology, party, lobbying) |
| `donor_county_fips` | str | 5-digit zero-padded county FIPS code | PO Box ZIP / unmappable location |
| `recip_id` | str | FEC Committee or Candidate ID | Never NULL |
| `recip_party` | str | Recipient party: `D`, `R`, `3` | No party affiliation |
| `recip_seat` | str | Office type: `F`, `S`, `L`, `P` | Committee recipient |
| `recip_incumbent` | str | Candidate status: `I`, `C`, `O` | Committee recipient |
| `total_amount` | float | Sum of contributions ($) | Never NULL |

### Data Quality Notes

**Coverage:**
- **Individuals:** All individual contributions from Stage 6 enrichment
- **PACs:** Excluded (separate in `contributions.parquet`)
- **Geographic:** ~97% of records have valid donor_county_fips (3% unmappable)
- **Industry:** ~76% have valid naics3; 24% are Tier 3 (ideology/party/lobbying)

**Known Limitations:**
- PO Box ZIPs (`00000`, `10075`, `10150`, etc.) cannot be geographically attributed → `donor_county_fips = NULL`
- Unresolved/unmappable ZIP codes → `donor_county_fips = NULL` (affects ~$330M in cycle 22)
- Individuals with NULL naics3 are **included** (not filtered) — reflects legitimate non-business contributions

**Stage 8 Fix (2026-03-31):**
- Previous version: 6.4M rows (filtered WHERE naics3 IS NOT NULL, INNER JOIN on ZIP mapping)
- Current version: 8.9M rows (+39.1% recovery)
- Fix: Removed naics3 filter, upgraded ZIP mapping to Census ZCTA file, changed to LEFT JOIN
- Result: All individual contributions now included; geographic attribution is NULL-safe

---

## 3. Data Relationships & Joins

### How to extend with external data:

**Census/Economic Data:**
```sql
SELECT
  indiv_geography_panel.*,
  bds.employment, bds.payroll
FROM indiv_geography_panel
JOIN census_bds bds
  ON indiv_geography_panel.naics3 = bds.naics3
  AND indiv_geography_panel.cycle_id = bds.year
```

**County Demographics:**
```sql
SELECT
  indiv_geography_panel.*,
  county_data.population, county_data.median_income
FROM indiv_geography_panel
JOIN county_demographics county_data
  ON indiv_geography_panel.donor_county_fips = county_data.fips
```

**Recipient Legislative Data:**
- Use `recip_id` to join with FEC candidate/committee databases
- Use `recip_party`, `recip_seat`, `recip_incumbent` for filtering

---

## 4. Technical Notes

- **File Format:** Apache Parquet (columnar), Snappy compression
- **Encoding:** UTF-8 (all text columns)
- **Tool Recommendation:** DuckDB (fastest), Pandas, or Polars for analysis
- **Dates:** All cycles are even years (1990, 1992, ..., 2022); no off-cycle elections included
- **FEC IDs:** `C` prefix = Committee, `H`/`S`/`P` prefix = Candidate; length varies

---

## 5. Data Caveats

1. **Double-Counting (Mitigated):** Individual donations itemized separately by amount may appear as multiple rows if split across recipients/cycles. Stage 7 applies deduplication logic.
2. **Industry Assignment Confidence:** Org-level (Stage 5 LLM) > realcode-level (auto); see `naics_source` column.
3. **Party Coding:** `3` = Independent/No Party; some candidates file with no party affiliation.
4. **Lobbying Firms:** Categorized as `nature=I` (Ideology) with NULL naics3, separate from business firms.
5. **Non-Itemized Contributions:** Excluded (under $200 threshold in FEC data).
