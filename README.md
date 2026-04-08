# Political Contributions & Industry Panel (1990–2022)

## Overview
This repository contains the public code and documentation for a reproducible pipeline that maps U.S. campaign-finance records from [OpenSecrets](https://www.opensecrets.org/) to 3-digit NAICS industries. The resulting data products are designed for researchers who need an auditable bridge between political contributions, industry classifications, and geographic units.

Versioned parquet outputs are distributed through Zenodo rather than committed directly to the repository.

**Data archive:** [10.5281/zenodo.19335725](https://doi.org/10.5281/zenodo.19335725)

---

## 1. The Dataset
The release consists of two primary datasets:
1.  **Core Industry Panel (`contributions.parquet`)**: donor industry × recipient × cycle, approximately 46.3 million records.
2.  **Geographic Individual Panel (`indiv_geography_panel.parquet`)**: donor county × industry × recipient × cycle, approximately 8.9 million records.

Detailed schema documentation is available in [documentation/data_dictionary.md](documentation/data_dictionary.md).

---

## 2. Pipeline Stages
The pipeline is organized into six upstream construction stages followed by two downstream analytical products derived from the enriched files.

| Stage | Process | Validation / Methodology |
| :--- | :--- | :--- |
| **1. Ingest** | Raw TXT to typed Parquet (DuckDB). | `validate_stage1.py` |
| **2. QA** | Schema and ID integrity checks. | `integrity_qa.py` |
| **3. Pre-agg** | Deduplication of identical transactions. | `validate_stage3.py` |
| **4. Resolution** | Fuzzy clustering of organization names. | [entity_resolution.md](documentation/entity_resolution.md) |
| **5. Assignment** | NAICS-3 assignment via 3-model LLM consensus. | [pipeline.md](documentation/pipeline.md) |
| **6. Mapping** | Enrichment of contributions with NAICS codes. | `industry_mapping.py` |
| **7A. Final DB** | Recipient metadata join and double-count filtering. | `build_final_db.py` |
| **7B. Geo Panel** | Geographic-industrial mapping for individual donors from `indivs_agg_enriched`. | `build_geographic_indiv_panel.py` |

Implementation details are described in [documentation/pipeline.md](documentation/pipeline.md), and the entity-resolution review process is documented in [documentation/entity_resolution.md](documentation/entity_resolution.md). The geographic panel is a parallel downstream product of the Stage 6 enriched individual files rather than an input to the main contribution panel.

---

## 3. Data Integrity
Validation is built into multiple stages of the workflow:
- **Ingestion:** raw-to-clean validation reports show discrepancies between 0.0% and 0.01%, depending on cycle and source file.
- **Pre-aggregation:** validation reports show 0.0% dollar discrepancy after transaction aggregation.
- **Entity resolution transparency:** manual cluster-review decisions and organization lookup tables are documented in the repository materials.
- **Geographic coverage:** the individual geographic panel assigns a valid county FIPS code to approximately 97% of records.

---

## 4. Research Utility
The datasets are structured to support descriptive and empirical work in political economy, including:
*   **Industry spending trends:** long-run sectoral comparisons across election cycles.
*   **PAC and individual giving:** comparisons between organizational and employee channels of political participation.
*   **Geographic analysis:** county-level analysis of individual political contributions.
*   **External data integration:** straightforward merges to NAICS-based economic data and county-level demographic data.

---

## 5. Setup & Reproducibility

### 5.1. Environment Requirements
- **Python 3.12+**
- **DuckDB** 
- **Specialized packages:** `deezymatch`, `faiss-cpu`, and `torch` (for entity resolution).

### 5.2. Local Installation
We recommend an isolated virtual environment. This repository includes a project-scoped pip config to ensure access to all required packages.

```powershell
# 1. Create and activate venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Configure pip to use public PyPI index
$env:PIP_CONFIG_FILE = "$PWD\.pip\pip.conf"

# 3. Install dependencies
pip install -r requirements.txt
```

### 5.3. Reproducing the Panel
To build the panel from scratch, run Stages 1 through 6 sequentially, then build the two downstream outputs from the enriched files. 
- Main contribution panel: `build_final_db.py`
- Geographic individual panel: `build_geographic_indiv_panel.py`
- Detailed methodology: [documentation/pipeline.md](documentation/pipeline.md)
- Data schema & usage: [documentation/data_dictionary.md](documentation/data_dictionary.md)

---

## 6. Data Archive
Versioned parquet files are published through Zenodo. The repository itself contains the code, documentation, and lookup materials required to audit or reproduce the released datasets, while Zenodo serves as the archival distribution point for dataset versions and DOI-based citation.

- Zenodo DOI: [10.5281/zenodo.19335725](https://doi.org/10.5281/zenodo.19335725)
- Public repository: [github.com/MValdezQ/us-campaign-contributions-panel](https://github.com/MValdezQ/us-campaign-contributions-panel)

## 7. License
The repository code is intended to be released under the Apache License 2.0.

---

## 8. Acknowledgment
This project would not have been possible without OpenSecrets and the Center for Responsive Politics. Their public campaign-finance data infrastructure, coding work, and documentation are foundational to the entire pipeline.

- OpenSecrets: [opensecrets.org](https://www.opensecrets.org/)
- OpenSecrets OpenData User's Guide: [local copy](documentation/UserGuide_OpenSecrets.pdf) and [official PDF](https://www.opensecrets.org/open-data/UserGuide.pdf)
- If you use this repository and find it valuable, please consider supporting OpenSecrets directly: [Donate to OpenSecrets](https://www.opensecrets.org/donate)

