# Political Contributions & Industry Panel (1990–2022)

## Overview
This repository provides a reproducible, auditable pipeline to build a comprehensive panel of U.S. political contribution flows. The pipeline maps campaign finance data from the [OpenSecrets](https://www.opensecrets.org/) database to 3-digit NAICS industries, creating a high-resolution bridge between political spending and industrial economic data.

---

## 1. Directory Structure
The project is organized into three distinct environments to maintain a clean separation between data infrastructure, public assets, and private research.

- **`src/`, `data/`, `documentation/`**: The core "Infrastructure" environment. This contains the full pipeline (Stages 1-8) and the reference files used to build the panel.
- **`campaign_finance_public/`**: A "Clean Room" repository prepped for public release. It contains meticulously scrubbed documentation and code, logic lookups, and a data guide.
- **`paper/`**: A private research environment for dataset documentation, manuscript drafting (Data Note), and stylized fact exploration. It is kept in the private repository rather than the public clean-room repo.

---

## 2. The Dataset
The final output consists of two enriched, contribution-level databases:
1.  **Core Industry Panel (`contributions.parquet`)**: donor industry × recipient × cycle. (~46.3M records, individuals + PACs).
2.  **Geographic Individual Panel (`indiv_geography_panel.parquet`)**: donor county × industry × recipient × cycle. (~8.9M records, individuals only; retains individual contributions to PACs/outside groups).

---

## 3. Pipeline Stages
| Stage | Process | Validation |
| :--- | :--- | :--- |
| **1. Ingest** | Raw TXT to typed Parquet (DuckDB). | `validate_stage1.py` |
| **2. QA** | Schema and ID integrity checks. | `integrity_qa.py` |
| **3. Pre-agg** | Deduplication of identical transactions. | `validate_stage3.py` |
| **4. Entity Resolution** | Fuzzy clustering of organization names. | `CLUSTER_REVIEW_DECISIONS.md` |
| **5. Industry Assignment** | NAICS-3 assignment via 3-model LLM consensus. | `assign_org_naics.py` |
| **6. Industry Mapping** | Enrichment of contributions with NAICS codes. | `industry_mapping.py` |
| **7. Final Database** | Recipient metadata join and double-count filtering. | `build_final_db.py` |
| **8. Geo Panel** | Geographic mapping for individual donors. | `build_geographic_indiv_panel.py` |

---

## 4. Setup & Usage
Requires Python 3.12+.
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# Use project pip config for specialized packages
$env:PIP_CONFIG_FILE = "$PWD\.pip\pip.conf"
pip install -r requirements.txt
```
To reproduce the panel, run scripts 1-8 in `src/data/` sequentially. Detailed methodology is in `documentation/pipeline.md`.

---

## 5. Acknowledgment
This project depends heavily on OpenSecrets and the Center for Responsive Politics. Their coding work, documentation, and long-run campaign-finance data infrastructure made the panel possible.

- OpenSecrets: [opensecrets.org](https://www.opensecrets.org/)
- OpenSecrets OpenData User's Guide: [local copy](documentation/UserGuide_OpenSecrets.pdf) and [official PDF](https://www.opensecrets.org/open-data/UserGuide.pdf)
- If this repository is useful to you, please consider supporting OpenSecrets directly: [Donate to OpenSecrets](https://www.opensecrets.org/donate)
