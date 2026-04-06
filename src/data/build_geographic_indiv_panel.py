"""
Stage 8: Build the Geographic Individual Panel.
Maps individual contributions to donor counties (FIPS) using Zip codes.
"""

import pandas as pd
from pathlib import Path
import duckdb
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DERIVED_DIR = DATA_DIR / "derived"
CLEAN_DIR = DATA_DIR / "clean"
LOOKUPS_DIR = DATA_DIR / "lookups"
OUTPUT_DIR = PROJECT_ROOT / "output"

ALL_CYCLES = ["90", "92", "94", "96", "98", "00", "02", "04", "06", "08", "10", "12", "14", "16", "18", "20", "22"]

def build_zip_fips_map():
    print("Building ZIP-to-FIPS mapping...")
    # Load crosswalks
    # scpike geo-data: state_fips,state,state_abbr,zipcode,county,city
    zip_county = pd.read_csv(LOOKUPS_DIR / "zip_county_fips.csv", dtype={'zipcode': str, 'state_fips': str}, encoding='latin1')
    # kjhealy fips_master: fips,county_name,state_abbr,state_name,...
    fips_master = pd.read_csv(LOOKUPS_DIR / "fips_master.csv", dtype={'fips': str, 'state': str, 'county': str}, encoding='latin1')
    
    # Clean zip_county
    zip_county = zip_county.dropna(subset=['zipcode', 'county', 'state_abbr'])
    zip_county['zipcode'] = zip_county['zipcode'].str.strip()
    # Ensure 5-digit zero-padding
    zip_county['zipcode'] = zip_county['zipcode'].str.zfill(5)
    # Keep only numeric 5-digit zips
    zip_county = zip_county[zip_county['zipcode'].str.match(r'^\d{5}$')]
    
    # Clean names for join
    def clean_name(s):
        return str(s).lower().replace(' county', '').replace('.', '').replace(' ', '').replace("'", "")
        
    zip_county['c_name'] = zip_county['county'].apply(clean_name)
    fips_master['c_name'] = fips_master['county_name'].apply(clean_name)
    
    # Join on state and cleaned county name
    mapping = zip_county.merge(
        fips_master[['fips', 'state_abbr', 'c_name']],
        on=['state_abbr', 'c_name'],
        how='inner'
    )
    
    # Handle duplicates (Zip in multiple counties) - pick first for this version
    mapping = mapping.drop_duplicates(subset=['zipcode'])
    
    print(f"  Resolved {len(mapping)} unique ZIP-to-FIPS mappings")
    return mapping[['zipcode', 'fips']]

def process_cycles(mapping_df):
    con = duckdb.connect()
    con.register("zip_map", mapping_df)
    
    final_output_path = OUTPUT_DIR / "indiv_geography_panel.parquet"
    
    dfs = []
    total_dollars_panel = 0
    
    for cycle in ALL_CYCLES:
        in_path = DERIVED_DIR / cycle / "indivs_agg_enriched.parquet"
        cmte_path = CLEAN_DIR / cycle / "committees.parquet"
        cand_path = CLEAN_DIR / cycle / "candidates.parquet"
        
        if not in_path.exists():
            continue
            
        print(f"Processing cycle {cycle}...")
        
        # Load and join in DuckDB for speed
        # Joining with committees and candidates to get recipient party/seat/incumbent
        query = f"""
            WITH cand_deduped AS (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY CandID ORDER BY CandID) as rn
                    FROM read_parquet('{str(cand_path).replace("\\", "/")}')
                ) WHERE rn = 1
            ),
            enriched_indivs AS (
                SELECT 
                    i.*,
                    LPAD(CAST(m.fips AS VARCHAR), 5, '0') as donor_county_fips
                FROM read_parquet('{str(in_path).replace("\\", "/")}') i
                JOIN zip_map m ON LPAD(CAST(i.Zip AS VARCHAR), 5, '0') = m.zipcode
                WHERE i.naics3 IS NOT NULL
            )
            SELECT 
                CAST({cycle} AS INTEGER) as cycle_id,
                e.naics3,
                e.donor_county_fips,
                e.CmteID as recip_id,
                COALESCE(cnd.Party, c.Party) as recip_party,
                cnd.DistIDCurr as recip_seat,
                cnd.CRPICO as recip_incumbent,
                SUM(e.total_amount) as total_amount
            FROM enriched_indivs e
            LEFT JOIN read_parquet('{str(cmte_path).replace("\\", "/")}') c
                ON UPPER(TRIM(e.CmteID)) = UPPER(TRIM(c.CmteID))
            LEFT JOIN cand_deduped cnd
                ON UPPER(TRIM(c.RecipID)) = UPPER(TRIM(cnd.CandID))
            GROUP BY cycle_id, e.naics3, donor_county_fips, recip_id, recip_party, recip_seat, recip_incumbent
        """
        
        try:
            df_cycle = con.execute(query).fetch_df()
            dfs.append(df_cycle)
            total_dollars_panel += df_cycle['total_amount'].sum()
        except Exception as e:
            print(f"  Error in cycle {cycle}: {e}")
            
    if not dfs:
        print("No data processed.")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_parquet(final_output_path, compression='snappy')
    print(f"Successfully saved {len(final_df)} rows to {final_output_path}")
    print(f"Total dollars in geographic panel: ${total_dollars_panel/1e9:.2f}B")

if __name__ == "__main__":
    m = build_zip_fips_map()
    process_cycles(m)
