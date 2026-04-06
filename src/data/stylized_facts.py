"""
Generate stylized facts and descriptive statistics for the campaign finance panel.
Matches the structure and findings reported in 'B. Political-side stylized facts'.
"""

from pathlib import Path
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
FINAL_DB_PATH = OUTPUT_DIR / "contributions.parquet"

ALL_CYCLES = ["90", "92", "94", "96", "98", "00", "02", "04", "06", "08", "10", "12", "14", "16", "18", "20", "22"]

def get_raw_stats(con, cycle):
    """Calculate raw totals and nature composition for a cycle."""
    clean_dir = DATA_DIR / "clean" / cycle
    indivs_path = str(clean_dir / "indivs.parquet").replace("\\", "/")
    pacs_path = str(clean_dir / "pacs.parquet").replace("\\", "/")
    cmte_path = str(clean_dir / "committees.parquet").replace("\\", "/")
    
    # Raw Indivs
    indiv_stats = con.execute(f"""
        SELECT 
            SUM(CAST(Amount AS DOUBLE)) as amount,
            COUNT(*) as rows,
            'indiv' as source,
            CASE
                WHEN UPPER(SUBSTRING(COALESCE(RealCode, ''), 1, 1)) = 'J' THEN 'I'
                WHEN UPPER(SUBSTRING(COALESCE(RealCode, ''), 1, 1)) = 'Z' THEN 'P'
                WHEN UPPER(SUBSTRING(COALESCE(RealCode, ''), 1, 1)) = 'L' THEN 'L'
                WHEN UPPER(SUBSTRING(COALESCE(RealCode, ''), 1, 1)) = 'X' THEN 'O'
                WHEN UPPER(SUBSTRING(COALESCE(RealCode, ''), 1, 1)) = 'Y' THEN 'U'
                ELSE 'B'
            END as nature
        FROM '{indivs_path}'
        GROUP BY nature
    """).fetch_df()
    
    # Raw PACs
    pac_stats = con.execute(f"""
        SELECT 
            SUM(CAST(p.Amount AS DOUBLE)) as amount,
            COUNT(*) as rows,
            'pac' as source,
            CASE 
                WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'B' THEN 'B'
                WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'L' THEN 'L'
                WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'I' THEN 'I'
                WHEN UPPER(SUBSTRING(COALESCE(c.RecipCode, 'B'), 2, 1)) = 'P' THEN 'P'
                ELSE 'B'
            END as nature
        FROM '{pacs_path}' p
        LEFT JOIN '{cmte_path}' c ON p.PACID = c.CmteID
        GROUP BY nature
    """).fetch_df()
    
    df = pd.concat([indiv_stats, pac_stats])
    df['cycle'] = int(cycle)
    return df

def main():
    if not FINAL_DB_PATH.exists():
        print(f"ERROR: {FINAL_DB_PATH} not found. Run Stage 7 first.")
        return

    con = duckdb.connect()
    final_path = str(FINAL_DB_PATH).replace("\\", "/")

    print("="*80)
    print("GENERATING STYLIZED FACTS")
    print("="*80)

    # 1. Collect Raw Stats
    print("\n[1/3] Collecting raw stats from Stage 1 files...")
    raw_dfs = []
    for cycle in ALL_CYCLES:
        try:
            raw_dfs.append(get_raw_stats(con, cycle))
        except Exception as e:
            print(f"  Warning: Could not process raw stats for cycle {cycle}: {e}")
    
    raw_df = pd.concat(raw_dfs)
    raw_total_all = raw_df['amount'].sum()
    
    # 2. B1: Political money growth
    print("\n--- B1. Political money growth ---")
    raw_by_cycle = raw_df.groupby('cycle')['amount'].sum()
    valid_by_cycle = con.execute(f"""
        SELECT cycle, SUM(total_amount) as amount
        FROM '{final_path}'
        WHERE naics3 IS NOT NULL
        GROUP BY cycle
        ORDER BY cycle
    """).fetch_df().set_index('cycle')['amount']

    # Peak cycle
    peak_cycle_idx = valid_by_cycle.idxmax()
    peak_cycle_year = 1900 + peak_cycle_idx if peak_cycle_idx >= 90 else 2000 + peak_cycle_idx
    print(f"Peak cycle in valid panel: {peak_cycle_year} (${valid_by_cycle.max()/1e9:.2f}B)")

    cycles_to_show = [1990, 2020, 2022]
    for c in cycles_to_show:
        r = raw_by_cycle.get(c % 100 if c < 2000 else c % 100, 0)
        v = valid_by_cycle.get(c % 100 if c < 2000 else c % 100, 0) # Wait, cycles in raw are 90, 00 etc, but final db might be INT
        # Let's fix cycle matching
        c_idx = c % 100
        r = raw_by_cycle.get(c_idx, 0)
        v = valid_by_cycle.get(c_idx, 0)
        share = (v/r)*100 if r > 0 else 0
        print(f"Cycle {c}: Raw ${r/1e6:,.1f}M, Valid Industry-Mapped ${v/1e6:,.1f}M ({share:.1f}%)")

    print(f"\nAcross all cycles combined:")
    valid_total_all = valid_by_cycle.sum()
    print(f"  Raw: ${raw_total_all/1e9:,.2f}B")
    print(f"  Valid Industry-Mapped: ${valid_total_all/1e9:,.2f}B")
    print(f"  Overall mapped share: {(valid_total_all/raw_total_all)*100:.1f}%")

    # 3. B2: Composition
    print("\n--- B2. Composition (Valid Panel) ---")
    comp_valid = con.execute(f"""
        SELECT 
            cycle,
            source,
            nature,
            SUM(total_amount) as amount,
            DI
        FROM '{final_path}'
        WHERE naics3 IS NOT NULL
        GROUP BY cycle, source, nature, DI
    """).fetch_df()

    def print_shares(target_cycle):
        c_idx = target_cycle % 100
        df_c = comp_valid[comp_valid['cycle'] == c_idx]
        total_c = df_c['amount'].sum()
        indiv_share = df_c[df_c['source'] == 'indiv']['amount'].sum() / total_c
        biz_share = df_c[df_c['nature'] == 'B']['amount'].sum() / total_c
        labor_share = df_c[df_c['nature'] == 'L']['amount'].sum() / total_c
        ideo_share = df_c[df_c['nature'] == 'I']['amount'].sum() / total_c
        indirect_share = df_c[df_c['DI'] == 'I']['amount'].sum() / total_c
        
        print(f"Cycle {target_cycle}:")
        print(f"  Indiv: {indiv_share*100:.1f}%")
        print(f"  Business: {biz_share*100:.1f}%")
        print(f"  Labor: {labor_share*100:.1f}%")
        print(f"  Ideological: {ideo_share*100:.1f}%")
        print(f"  Indirect: {indirect_share*100:.1f}%")

    print_shares(1990)
    print_shares(2022)

    # Survival rates
    print("\nFull-taxonomy comparison (Survival rates):")
    raw_nature_total = raw_df.groupby('nature')['amount'].sum()
    valid_nature_total = comp_valid.groupby('nature')['amount'].sum()
    
    for n in ['B', 'L', 'I', 'P', 'O', 'U']:
        r = raw_nature_total.get(n, 0)
        v = valid_nature_total.get(n, 0)
        s = (v/r)*100 if r > 0 else 0
        name = {'B': 'Business', 'L': 'Labor', 'I': 'Ideological', 'P': 'Party/Leadership',
                'O': 'Other (public emp/nonprofit)', 'U': 'Unknown'}[n]
        print(f"  {name}: {s:.1f}% survival (${v/1e9:,.2f}B valid / ${r/1e9:,.2f}B raw)")

    # 4. B3: Concentration
    # 4. B3.1: Partisan Alignment (Top Industries)
    print("\n--- B3.1. Partisan Alignment (Top 10 Industries, 2022) ---")
    party_stats = con.execute(f"""
        SELECT 
            naics3,
            COALESCE(naics3_name, 'Unknown') as naics3_name,
            SUM(CASE WHEN UPPER(recip_party) = 'R' THEN total_amount ELSE 0 END) as gop_amt,
            SUM(CASE WHEN UPPER(recip_party) = 'D' THEN total_amount ELSE 0 END) as dem_amt,
            SUM(total_amount) as total
        FROM '{final_path}'
        WHERE cycle = 22 AND naics3 IS NOT NULL
        GROUP BY naics3, naics3_name
        ORDER BY total DESC
        LIMIT 10
    """).fetch_df()

    for _, row in party_stats.iterrows():
        gop_p = (row['gop_amt'] / row['total']) * 100
        dem_p = (row['dem_amt'] / row['total']) * 100
        name = str(row['naics3_name'])
        print(f"  {row['naics3']} ({name[:20]}...): GOP {gop_p:.1f}%, DEM {dem_p:.1f}%")

    # 5. B3.2: Incumbency Moat (Top Industries)
    print("\n--- B3.2. Incumbency Moat (Top 10 Industries, 2022) ---")
    # CRPICO codes: I=Incumbent, C=Challenger, O=Open Seat
    inc_stats = con.execute(f"""
        SELECT 
            naics3,
            COALESCE(naics3_name, 'Unknown') as naics3_name,
            SUM(CASE WHEN UPPER(recip_incumbent) = 'I' THEN total_amount ELSE 0 END) as inc_amt,
            SUM(CASE WHEN UPPER(recip_incumbent) = 'C' THEN total_amount ELSE 0 END) as chal_amt,
            SUM(total_amount) as total
        FROM '{final_path}'
        WHERE cycle = 22 AND naics3 IS NOT NULL
        GROUP BY naics3, naics3_name
        ORDER BY total DESC
        LIMIT 10
    """).fetch_df()

    for _, row in inc_stats.iterrows():
        inc_p = (row['inc_amt'] / row['total']) * 100
        chal_p = (row['chal_amt'] / row['total']) * 100
        name = str(row['naics3_name'])
        print(f"  {row['naics3']} ({name[:20]}...): Incumbent {inc_p:.1f}%, Challenger {chal_p:.1f}%")

    con.close()

if __name__ == "__main__":
    main()
