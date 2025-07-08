import pandas as pd
import sqlite3
import os

# --- Configuration ---
INSTRUMENT = "XAU_USD"
GRANULARITY = "M1"
START_YEAR = 2020
CSV_FILENAME = f"{INSTRUMENT}_{GRANULARITY}_from_{START_YEAR}.csv"
CSV_PATH = os.path.join("data", CSV_FILENAME)

DB_FILENAME = "trading_data.db"
DB_PATH = os.path.join("data", DB_FILENAME)

# ========================================================================
# THE FIX: Implementing your suggestion to use an unambiguous label for Month ('1Mo')
# to avoid any potential case-sensitivity conflicts with '1m'.
# ========================================================================
TIMEFRAMES_TO_PRECALCULATE = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min",
    "30m": "30min", "45m": "45min", "1H": "1h", "2H": "2h",
    "3H": "3h", "4H": "4h", "1D": "D", "1W": "W", "1Mo": "ME", # Changed "1M" to "1Mo"
}

def create_preaggregated_database():
    """
    Reads a 1-minute CSV, pre-aggregates all timeframes in memory, and then
    writes them to a new SQLite database.
    """
    print("--- Starting Pre-Aggregated Database Creation ---")

    if not os.path.exists(CSV_PATH):
        print(f"Error: Source file not found at '{CSV_PATH}'")
        return

    print(f"Reading base 1-minute data from '{CSV_PATH}'...")
    try:
        base_df = pd.read_csv(CSV_PATH)
        base_df['DateTime'] = pd.to_datetime(base_df['DateTime'])
        print(f"Successfully read {len(base_df)} records.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # --- STAGE 1: Prepare all dataframes in memory first ---
    print("\n--- STAGE 1: Resampling all timeframes in memory ---")
    dataframes_to_write = {}
    df_for_resampling = base_df.set_index('DateTime')

    for tf_label, pd_code in TIMEFRAMES_TO_PRECALCULATE.items():
        table_name = f"{INSTRUMENT}_{tf_label}"
        print(f"  > Resampling for timeframe '{tf_label}'...")

        if tf_label == '1m':
            df_to_save = base_df.copy()
        else:
            resampled_df = df_for_resampling.resample(pd_code).agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
            
            if resampled_df.empty:
                print(f"    - No data after resampling for {tf_label}, skipping.")
                continue
            
            df_to_save = resampled_df.reset_index()
        
        dataframes_to_write[table_name] = df_to_save
        print(f"    - Prepared {len(df_to_save)} records for table '{table_name}'.")

    # --- STAGE 2: Write all prepared dataframes to the database ---
    print("\n--- STAGE 2: Writing all prepared data to the database ---")
    
    if os.path.exists(DB_PATH):
        print(f"Old database found at '{DB_PATH}'. Removing it for a clean build.")
        os.remove(DB_PATH)

    conn = None
    try:
        print(f"Connecting to new database at '{DB_PATH}'...")
        conn = sqlite3.connect(DB_PATH)

        for table_name, df in dataframes_to_write.items():
            print(f"  > Writing table '{table_name}'...")
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_datetime ON {table_name} (DateTime)")
            conn.commit()
            print(f"    - Successfully wrote and committed table '{table_name}' with {len(df)} records.")
        
        print("\n--- Pre-Aggregation Complete! ---")

    except Exception as e:
        print(f"An error occurred during database writing: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    create_preaggregated_database()
