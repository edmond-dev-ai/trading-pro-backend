import pandas as pd
import sqlite3
import os
import glob

# --- CONFIGURATION ---
DATA_DIRECTORY = 'data'
DATABASE_FILE_PATH = os.path.join(DATA_DIRECTORY, 'trading_data.db')

def convert_all_csv_to_sqlite():
    """
    Scans the data directory for M1 CSV files, and writes each one to a
    corresponding table in the SQLite database. This will replace tables
    if they already exist.
    """
    # Find all M1 CSV files in the specified directory
    csv_files = glob.glob(os.path.join(DATA_DIRECTORY, '*_M1_from_*.csv'))

    if not csv_files:
        print(f"No M1 CSV files found in the '{DATA_DIRECTORY}' directory.")
        print("Please make sure your CSV files are named like 'INSTRUMENT_M1_from_YEAR.csv'")
        return

    print(f"Found {len(csv_files)} CSV files to process.")

    try:
        print(f"Connecting to database '{DATABASE_FILE_PATH}'...")
        with sqlite3.connect(DATABASE_FILE_PATH) as con:
            for csv_file_path in csv_files:
                try:
                    # --- 1. Determine Table Name from Filename ---
                    # Example: 'data/XAU_USD_M1_from_2020.csv' -> 'XAU_USD_1m'
                    filename = os.path.basename(csv_file_path)
                    instrument_name = filename.split('_M1_from_')[0]
                    table_name = f"{instrument_name}_1m"
                    
                    print(f"\n--- Processing '{filename}' ---")
                    print(f"Target table: '{table_name}'")

                    # --- 2. Read and Format CSV Data ---
                    print(f"Reading data from '{csv_file_path}'...")
                    df = pd.read_csv(csv_file_path)
                    
                    # Ensure DateTime column exists and is formatted correctly
                    if 'DateTime' not in df.columns:
                        print(f"Warning: 'DateTime' column not found in '{filename}'. Skipping file.")
                        continue
                        
                    df['DateTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"Found {len(df)} records.")

                    # --- 3. Write to SQLite Database ---
                    print(f"Writing data to table '{table_name}'... This may take a moment.")
                    # This will create or replace the table with the new data.
                    df.to_sql(name=table_name, con=con, if_exists='replace', index=False)
                    print(f"Successfully wrote data to '{table_name}'.")

                except Exception as e:
                    print(f"An error occurred while processing file '{csv_file_path}': {e}")
                    print("Skipping to the next file.")

        print("\n======================================================")
        print("Successfully converted all CSV files to the SQLite database!")
        print("======================================================")

    except Exception as e:
        print(f"\nAn error occurred with the database connection: {e}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    convert_all_csv_to_sqlite()