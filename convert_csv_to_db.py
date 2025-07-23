import pandas as pd
import sqlite3
import os

# --- CONFIGURATION ---
CSV_FILE_PATH = os.path.join('data', 'XAU_USD_M1_from_2020.csv')
DATABASE_FILE_PATH = os.path.join('data', 'trading_data.db')

# ========================================================================
# THE FIX: Change this table name to match what main.py expects.
# ========================================================================
TABLE_NAME = 'XAU_USD_1m' # Changed from 'XAUUSD_1m'

def convert_csv_to_sqlite():
    """
    Reads data from the specified CSV file and writes it to an SQLite database table.
    This will replace the table if it already exists.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at '{CSV_FILE_PATH}'")
        return

    try:
        print(f"Reading data from '{CSV_FILE_PATH}'...")
        df = pd.read_csv(CSV_FILE_PATH)
        df['DateTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        print(f"Found {len(df)} records in the CSV file.")

        print(f"Connecting to database '{DATABASE_FILE_PATH}'...")
        with sqlite3.connect(DATABASE_FILE_PATH) as con:
            print(f"Writing data to table '{TABLE_NAME}'... This may take a moment.")
            
            # This will now create/replace the correctly named table.
            df.to_sql(name=TABLE_NAME, con=con, if_exists='replace', index=False)

        print("\nSuccessfully converted CSV data to the SQLite database!")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    convert_csv_to_sqlite()