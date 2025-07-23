import pandas as pd
import sqlite3
import os

# --- CONFIGURATION ---
# The CSV file you downloaded with the Binance data
CSV_FILE_PATH = os.path.join('data', 'BTCUSDT_1m_from_2020.csv')

# The database file your main.py application uses
DATABASE_FILE_PATH = os.path.join('data', 'trading_data.db')

# The name for the new table inside the database
# IMPORTANT: This must match the format your backend expects
TABLE_NAME = 'BTCUSDT_1m'

def convert_csv_to_sqlite():
    """
    Reads data from the crypto CSV file and writes it to a new SQLite database table.
    This will replace the table if it already exists to ensure a clean import.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at '{CSV_FILE_PATH}'")
        return

    try:
        print(f"Reading data from '{CSV_FILE_PATH}'...")
        df = pd.read_csv(CSV_FILE_PATH)

        # Ensure the DateTime column is in the correct text format for the DB
        df['DateTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%Y-%m-%d %H:%M:%S')

        print(f"Found {len(df)} records in the CSV file.")

        # Connect to the SQLite database
        print(f"Connecting to database '{DATABASE_FILE_PATH}'...")
        with sqlite3.connect(DATABASE_FILE_PATH) as con:
            print(f"Writing data to new table '{TABLE_NAME}'... This may take a moment.")
            
            # Write the DataFrame to the new SQL table.
            # if_exists='replace' will drop the table first if it exists.
            df.to_sql(name=TABLE_NAME, con=con, if_exists='replace', index=False)

        print("\nSuccessfully converted crypto CSV data to the SQLite database!")
        print(f"A new table '{TABLE_NAME}' has been created in '{DATABASE_FILE_PATH}'.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    convert_csv_to_sqlite()
