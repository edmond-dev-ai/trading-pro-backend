import os
import oandapyV20
import oandapyV20.endpoints.instruments as instruments
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from oandapyV20.contrib.factories import InstrumentsCandlesFactory
import time
from datetime import datetime, timezone

# --- CONFIGURATION ---
load_dotenv()

OANDA_ACCESS_TOKEN = os.environ.get('OANDA_ACCESS_TOKEN')
OANDA_ACCOUNT_ID = os.environ.get('OANDA_ACCOUNT_ID')
DATABASE_FILE = 'data/trading_data.db'
INSTRUMENT_TO_FETCH = "XAU_USD"
TABLE_NAME = "XAUUSD_1m"
GRANULARITY = "M1"

def ensure_database_directory():
    """Ensure the data directory exists"""
    os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True)

def get_last_timestamp_from_db():
    """
    Connects to the database and gets the timestamp of the last saved candle.
    Returns the timestamp in the format OANDA expects, or None if no data exists.
    """
    try:
        ensure_database_directory()
        with sqlite3.connect(DATABASE_FILE) as con:
            cursor = con.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}';")
            if cursor.fetchone() is None:
                print(f"Table '{TABLE_NAME}' not found. Will start download from a default date.")
                return None
            
            # Get the last timestamp from the database
            cursor.execute(f"SELECT DateTime FROM '{TABLE_NAME}' ORDER BY DateTime DESC LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                last_timestamp_str = result[0]
                print(f"Last timestamp in DB: {last_timestamp_str}")
                
                # Parse the timestamp and add 1 minute to get the next candle
                last_ts = pd.to_datetime(last_timestamp_str)
                
                # Ensure timezone awareness
                if last_ts.tz is None:
                    last_ts = last_ts.tz_localize('UTC')
                
                next_ts = last_ts + pd.Timedelta(minutes=1)
                oanda_format_ts = next_ts.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
                print(f"Will fetch from: {oanda_format_ts}")
                return oanda_format_ts
            else:
                print("Table is empty. Will start download from a default date.")
                return None
                
    except Exception as e:
        print(f"Error reading from database: {e}")
        return None

def get_current_utc_time():
    """Get current UTC time in OANDA format"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000000000Z')

def fetch_and_save_all_missing_data(start_date=None):
    """
    Uses InstrumentsCandlesFactory to fetch all data from a start date
    until the present, saving it to the database in chunks.
    """
    if not OANDA_ACCESS_TOKEN:
        print("ERROR: OANDA_ACCESS_TOKEN not found in environment variables!")
        return
    
    client = oandapyV20.API(access_token=OANDA_ACCESS_TOKEN, environment="practice")
    
    params = {"granularity": GRANULARITY, "price": "M"}
    if start_date:
        params["from"] = start_date
        # Add 'to' parameter to avoid getting future data
        params["to"] = get_current_utc_time()

    print(f"Starting download loop for {INSTRUMENT_TO_FETCH} with params: {params}")

    total_candles_fetched = 0
    chunks_processed = 0
    
    try:
        for chunk in InstrumentsCandlesFactory(instrument=INSTRUMENT_TO_FETCH, params=params):
            try:
                client.request(chunk)
                response = chunk.response
                chunks_processed += 1

                if not response.get('candles'):
                    print("No more new candle data from OANDA.")
                    break

                data = []
                for candle in response['candles']:
                    if candle['complete']:
                        data.append({
                            "DateTime": candle['time'].split('.')[0].replace('T', ' '),
                            "Open": float(candle['mid']['o']),
                            "High": float(candle['mid']['h']),
                            "Low": float(candle['mid']['l']),
                            "Close": float(candle['mid']['c']),
                            "Volume": int(candle['volume'])
                        })
                
                if not data:
                    print(f"Chunk {chunks_processed}: No complete candles found.")
                    continue

                chunk_df = pd.DataFrame(data)
                chunk_df['DateTime'] = pd.to_datetime(chunk_df['DateTime'])
                
                new_records = save_data_to_db(chunk_df)
                total_candles_fetched += new_records
                
                print(f"Chunk {chunks_processed}: Processed {len(chunk_df)} candles, {new_records} were new.")
                
                # Rate limiting to be nice to OANDA's servers
                time.sleep(0.5)

            except oandapyV20.exceptions.V20Error as e:
                print(f"OANDA API Error during chunk {chunks_processed}: {e}")
                if "401" in str(e):
                    print("Authentication error - check your access token!")
                break
            except Exception as e:
                print(f"Unexpected error during chunk {chunks_processed}: {e}")
                break
    
    except Exception as e:
        print(f"Error in main download loop: {e}")
    
    print(f"\nFinished download loop. Processed {chunks_processed} chunks.")
    print(f"Total new candles saved: {total_candles_fetched}")

def save_data_to_db(df):
    """
    Saves the DataFrame to the SQLite database, handling duplicates and table creation.
    Returns the number of new records saved.
    """
    if df is None or df.empty:
        print("No data to save.")
        return 0

    try:
        ensure_database_directory()
        with sqlite3.connect(DATABASE_FILE) as con:
            cursor = con.cursor()
            
            # Check if the table exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}';")
            table_exists = cursor.fetchone() is not None

            df_to_save = df.copy()

            # If the table already exists, check for duplicates
            if table_exists:
                # Get existing timestamps for comparison
                existing_dates_query = f"SELECT DateTime FROM '{TABLE_NAME}' WHERE DateTime >= ? AND DateTime <= ?"
                min_date = df['DateTime'].min()
                max_date = df['DateTime'].max()
                
                existing_dates = pd.read_sql_query(
                    existing_dates_query, 
                    con, 
                    params=[min_date, max_date]
                )
                
                if not existing_dates.empty:
                    # Convert to string for comparison
                    existing_dates_set = set(existing_dates['DateTime'].astype(str))
                    df_to_save = df[~df['DateTime'].astype(str).isin(existing_dates_set)]

            if df_to_save.empty:
                return 0

            # Save new records
            df_to_save.to_sql(TABLE_NAME, con, if_exists='append', index=False)
            
            if not table_exists:
                # Create an index on DateTime for better performance
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_datetime ON '{TABLE_NAME}' (DateTime)")
                print(f"Created table '{TABLE_NAME}' with {len(df_to_save)} initial records.")
            
            return len(df_to_save)

    except Exception as e:
        print(f"Error saving data to database: {e}")
        return 0

def import_existing_csv_data(csv_file_path):
    """
    Import existing CSV data into the database.
    This is useful for initial setup when you already have historical data.
    """
    if not os.path.exists(csv_file_path):
        print(f"CSV file not found: {csv_file_path}")
        return
    
    try:
        print(f"Importing data from {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        
        # Add Volume column if it doesn't exist
        if 'Volume' not in df.columns:
            df['Volume'] = 0
        
        records_saved = save_data_to_db(df)
        print(f"Imported {records_saved} records from CSV.")
        
    except Exception as e:
        print(f"Error importing CSV data: {e}")

def get_data_summary():
    """Get a summary of the data in the database"""
    try:
        with sqlite3.connect(DATABASE_FILE) as con:
            cursor = con.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}';")
            if cursor.fetchone() is None:
                print(f"Table '{TABLE_NAME}' not found.")
                return
            
            # Get count and date range
            cursor.execute(f"SELECT COUNT(*), MIN(DateTime), MAX(DateTime) FROM '{TABLE_NAME}'")
            count, min_date, max_date = cursor.fetchone()
            
            print(f"\n--- Data Summary for {TABLE_NAME} ---")
            print(f"Total records: {count:,}")
            print(f"Date range: {min_date} to {max_date}")
            
            if count > 0:
                # Calculate data gaps
                cursor.execute(f"""
                    SELECT DateTime, 
                           LAG(DateTime) OVER (ORDER BY DateTime) as prev_datetime
                    FROM '{TABLE_NAME}'
                    ORDER BY DateTime
                """)
                
                gaps = []
                for row in cursor.fetchall():
                    if row[1]:  # prev_datetime exists
                        current = pd.to_datetime(row[0])
                        previous = pd.to_datetime(row[1])
                        diff = current - previous
                        if diff > pd.Timedelta(minutes=1):
                            gaps.append((previous, current, diff))
                
                if gaps:
                    print(f"\nFound {len(gaps)} data gaps:")
                    for prev, curr, diff in gaps[:5]:  # Show first 5 gaps
                        print(f"  Gap from {prev} to {curr} ({diff})")
                    if len(gaps) > 5:
                        print(f"  ... and {len(gaps) - 5} more gaps")
                else:
                    print("\nNo significant data gaps found.")
            
    except Exception as e:
        print(f"Error getting data summary: {e}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("--- Starting Data Collector ---")
    
    # Uncomment the following line if you want to import existing CSV data first
    # import_existing_csv_data("data/XAU_USD_M1_from_2020.csv")
    
    # Show current data summary
    get_data_summary()
    
    # Get the last timestamp and fetch new data
    last_timestamp = get_last_timestamp_from_db()
    
    if last_timestamp:
        print(f"\nResuming data collection from: {last_timestamp}")
    else:
        print("\nStarting fresh data collection...")
    
    fetch_and_save_all_missing_data(last_timestamp)
    
    # Show updated summary
    get_data_summary()
    
    print("\n--- Data Collector finished its run. ---")

# This script fetches historical 1-minute candlestick data for XAU_USD from the OANDA API
# and stores it in a SQLite database. It handles database creation, duplicate entries,
# and data gaps.  The script also includes functions to import data from a CSV file and to
# display a summary of the existing data.  Rate limiting is implemented to prevent overloading
# the OANDA servers.  Environment variables are used for OANDA API credentials and database
# configuration.
