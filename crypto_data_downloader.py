import pandas as pd
from binance.client import Client
from datetime import datetime, timezone
import os
import time

# --- User Configuration ---
# You can get API keys from your Binance account for higher request limits
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', None)
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', None)

INSTRUMENT = "BTCUSDT"
GRANULARITY = Client.KLINE_INTERVAL_1MINUTE # 1-minute candles
# ========================================================================
# THE FIX: Using UTC timestamp for start date
# ========================================================================
START_DATE = "1 Jul, 2025 UTC" 

# --- Main Script ---
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

output_dir = "data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Create a unique filename for the crypto data
output_filename = os.path.join(output_dir, f"{INSTRUMENT}_{GRANULARITY}_from_2020.csv")

print(f"Starting historical data download for {INSTRUMENT}...")
print(f"Data will be saved to: {output_filename}")

def fetch_historical_data():
    """
    Fetches historical kline/candlestick data from Binance using UTC timestamps.
    """
    try:
        # The library handles the pagination for us. It will make multiple
        # requests if necessary to get all the data from the start_str.
        print(f"Fetching all {GRANULARITY} data for {INSTRUMENT} since {START_DATE}...")
        
        klines = client.get_historical_klines(INSTRUMENT, GRANULARITY, START_DATE)
        
        if not klines:
            print("No data returned from Binance.")
            return None

        # Process the data into a pandas DataFrame with UTC timestamps
        data = []
        for k in klines:
            # Convert timestamp to UTC datetime object
            utc_datetime = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            
            data.append({
                "DateTime": utc_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                "Open": float(k[1]),
                "High": float(k[2]),
                "Low": float(k[3]),
                "Close": float(k[4]),
                "Volume": float(k[5])
            })
        
        df = pd.DataFrame(data)
        # Convert DateTime column to pandas datetime with UTC timezone
        df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
        
        # Also create a proper datetime column from the timestamp for easier manipulation (for validation)
        df['DateTimeFromTimestamp'] = pd.to_datetime([k[0] for k in klines], unit='ms', utc=True)
        
        print(f"Successfully fetched a total of {len(df)} candles.")
        print(f"All timestamps are in UTC timezone.")
        return df

    except Exception as e:
        print(f"An error occurred during fetch: {e}")
        return None

def save_data_to_csv(df):
    """
    Saves the DataFrame to a CSV file with UTC timestamps.
    """
    if df is None or df.empty:
        print("No data to save.")
        return

    try:
        # Save only the original 6 columns in simple format
        output_df = df[['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
        
        # Convert DateTime back to string format without timezone info for CSV
        output_df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # This will overwrite the file if it exists, ensuring a fresh download.
        output_df.to_csv(output_filename, index=False)
        print(f"Successfully saved {len(output_df)} records to '{output_filename}'.")
        
        # Show the date range of the final data (in UTC)
        print(f"Data range (UTC): {df['DateTime'].min()} to {df['DateTime'].max()}")
        
        # Display first few rows to verify format
        print("\nFirst 3 rows of saved data:")
        print(output_df.head(3))

    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def validate_utc_timestamps(df):
    """
    Validates that timestamps are properly in UTC.
    """
    if df is None or df.empty:
        return
    
    print(f"\n--- UTC Timestamp Validation ---")
    print(f"DateTime column timezone: {df['DateTime'].dt.tz}")
    print(f"DateTimeFromTimestamp column timezone: {df['DateTimeFromTimestamp'].dt.tz}")
    
    # Check if both datetime columns match
    time_diff = (df['DateTime'] - df['DateTimeFromTimestamp']).abs().max()
    print(f"Max difference between DateTime columns: {time_diff}")
    
    if time_diff.total_seconds() == 0:
        print("✓ Timestamp conversion is consistent")
    else:
        print("⚠ Warning: Timestamp conversion inconsistency detected")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print(f"Current system time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    historical_df = fetch_historical_data()
    
    if historical_df is not None:
        validate_utc_timestamps(historical_df)
        save_data_to_csv(historical_df)
    
    print("--- Crypto data download finished. ---")