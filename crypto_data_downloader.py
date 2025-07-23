import pandas as pd
from binance.client import Client
from datetime import datetime
import os
import time

# --- User Configuration ---
# You can get API keys from your Binance account for higher request limits
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', None)
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', None)

INSTRUMENT = "BTCUSDT"
GRANULARITY = Client.KLINE_INTERVAL_1MINUTE # 1-minute candles
# ========================================================================
# THE FIX: Changed the start date to the beginning of the current month
# ========================================================================
START_DATE = "1 Jul, 2025" 

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
    Fetches historical kline/candlestick data from Binance.
    """
    try:
        # The library handles the pagination for us. It will make multiple
        # requests if necessary to get all the data from the start_str.
        print(f"Fetching all {GRANULARITY} data for {INSTRUMENT} since {START_DATE}...")
        
        klines = client.get_historical_klines(INSTRUMENT, GRANULARITY, START_DATE)
        
        if not klines:
            print("No data returned from Binance.")
            return None

        # Process the data into a pandas DataFrame
        data = []
        for k in klines:
            data.append({
                "DateTime": datetime.fromtimestamp(k[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "Open": float(k[1]),
                "High": float(k[2]),
                "Low": float(k[3]),
                "Close": float(k[4]),
                "Volume": float(k[5])
            })
        
        df = pd.DataFrame(data)
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        print(f"Successfully fetched a total of {len(df)} candles.")
        return df

    except Exception as e:
        print(f"An error occurred during fetch: {e}")
        return None

def save_data_to_csv(df):
    """
    Saves the DataFrame to a CSV file.
    """
    if df is None or df.empty:
        print("No data to save.")
        return

    try:
        # This will overwrite the file if it exists, ensuring a fresh download.
        df.to_csv(output_filename, index=False)
        print(f"Successfully saved {len(df)} records to '{output_filename}'.")
        
        # Show the date range of the final data
        print(f"Data range: {df['DateTime'].min()} to {df['DateTime'].max()}")

    except Exception as e:
        print(f"Error saving data to CSV: {e}")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    historical_df = fetch_historical_data()
    save_data_to_csv(historical_df)
    print("--- Crypto data download finished. ---")
