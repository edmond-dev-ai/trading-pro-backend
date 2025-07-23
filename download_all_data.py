import pandas as pd
from oandapyV20 import API
from oandapyV20.contrib.factories import InstrumentsCandlesFactory
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import time

# --- User Configuration ---
ACCOUNT_ID = "101-001-26896004-004"
ACCESS_TOKEN = "a81b6bc3ca858958d6d59a888ed37398-842ff2b2bbc9659cf3259c8c8a0fe3cf"
ENVIRONMENT = "practice"
INSTRUMENT = "XAU_USD"
GRANULARITY = "M1"
START_YEAR = 2020  # Only used if no existing data file found

# --- Main Script ---
api = API(access_token=ACCESS_TOKEN, environment=ENVIRONMENT)

output_dir = "data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_filename = os.path.join(output_dir, f"{INSTRUMENT}_{GRANULARITY}_from_{START_YEAR}.csv")

print(f"Starting historical data download for {INSTRUMENT}...")
print(f"Data will be saved to: {output_filename}")

# Check if existing data file exists and determine starting point
last_timestamp = None
if os.path.exists(output_filename):
    print(f"Found existing data file: {output_filename}")
    try:
        # Read only the last few rows to get the last timestamp efficiently
        existing_data = pd.read_csv(output_filename, parse_dates=['DateTime'])
        if not existing_data.empty:
            last_timestamp = existing_data['DateTime'].iloc[-1]
            print(f"Last recorded timestamp: {last_timestamp}")
            
            # Start from the last recorded timestamp (inclusive) to catch any gaps
            start_datetime = last_timestamp
            print(f"Continuing from: {start_datetime} (inclusive)")
        else:
            print("Existing file is empty. Starting from the beginning.")
            start_datetime = datetime(START_YEAR, 1, 1)
    except Exception as e:
        print(f"Error reading existing file: {e}")
        print("Starting from the beginning.")
        start_datetime = datetime(START_YEAR, 1, 1)
else:
    print("No existing data file found. Starting from the beginning.")
    start_datetime = datetime(START_YEAR, 1, 1)

# Set end datetime to current time
end_datetime = datetime.now()

print(f"Download period: {start_datetime} to {end_datetime}")

# If start_datetime is already past current time, no need to download
if start_datetime >= end_datetime:
    print("Data is already up to date!")
    exit()

# Initialize DataFrame to store new data
new_data_df = pd.DataFrame()

# Download data month by month from start_datetime to end_datetime
current_date = start_datetime.replace(day=1)  # Start from beginning of the month

while current_date <= end_datetime:
    # Calculate month boundaries
    month_start = current_date
    month_end = month_start + relativedelta(months=1) - relativedelta(seconds=1)
    
    # Adjust for the actual start and end dates
    actual_start = max(month_start, start_datetime)
    actual_end = min(month_end, end_datetime)
    
    # Skip if this month is entirely before our start date
    if actual_end < start_datetime:
        current_date += relativedelta(months=1)
        continue
    
    from_date_str = actual_start.strftime('%Y-%m-%dT%H:%M:%S') + "Z"
    to_date_str = actual_end.strftime('%Y-%m-%dT%H:%M:%S') + "Z"
    
    print(f"  > Fetching data for {current_date.year}-{current_date.month:02d} (from {actual_start} to {actual_end})...")
    
    params = {
        "granularity": GRANULARITY,
        "from": from_date_str,
        "to": to_date_str,
    }
    
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 10
    for attempt in range(MAX_RETRIES):
        try:
            # This list will hold all candles for the current month.
            monthly_candles = []
            
            # InstrumentsCandlesFactory creates a generator that yields requests.
            # We loop through each request, execute it, and add its data to our list.
            for r in InstrumentsCandlesFactory(instrument=INSTRUMENT, params=params):
                api.request(r)
                
                # Check if the response for this chunk is valid
                if 'candles' not in r.response:
                    raise Exception(f"Unexpected response from OANDA: {r.response}")

                # Add the fetched candles from this chunk to our monthly list
                monthly_candles.extend(r.response.get('candles', []))

            if not monthly_candles:
                print(f"    - No data found for {current_date.year}-{current_date.month:02d}.")
                break # Success (no data), break the retry loop

            processed_data = []
            for candle in monthly_candles:
                if candle.get('complete'):
                    candle_datetime = datetime.fromisoformat(candle['time'].split('.')[0].replace('T', ' '))
                    
                    # Only include candles that are after our last recorded timestamp
                    if last_timestamp is None or candle_datetime > last_timestamp:
                        processed_data.append({
                            "DateTime": candle['time'].split('.')[0].replace('T', ' '),
                            "Open": float(candle['mid']['o']),
                            "High": float(candle['mid']['h']),
                            "Low": float(candle['mid']['l']),
                            "Close": float(candle['mid']['c']),
                        })
            
            if processed_data:
                month_df = pd.DataFrame(processed_data)
                new_data_df = pd.concat([new_data_df, month_df], ignore_index=True)
                print(f"    - Fetched {len(processed_data)} new candles.")
            else:
                print(f"    - No new data found for {current_date.year}-{current_date.month:02d}.")
            
            break # Success, break the retry loop

        except Exception as e:
            print(f"    - An error occurred for {current_date.year}-{current_date.month:02d} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"    - Retrying in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                print(f"    - Max retries reached for {current_date.year}-{current_date.month:02d}. Skipping this month.")
    
    # Move to next month
    current_date += relativedelta(months=1)

# Save the new data
if not new_data_df.empty:
    print(f"\nDownload complete. Found {len(new_data_df)} new records.")
    
    # Remove duplicates and sort
    new_data_df.drop_duplicates(subset='DateTime', inplace=True)
    new_data_df.sort_values(by='DateTime', inplace=True)
    
    # If existing file exists, append new data
    if os.path.exists(output_filename):
        print("Appending new data to existing file...")
        # Read existing data
        existing_data = pd.read_csv(output_filename)
        
        # Combine existing and new data
        combined_data = pd.concat([existing_data, new_data_df], ignore_index=True)
        
        # Remove any duplicates that might have occurred during the merge
        combined_data.drop_duplicates(subset='DateTime', inplace=True)
        combined_data.sort_values(by='DateTime', inplace=True)
        
        # Save combined data
        combined_data.to_csv(output_filename, index=False)
        print(f"Successfully appended {len(new_data_df)} new records to '{output_filename}'")
        print(f"Total records in file: {len(combined_data)}")
    else:
        # Save new data as new file
        new_data_df.to_csv(output_filename, index=False)
        print(f"Successfully saved {len(new_data_df)} new records to '{output_filename}'")
        
    # Show the date range of the final data
    if os.path.exists(output_filename):
        final_data = pd.read_csv(output_filename, parse_dates=['DateTime'])
        if not final_data.empty:
            print(f"Data range: {final_data['DateTime'].min()} to {final_data['DateTime'].max()}")
else:
    print("\nNo new data was downloaded. Your data is already up to date!")