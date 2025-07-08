import pandas as pd
from oandapyV20 import API
from oandapyV20.contrib.factories import InstrumentsCandlesFactory
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import time

# --- User Configuration ---
ACCOUNT_ID = "101-001-26896004-004"
ACCESS_TOKEN = "a81b6bc3ca858958d6d59a888ed37398-842ff2b2bbc9659cf3259c8c8a0fe3cf"
ENVIRONMENT = "practice"
INSTRUMENT = "XAU_USD"
GRANULARITY = "M1"
# ========================================================================
# THE FIX: Changed the start year to 2020 as requested
# ========================================================================
START_YEAR = 2020
END_YEAR = datetime.now().year

# --- Main Script ---
api = API(access_token=ACCESS_TOKEN, environment=ENVIRONMENT)
all_data_df = pd.DataFrame()

output_dir = "data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_filename = os.path.join(output_dir, f"{INSTRUMENT}_{GRANULARITY}_from_{START_YEAR}.csv")

print(f"Starting historical data download for {INSTRUMENT}...")
print(f"Data will be saved to: {output_filename}")

for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        if year == datetime.now().year and month > datetime.now().month:
            break

        start_date = datetime(year, month, 1)
        end_date = start_date + relativedelta(months=1) - relativedelta(seconds=1)

        from_date_str = start_date.isoformat() + "Z"
        to_date_str = end_date.isoformat() + "Z"

        print(f"  > Fetching data for {year}-{month:02d}...")

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
                    print(f"    - No data found for {year}-{month:02d}.")
                    break # Success (no data), break the retry loop

                processed_data = []
                for candle in monthly_candles:
                    if candle.get('complete'):
                        processed_data.append({
                            "DateTime": candle['time'].split('.')[0].replace('T', ' '),
                            "Open": float(candle['mid']['o']),
                            "High": float(candle['mid']['h']),
                            "Low": float(candle['mid']['l']),
                            "Close": float(candle['mid']['c']),
                        })
                
                month_df = pd.DataFrame(processed_data)
                all_data_df = pd.concat([all_data_df, month_df], ignore_index=True)

                print(f"    - Fetched {len(processed_data)} candles.")
                break # Success, break the retry loop

            except Exception as e:
                print(f"    - An error occurred for {year}-{month:02d} (Attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"    - Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    print(f"    - Max retries reached for {year}-{month:02d}. Skipping this month.")

# Save the final complete DataFrame
if not all_data_df.empty:
    print("\nDownload complete. Consolidating and saving data...")
    all_data_df.drop_duplicates(subset='DateTime', inplace=True)
    all_data_df.sort_values(by='DateTime', inplace=True)
    all_data_df.to_csv(output_filename, index=False)
    print(f"\nAll data successfully saved to '{output_filename}'")
else:
    print("\nNo data was downloaded.")
