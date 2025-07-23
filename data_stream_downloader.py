import os
import oandapyV20
import oandapyV20.endpoints.pricing as pricing
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import json
import time

# --- CONFIGURATION ---
load_dotenv()

OANDA_ACCESS_TOKEN = os.environ.get('OANDA_ACCESS_TOKEN')
OANDA_ACCOUNT_ID = os.environ.get('OANDA_ACCOUNT_ID')
DATABASE_FILE = 'data/trading_data.db'
INSTRUMENT_TO_STREAM = "XAU_USD"
TABLE_NAME = "XAU_USD_1s" # New table for 1-second data

# --- Candle Aggregator Class ---
class CandleAggregator:
    """
    A class to manage the aggregation of live ticks into candles.
    """
    def __init__(self, instrument):
        self.instrument = instrument
        self.current_candle = None
        self.last_tick_time = None

    def process_tick(self, tick):
        """
        Processes a single price tick and updates the current candle.
        Returns a completed candle dictionary if a new candle has been formed.
        """
        price = float(tick['price'])
        tick_time = pd.to_datetime(tick['time'])

        # ========================================================================
        # THE FIX IS HERE: Changed 'S' to 's' to resolve the FutureWarning.
        # ========================================================================
        candle_start_time = tick_time.floor('s')

        completed_candle = None

        if self.current_candle is None:
            # First tick, start a new candle
            self.current_candle = {
                "DateTime": candle_start_time,
                "Open": price,
                "High": price,
                "Low": price,
                "Close": price
            }
        elif candle_start_time > self.current_candle["DateTime"]:
            # New second has started, so the previous candle is complete
            completed_candle = self.current_candle
            
            # Start a new candle for the new second
            self.current_candle = {
                "DateTime": candle_start_time,
                "Open": price,
                "High": price,
                "Low": price,
                "Close": price
            }
        else:
            # Tick is within the current candle's second
            self.current_candle["High"] = max(self.current_candle["High"], price)
            self.current_candle["Low"] = min(self.current_candle["Low"], price)
            self.current_candle["Close"] = price

        self.last_tick_time = tick_time
        return completed_candle

def save_candle_to_db(candle):
    """
    Saves a single completed candle to the SQLite database.
    """
    if not candle:
        return

    df = pd.DataFrame([candle])
    # Format DateTime to a string that SQLite understands well
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        with sqlite3.connect(DATABASE_FILE) as con:
            df.to_sql(TABLE_NAME, con, if_exists='append', index=False)
            print(f"Saved candle: {candle['DateTime']} | C: {candle['Close']}")
    except Exception as e:
        print(f"Error saving candle to database: {e}")

def stream_live_prices():
    """
    Connects to OANDA's live price stream and processes the data.
    """
    api = oandapyV20.API(access_token=OANDA_ACCESS_TOKEN, environment="practice")
    
    # The request to get a live stream of prices
    request = pricing.PricingStream(accountID=OANDA_ACCOUNT_ID, params={"instruments": INSTRUMENT_TO_STREAM})
    
    aggregator = CandleAggregator(INSTRUMENT_TO_STREAM)

    print(f"--- Connecting to live stream for {INSTRUMENT_TO_STREAM} ---")
    print(f"--- Saving 1-second candles to table '{TABLE_NAME}' ---")
    print("--- Press CTRL+C to stop the script ---")

    while True:
        try:
            for response_chunk in api.request(request):
                data = response_chunk 
                
                if data['type'] == 'PRICE' and 'bids' in data:
                    # We will use the midpoint price for our candle
                    bid = float(data['bids'][0]['price'])
                    ask = float(data['asks'][0]['price'])
                    mid_price = (bid + ask) / 2
                    
                    tick = {'time': data['time'], 'price': mid_price}
                    
                    # Process the tick to see if a candle is complete
                    completed_candle = aggregator.process_tick(tick)
                    
                    if completed_candle:
                        save_candle_to_db(completed_candle)

        except oandapyV20.exceptions.V20Error as e:
            print(f"Stream Error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    stream_live_prices()
