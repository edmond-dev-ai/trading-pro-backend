import pandas as pd
import sqlite3
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

# --- Configuration ---
app = FastAPI()
DB_FILENAME = "trading_data.db"
DB_PATH = os.path.join("data", DB_FILENAME)
DATA_CACHE = {}
# List of timeframes that we have pre-calculated tables for.
STANDARD_TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "45m", "1H", "2H", "3H", "4H", "1D", "1W", "1Mo"]


# --- CORS Middleware ---
origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Functions ---
def get_db_connection():
    """Creates a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def load_base_data_for_instrument(instrument: str):
    """ Loads the base 1-minute data required for on-the-fly resampling for custom timeframes. """
    table_name_1m = f"{instrument.replace('USD', '_USD')}_1m"
    if table_name_1m in DATA_CACHE:
        return DATA_CACHE[table_name_1m]
    
    print(f"Cache miss for 1m data. Loading '{table_name_1m}' from database...")
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(f'SELECT * FROM "{table_name_1m}"', conn)
        conn.close()
        
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df.set_index('DateTime', inplace=True)
        
        DATA_CACHE[table_name_1m] = df
        print(f"Successfully loaded and cached {len(df)} records for '{table_name_1m}'.")
        return df
    except Exception as e:
        print(f"Could not load base 1m data for '{instrument}': {e}")
        DATA_CACHE[table_name_1m] = None
        return None

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "High-Performance Backend is running!"}

@app.get("/api/data")
def get_data(
    instrument: str, 
    timeframe: str, 
    limit: int,
    end_date: Optional[str] = None,
    after_date: Optional[str] = None,
):
    """
    This endpoint is now flexible enough to handle both initial loads
    and subsequent lazy-loads for the replay feature.
    """
    print(f"Request for {instrument} @ {timeframe}. Mode: {'Replay Lazy-Load' if after_date else 'Normal'}")
    
    try:
        # THE FIX: Re-introducing the logic to check for standard vs. custom timeframes.
        if timeframe in STANDARD_TIMEFRAMES:
            # --- FAST MODE ---
            table_name = f"{instrument.replace('USD', '_USD')}_{timeframe}"
            print(f"Fast mode: Querying pre-aggregated table '{table_name}'")
            conn = get_db_connection()
            if after_date:
                query = f'SELECT * FROM "{table_name}" WHERE DateTime > ? ORDER BY DateTime ASC LIMIT ?'
                params = (after_date, limit)
            else:
                if not end_date: return {"error": "end_date is required"}
                query = f'SELECT * FROM (SELECT * FROM "{table_name}" WHERE DateTime < ? ORDER BY DateTime DESC LIMIT ?) ORDER BY DateTime ASC'
                params = (end_date, limit)
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
        else:
            # --- FLEXIBLE MODE (for custom timeframes like '2m') ---
            print(f"Flexible mode: Resampling 1m data to '{timeframe}'...")
            base_df = load_base_data_for_instrument(instrument)
            if base_df is None:
                return {"error": f"Base 1m data for instrument '{instrument}' not found for resampling."}

            pd_code = timeframe.replace('m', 'min').replace('H', 'h').replace('D', 'D').replace('W', 'W').replace('Mo', 'M')
            resampled_df = base_df.resample(pd_code).agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
            
            if after_date:
                filtered_df = resampled_df.loc[resampled_df.index > after_date]
                df_chunk = filtered_df.head(limit)
            else:
                if not end_date: return {"error": "end_date is required"}
                filtered_df = resampled_df.loc[resampled_df.index < end_date]
                df_chunk = filtered_df.tail(limit)
            
            df = df_chunk.reset_index()

        if df.empty:
            return {"message": "No more data found for the specified range."}
        
        print(f"Serving {len(df)} records from timeframe '{timeframe}'.")
        return df.to_dict(orient="records")

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}
