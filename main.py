import pandas as pd
import sqlite3
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from datetime import datetime, timedelta

# --- Configuration ---
DATABASE_FILE = 'data/trading_data.db'
CACHE = {}
LOG = logging.getLogger("uvicorn")
# Define the session open time as a constant
MARKET_OPEN_HOUR_UTC = 21

# --- Helper Functions ---

def format_instrument_to_tablename(instrument: str, timeframe: str) -> str:
    if 'USD' in instrument and '_USD' not in instrument:
        instrument = instrument.replace('USD', '_USD')
    return f"{instrument.replace('/', '_')}_{timeframe}"

def get_pandas_timeframe(timeframe_str: str) -> str:
    timeframe_str = timeframe_str.upper()
    if 'M' in timeframe_str and 'MO' not in timeframe_str:
        return timeframe_str.replace('M', 'T')
    elif 'MO' in timeframe_str:
        return timeframe_str.replace('MO', 'M')
    elif 'H' in timeframe_str:
        return timeframe_str.replace('H', 'h')
    return timeframe_str

def resample_data(df, rule):
    resampling_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    if 'Volume' in df.columns:
        resampling_rules['Volume'] = 'sum'
    
    pandas_rule = get_pandas_timeframe(rule)
    # Simple resample without timezone origin to avoid timestamp mismatches
    resampled_df = df.resample(pandas_rule, label='left', closed='left').apply(resampling_rules).dropna()
    return resampled_df

def get_data_from_db(table_name):
    if table_name in CACHE:
        LOG.info(f"Cache hit for '{table_name}'.")
        return CACHE[table_name]

    LOG.info(f"Cache miss for '{table_name}'. Loading from database...")
    try:
        with sqlite3.connect(DATABASE_FILE) as con:
            df = pd.read_sql_query(f"SELECT * FROM '{table_name}'", con)
        
        df['DateTime'] = pd.to_datetime(df['DateTime'], utc=True)
        df.set_index('DateTime', inplace=True)
        
        CACHE[table_name] = df
        LOG.info(f"Successfully loaded and cached {len(df)} records for '{table_name}'.")
        return df
    except Exception as e:
        LOG.error(f"Failed to load data for table '{table_name}'. Error: {e}")
        return None
        
def parse_timeframe_to_minutes(tf_str):
    """Convert timeframe string to minutes"""
    try:
        if tf_str.upper().endswith('MO'):
            value = int(tf_str[:-2])
            return value * 43200  # 30 days * 24 hours * 60 minutes
        elif tf_str.upper().endswith('W'):
            value = int(tf_str[:-1])
            return value * 10080  # 7 days * 24 hours * 60 minutes
        else:
            value = int(tf_str[:-1])
            unit = tf_str[-1].upper()
            if unit == 'M':
                return value
            elif unit == 'H':
                return value * 60
            elif unit == 'D':
                return value * 1440  # 24 * 60
        return 0
    except (ValueError, IndexError):
        return 0

def parse_timeframe_to_seconds(tf_str):
    """Keep this for backward compatibility"""
    return parse_timeframe_to_minutes(tf_str) * 60

# --- FastAPI Application Setup ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    LOG.info("Application startup...")
    LOG.info("Pre-loading critical data into cache...")
    critical_table = format_instrument_to_tablename('XAUUSD', '1m')
    get_data_from_db(critical_table)
    yield
    LOG.info("Application shutdown. Clearing cache...")
    CACHE.clear()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Endpoints ---

@app.get("/api/data")
async def get_data(
    instrument: str = Query(...),
    timeframe: str = Query(...),
    limit: int = Query(...),
    end_date: str = Query(None),
    after_date: str = Query(None),
    replay_anchor_time: str = Query(None),
    replay_anchor_timeframe: str = Query(None)
):
    LOG.info(f"Request for {instrument} @ {timeframe}")
    base_table_name = format_instrument_to_tablename(instrument, "1m")

    base_df = get_data_from_db(base_table_name)
    if base_df is None:
        raise HTTPException(status_code=404, detail=f"Base data for {instrument} not found.")

    if replay_anchor_time and replay_anchor_timeframe:
        LOG.info("Smart Replay: Universal Anchor Mode")
        
        # Parse anchor time and convert timeframes to minutes
        anchor_start_dt = pd.to_datetime(replay_anchor_time, utc=True)
        anchor_timeframe_minutes = parse_timeframe_to_minutes(replay_anchor_timeframe)
        target_timeframe_minutes = parse_timeframe_to_minutes(timeframe)
        
        LOG.info(f"=== REPLAY CALCULATION DEBUG ===")
        LOG.info(f"Input anchor_time: {replay_anchor_time}")
        LOG.info(f"Input anchor_timeframe: {replay_anchor_timeframe}")
        LOG.info(f"Input target_timeframe: {timeframe}")
        LOG.info(f"Parsed anchor_start_dt: {anchor_start_dt}")
        LOG.info(f"Anchor timeframe in minutes: {anchor_timeframe_minutes}")
        LOG.info(f"Target timeframe in minutes: {target_timeframe_minutes}")
        
        if anchor_timeframe_minutes == 0 or target_timeframe_minutes == 0:
            raise HTTPException(status_code=400, detail="Invalid timeframe for replay.")
        
        # Calculate anchor end time using minutes (more precise)
        # For 4H (240m) starting at 11:00, end time is 11:00 + 240m + 59m = 15:59
        anchor_end_dt = anchor_start_dt + pd.to_timedelta(anchor_timeframe_minutes + 59, unit='m')
        LOG.info(f"Calculated anchor_end_dt: {anchor_end_dt}")
        
        # Universal formula using minutes
        anchor_end_minutes = anchor_end_dt.hour * 60 + anchor_end_dt.minute
        LOG.info(f"Anchor end in minutes from midnight: {anchor_end_minutes}")
        
        target_candle_start_minutes = (anchor_end_minutes // target_timeframe_minutes) * target_timeframe_minutes
        LOG.info(f"Target candle start in minutes: {target_candle_start_minutes}")
        LOG.info(f"Calculation: ({anchor_end_minutes} // {target_timeframe_minutes}) * {target_timeframe_minutes} = {target_candle_start_minutes}")
        
        # Convert back to datetime by adding minutes to the anchor date
        target_candle_start_dt = anchor_start_dt.replace(hour=0, minute=0, second=0, microsecond=0) + pd.to_timedelta(target_candle_start_minutes, unit='m')
        LOG.info(f"Final target_candle_start_dt: {target_candle_start_dt}")
        LOG.info(f"=== END REPLAY CALCULATION DEBUG ===")
        
        resampled_df = resample_data(base_df.copy(), timeframe)
        final_df = resampled_df[resampled_df.index <= target_candle_start_dt].tail(limit)

    else:
        LOG.info("Live Chart Mode")
        final_df = resample_data(base_df.copy(), timeframe)
        if after_date:
            after_datetime = pd.to_datetime(after_date, utc=True)
            final_df = final_df[final_df.index > after_datetime].head(limit)
        elif end_date:
            end_datetime = pd.to_datetime(end_date, utc=True)
            final_df = final_df[final_df.index < end_datetime].tail(limit)
        else:
            final_df = final_df.tail(limit)

    if final_df.empty:
         LOG.warning("No data found for the given parameters. Returning empty list.")
         return []

    final_df = final_df.reset_index()
    final_df['DateTime'] = final_df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    LOG.info(f"Serving {len(final_df)} records.")
    return final_df.to_dict(orient='records')


@app.get("/api/instruments")
async def get_instruments():
    return ["XAUUSD", "EURUSD", "AAPL"]