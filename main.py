import pandas as pd
import sqlite3
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import json
import pandas_ta as ta # --- MODIFICATION: Added pandas_ta import ---

# --- Configuration ---
DATABASE_FILE = 'data/trading_data.db'
CACHE = {}
LOG = logging.getLogger("uvicorn")

# --- Helper Functions ---

def format_instrument_to_tablename(instrument: str, timeframe: str) -> str:
    """
    Formats the instrument name from the frontend to the correct table name.
    """
    if instrument == "XAUUSD":
        formatted_instrument = "XAU_USD"
    else:
        formatted_instrument = instrument.replace('/', '_')
        
    return f"{formatted_instrument}_{timeframe}"

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
    try:
        if tf_str.upper().endswith('MO'):
            value = int(tf_str[:-2])
            return value * 43200
        elif tf_str.upper().endswith('W'):
            value = int(tf_str[:-1])
            return value * 10080
        else:
            value = int(tf_str[:-1])
            unit = tf_str[-1].upper()
            if unit == 'M':
                return value
            elif unit == 'H':
                return value * 60
            elif unit == 'D':
                return value * 1440
        return 0
    except (ValueError, IndexError):
        return 0

# --- ADDED: Dynamic indicator helper functions ---
def prepare_indicator_args(params: dict) -> dict:
    """Convert parameter values to appropriate types for pandas_ta functions"""
    clean_params = {}
    for key, value in params.items():
        if key in ['id', 'name']:
            continue
        
        # Try to convert to numeric, keep as string if it fails
        try:
            if isinstance(value, str):
                # Handle boolean strings
                if value.lower() in ['true', 'false']:
                    clean_params[key] = value.lower() == 'true'
                else:
                    # Try numeric conversion
                    clean_params[key] = pd.to_numeric(value)
            else:
                clean_params[key] = value
        except (ValueError, TypeError):
            clean_params[key] = value
    
    return clean_params

def get_indicator_columns(df: pd.DataFrame, indicator_name: str, before_cols: set) -> list:
    """Find new columns added after indicator calculation"""
    after_cols = set(df.columns)
    new_cols = after_cols - before_cols
    
    # Filter to likely indicator columns (usually contain the indicator name or are numeric)
    indicator_cols = []
    for col in new_cols:
        col_upper = str(col).upper()
        if (indicator_name.upper() in col_upper or 
            col_upper.startswith(indicator_name.upper()) or
            any(char.isdigit() for char in str(col))):
            indicator_cols.append(col)
    
    return sorted(indicator_cols)

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

# --- Shared Logic ---
async def get_chart_data(
    instrument: str,
    timeframe: str,
    limit: int,
    end_date: str | None = None,
    after_date: str | None = None,
    replay_anchor_time: str | None = None,
    replay_anchor_timeframe: str | None = None
):
    LOG.info(f"Request for {instrument} @ {timeframe}")
    base_table_name = format_instrument_to_tablename(instrument, "1m")

    base_df = get_data_from_db(base_table_name)
    if base_df is None:
        LOG.error(f"Base data for table '{base_table_name}' not found.")
        return []

    if replay_anchor_time and replay_anchor_timeframe:
        anchor_start_dt = pd.to_datetime(replay_anchor_time, utc=True)
        anchor_timeframe_minutes = parse_timeframe_to_minutes(replay_anchor_timeframe)
        target_timeframe_minutes = parse_timeframe_to_minutes(timeframe)
        if anchor_timeframe_minutes == 0 or target_timeframe_minutes == 0:
            raise HTTPException(status_code=400, detail="Invalid timeframe for replay.")
        anchor_end_dt = anchor_start_dt + pd.to_timedelta(anchor_timeframe_minutes + 59, unit='m')
        anchor_end_minutes = anchor_end_dt.hour * 60 + anchor_end_dt.minute
        target_candle_start_minutes = (anchor_end_minutes // target_timeframe_minutes) * target_timeframe_minutes
        target_candle_start_dt = anchor_start_dt.replace(hour=0, minute=0, second=0, microsecond=0) + pd.to_timedelta(target_candle_start_minutes, unit='m')
        resampled_df = resample_data(base_df.copy(), timeframe)
        final_df = resampled_df[resampled_df.index <= target_candle_start_dt].tail(limit)
    else:
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
         return []

    final_df = final_df.reset_index()
    final_df['DateTime'] = final_df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    LOG.info(f"Serving {len(final_df)} records.")
    return final_df.to_dict(orient='records')

# --- API Endpoints ---
@app.get("/api/data")
async def get_data_http(
    instrument: str = Query(...),
    timeframe: str = Query(...),
    limit: int = Query(...),
    end_date: str = Query(None),
    after_date: str = Query(None),
    replay_anchor_time: str = Query(None),
    replay_anchor_timeframe: str = Query(None)
):
    return await get_chart_data(
        instrument=instrument,
        timeframe=timeframe,
        limit=limit,
        end_date=end_date,
        after_date=after_date,
        replay_anchor_time=replay_anchor_time,
        replay_anchor_timeframe=replay_anchor_timeframe
    )

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    LOG.info("WebSocket connection established.")
    try:
        while True:
            request_data = await websocket.receive_text()
            request_json = json.loads(request_data)
            action = request_json.get("action")
            LOG.info(f"WebSocket received message: {action}")
            
            response_payload = {}

            # --- MODIFIED: Dynamic indicator calculation logic ---
            if action == "get_indicator":
                params = request_json.get("params", {})
                indicator_name = params.get("name", "").lower()
                indicator_id = params.get("id", f"{indicator_name}_default")
                ohlc_data = request_json.get("data", [])

                if not ohlc_data:
                    response_payload = {"action": "error", "message": "No data provided for indicator calculation"}
                    await websocket.send_json(response_payload)
                    continue

                try:
                    # Convert to DataFrame
                    df = pd.DataFrame(ohlc_data)
                    df['time'] = pd.to_numeric(df['time'])
                    df['datetime'] = pd.to_datetime(df['time'], unit='s', utc=True)
                    df.set_index('datetime', inplace=True)

                    # Store original columns to identify new ones
                    original_columns = set(df.columns)

                    # --- DYNAMIC INDICATOR CALCULATION ---
                    if hasattr(ta, indicator_name):
                        # Get the indicator function
                        indicator_func = getattr(ta, indicator_name)
                        
                        # Prepare arguments
                        clean_args = prepare_indicator_args(params)
                        
                        LOG.info(f"Calculating {indicator_name} with args: {clean_args}")
                        
                        # Calculate indicator using pandas_ta's strategy approach
                        try:
                            # Method 1: Try using df.ta() method (recommended approach)
                            df.ta(kind=indicator_name, append=True, **clean_args)
                        except Exception as e1:
                            LOG.warning(f"df.ta() failed for {indicator_name}: {e1}. Trying direct function call.")
                            try:
                                # Method 2: Direct function call
                                if indicator_name in ['sma', 'ema', 'rsi', 'roc', 'mom']:
                                    # Single series indicators
                                    result = indicator_func(df['close'], **clean_args)
                                elif indicator_name in ['macd']:
                                    # MACD returns multiple series
                                    result = indicator_func(df['close'], **clean_args)
                                elif indicator_name in ['bbands']:
                                    # Bollinger Bands
                                    result = indicator_func(df['close'], **clean_args)
                                elif indicator_name in ['stoch']:
                                    # Stochastic needs high, low, close
                                    result = indicator_func(df['high'], df['low'], df['close'], **clean_args)
                                elif indicator_name in ['atr']:
                                    # ATR needs high, low, close
                                    result = indicator_func(df['high'], df['low'], df['close'], **clean_args)
                                else:
                                    # Generic approach - try with close price
                                    result = indicator_func(df['close'], **clean_args)
                                
                                # Handle result assignment
                                if isinstance(result, pd.DataFrame):
                                    for col in result.columns:
                                        df[col] = result[col]
                                elif isinstance(result, pd.Series):
                                    df[f"{indicator_name.upper()}"] = result
                                else:
                                    raise ValueError(f"Unexpected result type: {type(result)}")
                                    
                            except Exception as e2:
                                LOG.error(f"Direct function call failed for {indicator_name}: {e2}")
                                response_payload = {"action": "error", "message": f"Failed to calculate {indicator_name}: {str(e2)}"}
                                await websocket.send_json(response_payload)
                                continue

                        # Find new indicator columns
                        indicator_cols = get_indicator_columns(df, indicator_name, original_columns)
                        
                        if not indicator_cols:
                            response_payload = {"action": "error", "message": f"No output columns found for {indicator_name}"}
                            await websocket.send_json(response_payload)
                            continue

                        # Filter columns for Bollinger Bands to only include BBL, BBM, BBU
                        if indicator_name == 'bbands':
                            indicator_cols = [col for col in indicator_cols if any(prefix in str(col).upper() for prefix in ['BBL', 'BBM', 'BBU'])]

                        LOG.info(f"Final indicator columns to send: {indicator_cols}")

                        # Prepare list for all indicator lines
                        lines_data = []
                        for col_name in indicator_cols:
                            result_df = df.dropna(subset=[col_name])
                            
                            if result_df.empty:
                                LOG.warning(f"No valid data for column {col_name}")
                                continue
                            
                            indicator_data = [
                                {"time": int(row['time']), "value": float(row[col_name])}
                                for _, row in result_df.iterrows()
                                if pd.notna(row[col_name])
                            ]

                            # Create unique ID for each line
                            if len(indicator_cols) == 1:
                                line_id = indicator_id
                            else:
                                # Multiple columns - create unique IDs
                                line_id = f"{indicator_id}_{len(lines_data)}"

                            lines_data.append({
                                "indicator_id": line_id,
                                "line_name": str(col_name),
                                "data": indicator_data,
                                "params": params
                            })

                        # Send all lines in one message
                        response_payload = {
                            "action": "indicator_data",
                            "lines": lines_data
                        }
                        
                    else:
                        available_indicators = [attr for attr in dir(ta) if not attr.startswith('_') and callable(getattr(ta, attr))]
                        response_payload = {
                            "action": "error", 
                            "message": f"Indicator '{indicator_name}' not found. Available indicators: {available_indicators[:20]}..."
                        }

                except Exception as e:
                    LOG.error(f"Error calculating indicator {indicator_name}: {e}", exc_info=True)
                    response_payload = {"action": "error", "message": f"Error calculating {indicator_name}: {str(e)}"}

            else:
                # This is the existing logic for fetching chart data
                data = await get_chart_data(
                    instrument=request_json.get("instrument"),
                    timeframe=request_json.get("timeframe"),
                    limit=request_json.get("limit"),
                    end_date=request_json.get("end_date"),
                    after_date=request_json.get("after_date"),
                    replay_anchor_time=request_json.get("replay_anchor_time"),
                    replay_anchor_timeframe=request_json.get("replay_anchor_timeframe")
                )
                
                response_payload = {
                    "action": action,
                    "data": data
                }
                if "requestId" in request_json:
                    response_payload["requestId"] = request_json["requestId"]

            await websocket.send_json(response_payload)
            LOG.info(f"WebSocket response sent for action: {response_payload.get('action')}")

    except WebSocketDisconnect:
        LOG.info("WebSocket connection closed.")
    except Exception as e:
        LOG.error(f"Error in WebSocket: {e}", exc_info=True)
        await websocket.close(code=1011)

@app.get("/api/instruments")
async def get_instruments():
    return ["XAUUSD", "EURUSD", "BTCUSDT", "AAPL"]

# --- ADDED: Get available indicators endpoint ---
@app.get("/api/indicators")
async def get_available_indicators():
    """Return list of available indicators from pandas_ta"""
    try:
        # Get all callable functions from pandas_ta that don't start with underscore
        indicators = []
        for attr_name in dir(ta):
            if not attr_name.startswith('_'):
                attr = getattr(ta, attr_name)
                if callable(attr) and hasattr(attr, '__doc__'):
                    indicators.append({
                        "name": attr_name,
                        "display_name": attr_name.upper(),
                        "description": (attr.__doc__ or "").split('\n')[0] if attr.__doc__ else ""
                    })
        
        return {"indicators": sorted(indicators, key=lambda x: x['name'])[:100]}  # Limit to first 100
    except Exception as e:
        LOG.error(f"Error getting indicators: {e}")
        return {"indicators": []}