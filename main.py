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

def get_market_type(instrument: str) -> str:
    """
    Determines the market type based on instrument to apply correct daily alignment.
    """
    instrument_upper = instrument.upper()
    
    # Gold and precious metals - METALS_AND_INDICES
    if instrument_upper in ['XAUUSD', 'XAGUSD', 'XAU_USD', 'XAG_USD']:
        return 'METALS_AND_INDICES'
    
    # Stock indices (common ones) - METALS_AND_INDICES
    elif any(idx in instrument_upper for idx in ['SPX', 'NAS', 'DOW', 'DAX', 'FTSE', 'NIKKEI', 'ASX']):
        return 'METALS_AND_INDICES'
    
    # Forex pairs (most common pattern) - FOREX_AND_CRYPTO
    elif len(instrument_upper) == 6 or '_' in instrument_upper:
        return 'FOREX_AND_CRYPTO'
    
    # Crypto - FOREX_AND_CRYPTO
    elif any(crypto in instrument_upper for crypto in ['BTC', 'ETH', 'LTC', 'XRP']):
        return 'FOREX_AND_CRYPTO'
    
    # Default to forex behavior
    return 'FOREX_AND_CRYPTO'

def get_daily_offset(instrument: str) -> str:
    """
    Returns the daily offset based on market type and Oanda's session times.
    - Metals/Indices: 21:00 UTC (actual market open time)
    - Forex/Crypto: 21:00 UTC (24/7 market with daily reset at 21:00)
    """
    market_type = get_market_type(instrument)
    
    if market_type == 'METALS_AND_INDICES':
        return '21H'  # 21:00 UTC start (actual market open)
    else:  # FOREX_AND_CRYPTO
        return '21H'  # 21:00 UTC start

def get_pandas_timeframe(timeframe_str: str) -> str:
    timeframe_str = timeframe_str.upper()
    if 'M' in timeframe_str and 'MO' not in timeframe_str:
        return timeframe_str.replace('M', 'T')
    elif 'MO' in timeframe_str:
        return timeframe_str.replace('MO', 'M')
    elif 'H' in timeframe_str:
        return timeframe_str.replace('H', 'h')
    return timeframe_str

def is_daily_or_above(timeframe: str) -> bool:
    """
    Check if timeframe is daily or above (D, W, MO)
    """
    timeframe_upper = timeframe.upper()
    return 'D' in timeframe_upper or 'W' in timeframe_upper or 'MO' in timeframe_upper

def get_resample_anchor(timeframe: str, instrument: str = None) -> str:
    """
    Returns the proper anchor for resampling based on timeframe and instrument.
    
    For metals/indices (have trading gaps):
    - Daily and above: Use market-specific alignment (21:00 UTC offset)
    - Hour-based timeframes: Use even/odd logic for TradingView alignment
      * Even hours (2H, 4H, 6H, 8H, etc.): Use TradingView alignment with 1H offset
      * Odd hours (3H, 5H, 7H, etc.): Use actual timestamp alignment (no offset)
    - Minute timeframes: Standard resampling
    
    For forex/crypto (continuous 24/7 data):
    - ALL timeframes: Use right labeling with right boundary to match TradingView timestamps
    """
    timeframe_upper = timeframe.upper()
    market_type = get_market_type(instrument) if instrument else 'FOREX_AND_CRYPTO'
    
    # For forex/crypto - use -1 offset for ALL timeframes
    if market_type == 'FOREX_AND_CRYPTO':
        return 'FOREX_CRYPTO_OFFSET'
    
    # For metals/indices only - apply different logic based on timeframe
    elif market_type == 'METALS_AND_INDICES':
        # For daily and above timeframes
        if is_daily_or_above(timeframe):
            if 'D' in timeframe_upper:
                return 'D_MARKET_SPECIFIC'
            elif 'W' in timeframe_upper:
                return 'W_MARKET_SPECIFIC'
            else:  # Monthly
                return 'MO_MARKET_SPECIFIC'
        
        # For hour-based timeframes (even/odd logic for metals/indices only)
        elif 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                return 'H_NO_OFFSET'  # 1H doesn't need offset
            elif hours % 2 == 0:  # Even hours (2H, 4H, 6H, 8H, etc.)
                return 'H_EVEN_OFFSET'  # Use TradingView alignment with 1H offset
            else:  # Odd hours (3H, 5H, 7H, etc.)
                return 'H_ODD_NO_OFFSET'  # Use actual timestamp alignment
        
        # For minute timeframes (standard resampling for metals/indices)
        else:
            return None
    
    # Default fallback
    return None

def resample_data(df, rule, instrument=None):
    """
    Resamples OHLCV data with different strategies based on instrument type.
    
    For metals/indices (XAUUSD) - have trading gaps:
    - Daily and above: Use market-specific offset (21:00 UTC)
    - Hour-based: Use even/odd logic for TradingView alignment
    - Minutes: Standard resampling
    
    For forex/crypto (EURUSD, BTCUSDT) - continuous 24/7 data:
    - ALL timeframes: Use -3H offset with left labeling to match TradingView timestamps
    """
    resampling_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    if 'Volume' in df.columns:
        resampling_rules['Volume'] = 'sum'
    
    pandas_rule = get_pandas_timeframe(rule)
    anchor = get_resample_anchor(rule, instrument)
    
    # Ensure the DataFrame index is timezone-aware UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    elif df.index.tz != 'UTC':
        df.index = df.index.tz_convert('UTC')
    
    # Handle different anchor types
    if anchor == 'FOREX_CRYPTO_OFFSET':
        # For forex/crypto (ALL timeframes) - use origin and offset for proper alignment
        timeframe_upper = rule.upper()
        
        if 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                # 1H: use offset to match TradingView (showing 7 instead of 8)
                resampled_df = df.resample(
                    pandas_rule,
                    origin='start_day',
                    offset='-1H',     # Start 1 hour earlier
                    label='left',
                    closed='right'
                ).apply(resampling_rules).dropna()
            else:
                # Multi-hour timeframes: align to midnight (00:00)
                resampled_df = df.resample(
                    pandas_rule,
                    origin='start_day',  # Start from midnight 00:00
                    label='left',
                    closed='left'
                ).apply(resampling_rules).dropna()
        else:
            # Daily and above: standard resampling
            resampled_df = df.resample(
                pandas_rule,
                origin='start_day',
                offset='-1H',
                label='left',
                closed='right'
            ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_EVEN_OFFSET':
        # Even hour timeframes for metals/indices only - use TradingView alignment
        # These align to 01:00, 03:00, 05:00, etc. instead of 00:00, 02:00, 04:00
        resampled_df = df.resample(
            pandas_rule, 
            origin='start_day',  # Start from beginning of day (00:00 UTC)
            offset='1H',         # 1 hour offset to align with TradingView's timing
            label='left',        # Label with the start time of the interval
            closed='left'        # Include left boundary, exclude right
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_ODD_NO_OFFSET':
        # Odd hour timeframes for metals/indices only - use actual timestamp alignment
        # These align to 00:00, 03:00, 06:00, 09:00, etc. (natural hour boundaries)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',  # Start from beginning of day (00:00 UTC)
            # No offset - use natural hour boundaries
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_NO_OFFSET':
        # 1H timeframe for metals/indices - no offset needed
        resampled_df = df.resample(
            pandas_rule,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'D_MARKET_SPECIFIC':
        # Daily timeframe with market-specific offset (metals/indices only)
        daily_offset = get_daily_offset(instrument)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=daily_offset,    # Market-specific daily start time (21:00 UTC)
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'W_MARKET_SPECIFIC':
        # Weekly timeframe with market-specific offset (metals/indices only)
        daily_offset = get_daily_offset(instrument)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=daily_offset,    # Use same offset as daily for weekly alignment
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'MO_MARKET_SPECIFIC':
        # Monthly timeframe with market-specific offset (metals/indices only)
        daily_offset = get_daily_offset(instrument)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=daily_offset,    # Use same offset as daily for monthly alignment
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    # For other timeframes, use standard resampling (metals/indices minute timeframes)
    else:
        # Minute-based timeframes (1m, 5m, 15m, 30m) - no offset needed
        resampled_df = df.resample(
            pandas_rule, 
            label='left', 
            closed='left'
        ).apply(resampling_rules).dropna()
    
    return resampled_df

# --- ADDED: Alternative manual 4H resampling function ---
def resample_4h_tradingview_manual(df):
    """
    Manual 4H resampling that exactly matches TradingView's behavior.
    TradingView 4H candles: 01:00-05:00, 05:00-09:00, 09:00-13:00, 13:00-17:00, 17:00-21:00, 21:00-01:00
    """
    resampling_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    if 'Volume' in df.columns:
        resampling_rules['Volume'] = 'sum'
    
    # Ensure UTC timezone
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    elif df.index.tz != 'UTC':
        df.index = df.index.tz_convert('UTC')
    
    # Create a custom grouper for TradingView 4H alignment
    def get_tradingview_4h_group(timestamp):
        hour = timestamp.hour
        # TradingView 4H groups: 1-4->1, 5-8->5, 9-12->9, 13-16->13, 17-20->17, 21-24/0->21
        if 1 <= hour <= 4:
            group_hour = 1
        elif 5 <= hour <= 8:
            group_hour = 5
        elif 9 <= hour <= 12:
            group_hour = 9
        elif 13 <= hour <= 16:
            group_hour = 13
        elif 17 <= hour <= 20:
            group_hour = 17
        else:  # 21-24 and 0
            if hour >= 21:
                group_hour = 21
            else:  # hour == 0
                # Midnight belongs to the 21:00-01:00 group of the previous day
                prev_day = timestamp - pd.Timedelta(days=1)
                return prev_day.replace(hour=21, minute=0, second=0, microsecond=0)
        
        return timestamp.replace(hour=group_hour, minute=0, second=0, microsecond=0)
    
    # Group by 4H periods and resample
    df_grouped = df.groupby(get_tradingview_4h_group).apply(
        lambda x: pd.Series({
            'Open': x['Open'].iloc[0] if len(x) > 0 else None,
            'High': x['High'].max() if len(x) > 0 else None,
            'Low': x['Low'].min() if len(x) > 0 else None,
            'Close': x['Close'].iloc[-1] if len(x) > 0 else None,
            'Volume': x['Volume'].sum() if 'Volume' in x.columns and len(x) > 0 else None
        })
    ).dropna()
    
    return df_grouped

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
        resampled_df = resample_data(base_df.copy(), timeframe, instrument)
        final_df = resampled_df[resampled_df.index <= target_candle_start_dt].tail(limit)
    else:
        final_df = resample_data(base_df.copy(), timeframe, instrument)
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