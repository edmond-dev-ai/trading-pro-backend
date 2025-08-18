import pandas as pd
import sqlite3
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import json
import pandas_ta as ta
from datetime import datetime, timezone, time
import pytz

# --- Configuration ---
DATABASE_FILE = 'data/trading_data.db'
CACHE = {}
FOREX_DST_PROFILE_MAP = {}  # NEW: Forex-specific DST profile map
METALS_INDICES_DST_PROFILE_MAP = {}  # NEW: Metals/Indices-specific DST profile map
LOG = logging.getLogger("uvicorn")

# --- NEW: Market-Specific DST Profile Calculation and Caching ---
def get_forex_market_open_profile(date_obj: datetime.date) -> str:
    """
    Determines if a given date has a 21:00 or 22:00 UTC market open for FOREX based on US DST.
    """
    ny_tz = pytz.timezone('America/New_York')
    # Check DST status at 6 PM New York time for the given date.
    market_time_ny = ny_tz.localize(datetime.combine(date_obj, time(18, 0)))
    # If the UTC offset's total seconds is -14400 (UTC-4), it's DST (21:00 open). Otherwise, it's standard time (22:00 open).
    return 'FOREX_PROFILE_2100' if market_time_ny.utcoffset().total_seconds() == -14400 else 'FOREX_PROFILE_2200'

def get_metals_indices_market_open_profile(date_obj: datetime.date) -> str:
    """
    Determines if a given date has a 22:00 or 23:00 UTC market open for METALS/INDICES based on US DST.
    """
    ny_tz = pytz.timezone('America/New_York')
    # Check DST status at 6 PM New York time for the given date.
    market_time_ny = ny_tz.localize(datetime.combine(date_obj, time(18, 0)))
    # If the UTC offset's total seconds is -14400 (UTC-4), it's DST (22:00 open). Otherwise, it's standard time (23:00 open).
    return 'METALS_INDICES_PROFILE_2200' if market_time_ny.utcoffset().total_seconds() == -14400 else 'METALS_INDICES_PROFILE_2300'

def build_dst_profile_maps():
    """
    Pre-calculates and caches the DST profiles for every day in the dataset at startup.
    """
    global FOREX_DST_PROFILE_MAP, METALS_INDICES_DST_PROFILE_MAP
    LOG.info("Building market-specific DST profile maps for the entire dataset...")
    try:
        # Use the pre-loaded 1m XAUUSD data as the reference for the date range
        reference_df = CACHE.get(format_instrument_to_tablename('XAUUSD', '1m'))
        if reference_df is None or reference_df.empty:
            LOG.error("Reference data not found in cache for DST profile building. Aborting.")
            return

        all_dates = pd.to_datetime(reference_df.index.date).unique()
        LOG.info(f"Calculating DST profiles for {len(all_dates)} unique dates...")
        
        for date in all_dates:
            date_str = date.strftime('%Y-%m-%d')
            FOREX_DST_PROFILE_MAP[date_str] = get_forex_market_open_profile(date.date())
            METALS_INDICES_DST_PROFILE_MAP[date_str] = get_metals_indices_market_open_profile(date.date())
        
        LOG.info(f"DST profile maps built successfully. Forex profiles: {len(FOREX_DST_PROFILE_MAP)}, Metals/Indices profiles: {len(METALS_INDICES_DST_PROFILE_MAP)}")
    except Exception as e:
        LOG.error(f"Failed to build DST profile maps: {e}", exc_info=True)
        FOREX_DST_PROFILE_MAP = {}
        METALS_INDICES_DST_PROFILE_MAP = {}

def get_forex_profile_for_date_str(date_str: str) -> str:
    """Fast lookup of a date's Forex DST profile from the pre-calculated map."""
    return FOREX_DST_PROFILE_MAP.get(date_str, 'FOREX_PROFILE_2100')  # Default to 21:00 if not found

def get_metals_indices_profile_for_date_str(date_str: str) -> str:
    """Fast lookup of a date's Metals/Indices DST profile from the pre-calculated map."""
    return METALS_INDICES_DST_PROFILE_MAP.get(date_str, 'METALS_INDICES_PROFILE_2200')  # Default to 22:00 if not found

def get_market_open_hour(date, instrument):
    """
    Get market open hour for a specific date and instrument using market-specific DST profiles.
    """
    if isinstance(date, pd.Timestamp):
        date_str = date.strftime('%Y-%m-%d')
    else:
        date_str = str(date)[:10]  # Handle string dates
    
    market_type = get_market_type(instrument)
    
    if market_type == 'FOREX':
        profile = get_forex_profile_for_date_str(date_str)
        return 21 if profile == 'FOREX_PROFILE_2100' else 22
    elif market_type == 'METALS_INDICES':
        profile = get_metals_indices_profile_for_date_str(date_str)
        return 22 if profile == 'METALS_INDICES_PROFILE_2200' else 23
    else:  # CRYPTO
        return 0  # No specific market open for crypto

def find_dst_transitions(start_date, end_date, instrument):
    """
    Find all DST transition points within a date range using the pre-calculated DST profile maps.
    Returns list of transition dates where market open hour changes for the specific instrument type.
    """
    transitions = []
    current_date = pd.to_datetime(start_date).normalize()
    end_date = pd.to_datetime(end_date).normalize()
    
    if current_date >= end_date:
        return transitions
    
    # Get the market open hour for start date
    current_open_hour = get_market_open_hour(current_date, instrument)
    
    # Check each day for changes
    check_date = current_date + pd.Timedelta(days=1)
    while check_date <= end_date:
        new_open_hour = get_market_open_hour(check_date, instrument)
        if new_open_hour != current_open_hour:
            transitions.append({
                'date': check_date,
                'from_hour': current_open_hour,
                'to_hour': new_open_hour
            })
            current_open_hour = new_open_hour
        check_date += pd.Timedelta(days=1)
    
    return transitions

def get_dst_aware_anchor(rule, instrument, market_open_hour):
    """
    Get resampling anchor based on market open hour (DST-aware) for different market types.
    """
    timeframe_upper = rule.upper()
    market_type = get_market_type(instrument)
    
    # For crypto - use no offset
    if market_type == 'CRYPTO':
        return 'CRYPTO_NO_OFFSET'
    
    # For forex - DST-aware logic (21:00/22:00 UTC)
    elif market_type == 'FOREX':
        # For daily and above timeframes
        if is_daily_or_above(rule):
            if 'D' in timeframe_upper:
                return 'FOREX_D_MARKET_SPECIFIC'
            elif 'W' in timeframe_upper:
                return 'FOREX_W_MARKET_SPECIFIC'
            else:  # Monthly
                return 'FOREX_MO_MARKET_SPECIFIC'
        
        # For hour-based timeframes - DST-aware even/odd logic
        elif 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                return 'H_NO_OFFSET'  # 1H doesn't need offset
            elif hours % 2 == 0:  # Even hours
                if market_open_hour == 21:
                    # 21:00 UTC open: Even hours get offset (same as metals @ 22:00)
                    return 'H_EVEN_OFFSET'
                else:  # market_open_hour == 22
                    # 22:00 UTC open: Even hours get 2H offset (same as metals @ 23:00)
                    return 'H_EVEN_2H_OFFSET'
            else:  # Odd hours
                if market_open_hour == 21:
                    # 21:00 UTC open: Odd hours get NO offset (same as metals @ 22:00)
                    return 'H_ODD_NO_OFFSET'
                else:  # market_open_hour == 22
                    # 22:00 UTC open: Odd hours get 1H offset (same as metals @ 23:00)
                    return 'H_ODD_1H_OFFSET'
        
        # For minute timeframes (standard resampling)
        else:
            return None
    
    # For metals/indices - DST-aware logic (22:00/23:00 UTC)
    elif market_type == 'METALS_INDICES':
        # For daily and above timeframes
        if is_daily_or_above(rule):
            if 'D' in timeframe_upper:
                return 'METALS_INDICES_D_MARKET_SPECIFIC'
            elif 'W' in timeframe_upper:
                return 'METALS_INDICES_W_MARKET_SPECIFIC'
            else:  # Monthly
                return 'METALS_INDICES_MO_MARKET_SPECIFIC'
        
        # For hour-based timeframes - DST-aware even/odd logic
        elif 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                return 'H_NO_OFFSET'  # 1H doesn't need offset
            elif hours % 2 == 0:  # Even hours
                if market_open_hour == 22:
                    # 22:00 UTC open: Even hours get offset
                    return 'H_EVEN_OFFSET'
                else:  # market_open_hour == 23
                    # 23:00 UTC open: Even hours get 2H offset
                    return 'H_EVEN_2H_OFFSET'
            else:  # Odd hours
                if market_open_hour == 22:
                    # 22:00 UTC open: Odd hours get NO offset
                    return 'H_ODD_NO_OFFSET'
                else:  # market_open_hour == 23
                    # 23:00 UTC open: Odd hours get 1H offset
                    return 'H_ODD_1H_OFFSET'
        
        # For minute timeframes (standard resampling)
        else:
            return None
    
    # Default fallback
    return None

def resample_data_chunk(df, rule, instrument, market_open_hour):
    """
    Resample a single chunk of data with consistent market open hour.
    This is the core resampling function for individual chunks.
    """
    resampling_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    if 'Volume' in df.columns:
        resampling_rules['Volume'] = 'sum'
    
    pandas_rule = get_pandas_timeframe(rule)
    anchor = get_dst_aware_anchor(rule, instrument, market_open_hour)
    
    # Ensure the DataFrame index is timezone-aware UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    elif df.index.tz != 'UTC':
        df.index = df.index.tz_convert('UTC')
    
    # Use the existing resampling logic with DST-aware anchor
    if anchor == 'CRYPTO_NO_OFFSET':
        # Crypto - simple resampling with no offset
        resampled_df = df.resample(
            pandas_rule,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_EVEN_OFFSET':
        # Even hour timeframes - use TradingView alignment with offset
        resampled_df = df.resample(
            pandas_rule, 
            origin='start_day',
            offset='1H',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_EVEN_2H_OFFSET':
        # Even hour timeframes (23:00/22:00 UTC open) - use 2H offset
        resampled_df = df.resample(
            pandas_rule, 
            origin='start_day',
            offset='2H',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_ODD_NO_OFFSET':
        # Odd hour timeframes - use actual timestamp alignment (no offset)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_ODD_1H_OFFSET':
        # Odd hour timeframes (23:00/22:00 UTC open) - use 1H offset
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset='1H',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_NO_OFFSET':
        # 1H timeframe - no offset needed
        resampled_df = df.resample(
            pandas_rule,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_D_MARKET_SPECIFIC', 'METALS_INDICES_D_MARKET_SPECIFIC']:
        # Daily timeframe with market-specific offset
        daily_offset = f'{market_open_hour}H'
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=daily_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_W_MARKET_SPECIFIC', 'METALS_INDICES_W_MARKET_SPECIFIC']:
        # Weekly timeframe with market-specific offset
        weekly_offset = f'{market_open_hour}H'
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=weekly_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_MO_MARKET_SPECIFIC', 'METALS_INDICES_MO_MARKET_SPECIFIC']:
        # Monthly timeframe with market-specific offset
        monthly_offset = f'{market_open_hour}H'
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=monthly_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    else:
        # Minute-based timeframes (standard resampling)
        resampled_df = df.resample(
            pandas_rule, 
            label='left', 
            closed='left'
        ).apply(resampling_rules).dropna()
    
    return resampled_df

def resample_data_dst_aware(df, rule, instrument):
    """
    Main DST-aware resampling function that handles chunk-based processing.
    Uses the pre-calculated market-specific DST profile maps for efficient processing.
    """
    if df is None or df.empty:
        return df
    
    # For crypto, use simple resampling
    market_type = get_market_type(instrument)
    if market_type == 'CRYPTO':
        LOG.info(f"Using simple resampling for crypto instrument: {instrument}")
        return resample_crypto_data(df, rule, instrument)
    
    LOG.info(f"Starting DST-aware resampling for {instrument} @ {rule} (market: {market_type})")
    
    # Get date range of the data
    start_date = df.index.min()
    end_date = df.index.max()
    
    LOG.info(f"Data range: {start_date} to {end_date}")
    
    # Find DST transition points using market-specific pre-calculated map
    transitions = find_dst_transitions(start_date, end_date, instrument)
    
    # If no transitions, resample as single chunk
    if not transitions:
        market_open_hour = get_market_open_hour(start_date, instrument)
        LOG.info(f"No DST transitions found. Using single chunk with market open hour: {market_open_hour}:00 UTC for {market_type}")
        result = resample_data_chunk(df, rule, instrument, market_open_hour)
        LOG.info(f"Single chunk resampling complete: {len(result)} candles")
        return result
    
    # Split data into chunks at transition points
    chunks = []
    chunk_start = start_date
    
    LOG.info(f"Found {len(transitions)} DST transitions for {market_type}. Processing chunks...")
    
    for i, transition in enumerate(transitions):
        # Create chunk from current start to transition date
        chunk_end = transition['date']
        chunk_df = df[chunk_start:chunk_end - pd.Timedelta(seconds=1)]
        
        if not chunk_df.empty:
            market_open_hour = transition['from_hour']
            LOG.info(f"Processing chunk {i+1}: {chunk_start} to {chunk_end} (market open: {market_open_hour}:00 UTC)")
            resampled_chunk = resample_data_chunk(chunk_df, rule, instrument, market_open_hour)
            if not resampled_chunk.empty:
                chunks.append(resampled_chunk)
                LOG.info(f"Chunk {i+1} complete: {len(resampled_chunk)} candles")
        
        # Update start for next chunk
        chunk_start = chunk_end
    
    # Process final chunk (from last transition to end)
    final_chunk_df = df[chunk_start:end_date]
    if not final_chunk_df.empty:
        market_open_hour = get_market_open_hour(chunk_start, instrument)
        LOG.info(f"Processing final chunk: {chunk_start} to {end_date} (market open: {market_open_hour}:00 UTC)")
        final_resampled = resample_data_chunk(final_chunk_df, rule, instrument, market_open_hour)
        if not final_resampled.empty:
            chunks.append(final_resampled)
            LOG.info(f"Final chunk complete: {len(final_resampled)} candles")
    
    # Combine all chunks
    if chunks:
        combined_df = pd.concat(chunks, axis=0).sort_index()
        
        # CRITICAL: Remove duplicate timestamps and log them
        before_dedup = len(combined_df)
        duplicate_mask = combined_df.index.duplicated(keep='first')
        duplicate_timestamps = combined_df[duplicate_mask].index.tolist()
        
        if len(duplicate_timestamps) > 0:
            LOG.warning(f"Found {len(duplicate_timestamps)} duplicate timestamps during chunk combination:")
            for dup_ts in duplicate_timestamps[:5]:  # Log first 5 duplicates
                LOG.warning(f"  Duplicate timestamp: {dup_ts}")
        
        combined_df = combined_df[~duplicate_mask]
        after_dedup = len(combined_df)
        
        LOG.info(f"Combined {len(chunks)} chunks: {before_dedup} -> {after_dedup} candles (removed {before_dedup - after_dedup} duplicates)")
        return combined_df
    else:
        LOG.warning("No valid chunks found after DST-aware resampling")
        return pd.DataFrame()

def resample_crypto_data(df, rule, instrument):
    """
    Simple resampling for crypto instruments with no offsets.
    """
    resampling_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
    if 'Volume' in df.columns:
        resampling_rules['Volume'] = 'sum'
    
    pandas_rule = get_pandas_timeframe(rule)
    
    # Ensure the DataFrame index is timezone-aware UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    elif df.index.tz != 'UTC':
        df.index = df.index.tz_convert('UTC')
    
    # Simple resampling with no offset for crypto
    resampled_df = df.resample(
        pandas_rule,
        label='left',
        closed='left'
    ).apply(resampling_rules).dropna()
    
    return resampled_df

def resample_weekly_monthly_via_daily(df, rule, instrument):
    """
    Resample weekly and monthly timeframes by first creating daily candles,
    then grouping daily candles together with proper market-specific handling.
    FIXED: Proper Monday-anchored weekly resampling and correct timestamp labeling using actual daily candle timestamps.
    """
    LOG.info(f"Using daily-based resampling for {instrument} @ {rule}")
    
    # Step 1: Create daily candles from 1m data
    market_type = get_market_type(instrument)
    if market_type == 'CRYPTO':
        daily_df = resample_crypto_data(df.copy(), '1D', instrument)
    else:
        daily_df = resample_data_dst_aware(df.copy(), '1D', instrument)
    
    if daily_df.empty:
        LOG.warning("No daily data generated for weekly/monthly resampling")
        return pd.DataFrame()
    
    LOG.info(f"Created {len(daily_df)} daily candles for {rule} resampling")
    
    # Step 2: Manual grouping with proper timestamp handling
    resampled_data = []
    
    if 'W' in rule.upper():
        # Weekly resampling - group by Monday-starting weeks
        LOG.info(f"Processing weekly resampling for {market_type}")
        
        if market_type == 'CRYPTO':
            # Crypto: Use all daily candles (Mon-Sun)
            working_df = daily_df.copy()
        else:
            # Forex/Metals/Indices: Filter to weekdays only (Mon-Fri)
            working_df = daily_df[daily_df.index.weekday.isin([0, 1, 2, 3, 4, 6])]  # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sun=6
            LOG.info(f"Filtered to {len(working_df)} weekday candles")
            LOG.info(f"Days in working_df for weekly: {[d.strftime('%A %Y-%m-%d') for d in working_df.index[:10]]}")
        
        if working_df.empty:
            return pd.DataFrame()
        
        # Group by week starting Monday
        current_week_start = None
        current_week_data = []
        
        for idx, row in working_df.iterrows():
            # FIXED INDENTATION: Both market types handled at same level
            if market_type != 'CRYPTO':
                days_since_sunday = (idx.weekday() + 1) % 7  # Sun=0, Mon=1, ..., Sat=6
                sunday_of_week = idx - pd.Timedelta(days=days_since_sunday)
                week_start = sunday_of_week.normalize()
            else:
                days_since_monday = idx.weekday()  # Mon=0, Sun=6
                week_start = (idx - pd.Timedelta(days=days_since_monday)).normalize()

            LOG.info(f"Processing daily candle {idx.strftime('%A %Y-%m-%d')}: week_start={week_start.strftime('%Y-%m-%d')}, current_week_start={current_week_start.strftime('%Y-%m-%d') if current_week_start else 'None'}")

            # If this is a new week, process the previous week
            if current_week_start is not None and week_start != current_week_start:
                if len(current_week_data) > 0:
                    # Create weekly candle from daily candles
                    week_open = current_week_data[0]['Open']
                    week_high = max([d['High'] for d in current_week_data])
                    week_low = min([d['Low'] for d in current_week_data])
                    week_close = current_week_data[-1]['Close']
                    week_volume = sum([d.get('Volume', 0) for d in current_week_data])
                    
                    weekly_timestamp = current_week_start + pd.Timedelta(days=1) if market_type != 'CRYPTO' else current_week_start

                    weekly_candle = {
                        'timestamp': weekly_timestamp,  # Use Monday timestamp
                        'Open': week_open,
                        'High': week_high,
                        'Low': week_low,
                        'Close': week_close
                    }
                    if 'Volume' in daily_df.columns:
                        weekly_candle['Volume'] = week_volume
                    
                    resampled_data.append(weekly_candle)
                    LOG.info(f"Created weekly candle: {current_week_start} with {len(current_week_data)} daily candles")
                
                # Start new week
                current_week_data = []
            
            # Set/update current week start
            current_week_start = week_start
            
            # Add this daily candle to current week
            daily_candle = {'Open': row['Open'], 'High': row['High'], 'Low': row['Low'], 'Close': row['Close']}
            if 'Volume' in row:
                daily_candle['Volume'] = row['Volume']
            current_week_data.append(daily_candle)
        
        # Process the last week
        if len(current_week_data) > 0:
            week_open = current_week_data[0]['Open']
            week_high = max([d['High'] for d in current_week_data])
            week_low = min([d['Low'] for d in current_week_data])
            week_close = current_week_data[-1]['Close']
            week_volume = sum([d.get('Volume', 0) for d in current_week_data])
            
            weekly_timestamp = current_week_start + pd.Timedelta(days=1) if market_type != 'CRYPTO' else current_week_start

            weekly_candle = {
                'timestamp': weekly_timestamp,
                'Open': week_open,
                'High': week_high,
                'Low': week_low,
                'Close': week_close
            }
            if 'Volume' in daily_df.columns:
                weekly_candle['Volume'] = week_volume
            
            resampled_data.append(weekly_candle)
            LOG.info(f"Created final weekly candle: {current_week_start} with {len(current_week_data)} daily candles")
    
    else:
        # Monthly resampling - group by date ranges (market-aware)
        LOG.info("Processing monthly resampling with date ranges")
        
        current_month_range = None
        current_month_data = []
        
        for idx, row in daily_df.iterrows():
            # Get the actual date (not timestamp) to determine which month this daily candle represents
            candle_date = (idx + pd.Timedelta(days=1)).date()  # Add 1 day to get the actual trading day
            
            # Calculate month range for this candle's actual trading date
            actual_month_year = (candle_date.year, candle_date.month)
            
            # Calculate date range for this month
            # Start: last date of previous month  
            # End: 2nd to last date of current month
            if candle_date.month == 1:  # January
                prev_month_last_date = (candle_date.replace(year=candle_date.year-1, month=12, day=31)).strftime('%Y-%m-%d')
            else:
                prev_month = candle_date.month - 1
                prev_year = candle_date.year
                if prev_month == 2:  # February
                    if prev_year % 4 == 0 and (prev_year % 100 != 0 or prev_year % 400 == 0):
                        last_day = 29
                    else:
                        last_day = 28
                elif prev_month in [4, 6, 9, 11]:
                    last_day = 30
                else:
                    last_day = 31
                prev_month_last_date = f"{prev_year:04d}-{prev_month:02d}-{last_day:02d}"
            
            # Current month 2nd to last date
            if candle_date.month == 2:  # February
                if candle_date.year % 4 == 0 and (candle_date.year % 100 != 0 or candle_date.year % 400 == 0):
                    second_last_day = 28
                else:
                    second_last_day = 27
            elif candle_date.month in [4, 6, 9, 11]:
                second_last_day = 29
            else:
                second_last_day = 30
            current_month_second_last_date = f"{candle_date.year:04d}-{candle_date.month:02d}-{second_last_day:02d}"
            
            month_range = (prev_month_last_date, current_month_second_last_date)
            
            # If this is a new month range, process the previous month
            if current_month_range is not None and month_range != current_month_range:
                if len(current_month_data) > 0:
                    # Create monthly candle from daily candles
                    month_open = current_month_data[0]['Open']
                    month_high = max([d['High'] for d in current_month_data])
                    month_low = min([d['Low'] for d in current_month_data])
                    month_close = current_month_data[-1]['Close']
                    month_volume = sum([d.get('Volume', 0) for d in current_month_data])
                    
                    # Calculate display timestamp: first trading day of actual month + market open hour
                    # Determine which calendar month we're representing
                    sample_candle_date = (current_month_data[0]['timestamp'] + pd.Timedelta(days=1)).date()
                    actual_month_year = sample_candle_date.year
                    actual_month = sample_candle_date.month
                    
                    # Find first day of this month
                    first_day_of_month = pd.Timestamp(year=actual_month_year, month=actual_month, day=1, tz='UTC')
                    
                    # Find first trading day based on market type
                    market_type = get_market_type(instrument)
                    if market_type == 'CRYPTO':
                        # Crypto: 24/7, use first day of month at 00:00 UTC
                        first_trading_day = first_day_of_month
                        monthly_timestamp = first_trading_day
                    else:
                        # Forex/Metals/Indices: 24/5 Mon-Fri, find first weekday
                        first_trading_day = first_day_of_month
                        while first_trading_day.weekday() > 4:  # 0=Mon, 1=Tue, ..., 4=Fri, 5=Sat, 6=Sun
                            first_trading_day += pd.Timedelta(days=1)
                        
                        # Add market open hour (DST-aware) and subtract 1 day for display
                        market_open_hour = get_market_open_hour(first_trading_day, instrument)
                        monthly_timestamp = first_trading_day + pd.Timedelta(hours=market_open_hour) - pd.Timedelta(days=1)
                    
                    monthly_candle = {
                        'timestamp': monthly_timestamp,
                        'Open': month_open,
                        'High': month_high,
                        'Low': month_low,
                        'Close': month_close
                    }
                    if 'Volume' in daily_df.columns:
                        monthly_candle['Volume'] = month_volume
                    
                    resampled_data.append(monthly_candle)
                    LOG.info(f"Created monthly candle: {monthly_timestamp} with {len(current_month_data)} daily candles (range: {current_month_range})")
                
                # Start new month
                current_month_data = []
            
            # Set/update current month range
            current_month_range = month_range
            
            # Add this daily candle to current month (with timestamp for later use)
            daily_candle = {
                'timestamp': idx,
                'Open': row['Open'], 
                'High': row['High'], 
                'Low': row['Low'], 
                'Close': row['Close']
            }
            if 'Volume' in row:
                daily_candle['Volume'] = row['Volume']
            current_month_data.append(daily_candle)
        
        # Process the last month
        if len(current_month_data) > 0:
            month_open = current_month_data[0]['Open']
            month_high = max([d['High'] for d in current_month_data])
            month_low = min([d['Low'] for d in current_month_data])
            month_close = current_month_data[-1]['Close']
            month_volume = sum([d.get('Volume', 0) for d in current_month_data])
            
            # Calculate display timestamp: first trading day of actual month + market open hour
            # Determine which calendar month we're representing
            sample_candle_date = (current_month_data[0]['timestamp'] + pd.Timedelta(days=1)).date()
            actual_month_year = sample_candle_date.year
            actual_month = sample_candle_date.month
            
            # Find first day of this month
            first_day_of_month = pd.Timestamp(year=actual_month_year, month=actual_month, day=1, tz='UTC')
            
            # Find first trading day based on market type
            market_type = get_market_type(instrument)
            if market_type == 'CRYPTO':
                # Crypto: 24/7, use first day of month at 00:00 UTC
                first_trading_day = first_day_of_month
                monthly_timestamp = first_trading_day
            else:
                # Forex/Metals/Indices: 24/5 Mon-Fri, find first weekday
                first_trading_day = first_day_of_month
                while first_trading_day.weekday() > 4:  # 0=Mon, 1=Tue, ..., 4=Fri, 5=Sat, 6=Sun
                    first_trading_day += pd.Timedelta(days=1)
                
                # Add market open hour (DST-aware) and subtract 1 day for display
                market_open_hour = get_market_open_hour(first_trading_day, instrument)
                monthly_timestamp = first_trading_day + pd.Timedelta(hours=market_open_hour) - pd.Timedelta(days=1)
            
            monthly_candle = {
                'timestamp': monthly_timestamp,
                'Open': month_open,
                'High': month_high,
                'Low': month_low,
                'Close': month_close
            }
            if 'Volume' in daily_df.columns:
                monthly_candle['Volume'] = month_volume
            
            resampled_data.append(monthly_candle)
            LOG.info(f"Created final monthly candle: {monthly_timestamp} with {len(current_month_data)} daily candles (range: {current_month_range})")
    
    # Convert to DataFrame
    if not resampled_data:
        LOG.warning("No resampled data created")
        return pd.DataFrame()
    
    final_df = pd.DataFrame(resampled_data)
    final_df.set_index('timestamp', inplace=True)
    final_df.index.name = 'DateTime'
    
    # Ensure timezone-aware index
    if final_df.index.tz is None:
        final_df.index = final_df.index.tz_localize('UTC')
    
    LOG.info(f"Manual {rule} resampling complete: {len(daily_df)} daily â†’ {len(final_df)} {rule} candles")
    return final_df

# --- Keep ALL existing helper functions with modifications ---

def format_instrument_to_tablename(instrument: str, timeframe: str) -> str:
    """
    Formats the instrument name from the frontend to the correct table name.
    Handles both underscore and non-underscore formats.
    """
    # If it already has an underscore, assume it's in the correct format
    if '_' in instrument:
        return f"{instrument}_{timeframe}"

    # Handle specific known formats first
    if instrument == "BTCUSD":
        return f"BTC_USDT_{timeframe}"
    if instrument == "XAUUSD":
        return f"XAU_USD_{timeframe}"
    if instrument == "XAGUSD":
        return f"XAG_USD_{timeframe}"
    if instrument == "NAS100USD":
        return f"NAS100_USD_{timeframe}"
    if instrument == "US30USD":
        return f"US30_USD_{timeframe}"
    if instrument == "SPX500USD":
        return f"SPX500_USD_{timeframe}"
    if instrument == "UK100GBP":
        return f"UK100_GBP_{timeframe}"
    
    # General rule for 6-character forex pairs (e.g., EURUSD)
    if len(instrument) == 6:
        return f"{instrument[:3]}_{instrument[3:]}_{timeframe}"
        
    # Fallback for any other cases
    return f"{instrument}_{timeframe}"

def get_market_type(instrument: str) -> str:
    """
    Determines the market type based on instrument.
    UPDATED: Now returns FOREX, METALS_INDICES, or CRYPTO
    """
    instrument_upper = instrument.upper()
    
    # Crypto - CRYPTO (separate category)
    if any(crypto in instrument_upper for crypto in ['BTC', 'ETH', 'LTC', 'XRP']):
        return 'CRYPTO'
    
    # Metals - METALS_INDICES (grouped with indices)
    if any(metal in instrument_upper for metal in ['XAU', 'XAG', 'GOLD', 'SILVER']):
        return 'METALS_INDICES'
    
    # Indices - METALS_INDICES (grouped with metals)
    if any(index in instrument_upper for index in ['NAS100', 'US30', 'SPX500', 'UK100', 'DOW', 'SP500', 'NASDAQ']):
        return 'METALS_INDICES'
    
    # Everything else (6-char pairs like EURUSD, GBPUSD, etc.) - FOREX
    return 'FOREX'

def get_daily_offset(instrument: str, market_open_hour: int = None) -> str:
    """
    Returns the daily offset based on market type and DST-aware market open hour.
    UPDATED: Now handles three market categories
    """
    market_type = get_market_type(instrument)
    
    if market_type == 'FOREX':
        if market_open_hour is not None:
            return f'{market_open_hour}H'  # DST-aware: 21H or 22H
        else:
            return '21H'  # Fallback to 21H for forex
    elif market_type == 'METALS_INDICES':
        if market_open_hour is not None:
            return f'{market_open_hour}H'  # DST-aware: 22H or 23H
        else:
            return '22H'  # Fallback to 22H for metals/indices
    else:  # CRYPTO
        return '0H'  # No offset for crypto

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

def is_weekly_or_monthly(timeframe: str) -> bool:
    """
    Check if timeframe is weekly or monthly (W, MO)
    """
    timeframe_upper = timeframe.upper()
    return 'W' in timeframe_upper or 'MO' in timeframe_upper

def is_2d_or_above(timeframe: str) -> bool:
    """
    Check if timeframe is 2D or above (2D, 3D, W, MO)
    """
    timeframe_upper = timeframe.upper()
    if 'D' in timeframe_upper:
        try:
            days = int(timeframe_upper.replace('D', ''))
            return days >= 2
        except ValueError:
            return False
    return 'W' in timeframe_upper or 'MO' in timeframe_upper

def get_resample_anchor(timeframe: str, instrument: str = None) -> str:
    """
    Returns the proper anchor for resampling based on timeframe and instrument.
    UPDATED: Now handles three market categories
    """
    timeframe_upper = timeframe.upper()
    market_type = get_market_type(instrument) if instrument else 'FOREX'
    
    # For crypto - use no offset
    if market_type == 'CRYPTO':
        return 'CRYPTO_NO_OFFSET'
    
    # For forex - apply DST-aware logic (21:00/22:00 UTC)
    elif market_type == 'FOREX':
        # For daily and above timeframes
        if is_daily_or_above(timeframe):
            if 'D' in timeframe_upper:
                return 'FOREX_D_MARKET_SPECIFIC'
            elif 'W' in timeframe_upper:
                return 'FOREX_W_MARKET_SPECIFIC'
            else:  # Monthly
                return 'FOREX_MO_MARKET_SPECIFIC'
        
        # For hour-based timeframes (even/odd logic)
        elif 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                return 'H_NO_OFFSET'  # 1H doesn't need offset
            elif hours % 2 == 0:  # Even hours
                return 'H_EVEN_OFFSET'  # Use TradingView alignment
            else:  # Odd hours
                return 'H_ODD_NO_OFFSET'  # Use actual timestamp alignment
        
        # For minute timeframes (standard resampling)
        else:
            return None
    
    # For metals/indices - apply DST-aware logic (22:00/23:00 UTC)
    elif market_type == 'METALS_INDICES':
        # For daily and above timeframes
        if is_daily_or_above(timeframe):
            if 'D' in timeframe_upper:
                return 'METALS_INDICES_D_MARKET_SPECIFIC'
            elif 'W' in timeframe_upper:
                return 'METALS_INDICES_W_MARKET_SPECIFIC'
            else:  # Monthly
                return 'METALS_INDICES_MO_MARKET_SPECIFIC'
        
        # For hour-based timeframes (even/odd logic)
        elif 'H' in timeframe_upper:
            hours = int(timeframe_upper.replace('H', ''))
            if hours == 1:
                return 'H_NO_OFFSET'  # 1H doesn't need offset
            elif hours % 2 == 0:  # Even hours
                return 'H_EVEN_OFFSET'  # Use TradingView alignment
            else:  # Odd hours
                return 'H_ODD_NO_OFFSET'  # Use actual timestamp alignment
        
        # For minute timeframes (standard resampling)
        else:
            return None
    
    # Default fallback
    return None

def resample_data(df, rule, instrument=None):
    """
    ORIGINAL resampling function - kept for backward compatibility.
    UPDATED: Now handles three market categories
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
    if anchor == 'CRYPTO_NO_OFFSET':
        # Simple resampling for crypto with no offset
        resampled_df = df.resample(
            pandas_rule,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_EVEN_OFFSET':
        # Even hour timeframes - use TradingView alignment
        resampled_df = df.resample(
            pandas_rule, 
            origin='start_day',
            offset='1H',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_ODD_NO_OFFSET':
        # Odd hour timeframes - use actual timestamp alignment
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor == 'H_NO_OFFSET':
        # 1H timeframe - no offset needed
        resampled_df = df.resample(
            pandas_rule,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_D_MARKET_SPECIFIC', 'METALS_INDICES_D_MARKET_SPECIFIC']:
        # Daily timeframe with market-specific offset
        market_open_hour = get_market_open_hour(df.index[0], instrument) if not df.empty else (21 if get_market_type(instrument) == 'FOREX' else 22)
        daily_offset = get_daily_offset(instrument, market_open_hour)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=daily_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_W_MARKET_SPECIFIC', 'METALS_INDICES_W_MARKET_SPECIFIC']:
        # Weekly timeframe with market-specific offset
        market_open_hour = get_market_open_hour(df.index[0], instrument) if not df.empty else (21 if get_market_type(instrument) == 'FOREX' else 22)
        weekly_offset = get_daily_offset(instrument, market_open_hour)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=weekly_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    elif anchor in ['FOREX_MO_MARKET_SPECIFIC', 'METALS_INDICES_MO_MARKET_SPECIFIC']:
        # Monthly timeframe with market-specific offset
        market_open_hour = get_market_open_hour(df.index[0], instrument) if not df.empty else (21 if get_market_type(instrument) == 'FOREX' else 22)
        monthly_offset = get_daily_offset(instrument, market_open_hour)
        resampled_df = df.resample(
            pandas_rule,
            origin='start_day',
            offset=monthly_offset,
            label='left',
            closed='left'
        ).apply(resampling_rules).dropna()
    
    # For other timeframes, use standard resampling
    else:
        # Minute-based timeframes - no offset needed
        resampled_df = df.resample(
            pandas_rule, 
            label='left', 
            closed='left'
        ).apply(resampling_rules).dropna()
    
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
    LOG.info("Pre-loading ALL data and building market-specific DST profile maps...")
    
    # Get list of all available instruments
    instruments_data = {
        "forex": [
            "USDCHF", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
            "EURGBP", "EURJPY", "GBPJPY", "AUDCAD"
        ],
        "metals_indices": [
            "XAUUSD", "XAGUSD", "NAS100USD", "US30USD", "SPX500USD", "UK100GBP"
        ],
        "crypto": [
            "BTCUSD"
        ]
    }
    
    # Flatten all instruments into one list
    all_instruments = []
    for category in instruments_data.values():
        all_instruments.extend(category)
    
    # Pre-load all 1m data for all instruments
    LOG.info(f"Pre-loading 1m data for {len(all_instruments)} instruments...")
    loaded_count = 0
    failed_instruments = []
    
    for instrument in all_instruments:
        try:
            table_name = format_instrument_to_tablename(instrument, '1m')
            LOG.info(f"Loading {table_name}...")
            
            df = get_data_from_db(table_name)
            if df is not None and not df.empty:
                loaded_count += 1
                LOG.info(f"✓ Loaded {len(df)} records for {instrument}")
            else:
                failed_instruments.append(instrument)
                LOG.warning(f"✗ No data found for {instrument}")
                
        except Exception as e:
            failed_instruments.append(instrument)
            LOG.error(f"✗ Failed to load {instrument}: {e}")
    
    LOG.info(f"Pre-loading complete: {loaded_count}/{len(all_instruments)} instruments loaded successfully")
    if failed_instruments:
        LOG.warning(f"Failed to load: {failed_instruments}")
    
    # Build the market-specific DST profile maps using XAUUSD as reference
    if 'XAU_USD_1m' in CACHE and not CACHE['XAU_USD_1m'].empty:
        build_dst_profile_maps()
        
        # Log some sample DST profiles for verification
        sample_dates = ['2024-01-15', '2024-03-15', '2024-06-15', '2024-11-15']
        for sample_date in sample_dates:
            forex_profile = get_forex_profile_for_date_str(sample_date)
            forex_hour = 21 if forex_profile == 'FOREX_PROFILE_2100' else 22
            
            metals_profile = get_metals_indices_profile_for_date_str(sample_date)
            metals_hour = 22 if metals_profile == 'METALS_INDICES_PROFILE_2200' else 23
            
            LOG.info(f"Sample DST profiles for {sample_date}:")
            LOG.info(f"  Forex: {forex_profile} ({forex_hour}:00 UTC)")
            LOG.info(f"  Metals/Indices: {metals_profile} ({metals_hour}:00 UTC)")
    else:
        LOG.error("Failed to load XAUUSD data - DST profile maps not built")
    
    LOG.info(f"Application startup complete. Cache contains {len(CACHE)} datasets.")
    
    yield
    
    LOG.info("Application shutdown. Clearing caches...")
    CACHE.clear()
    FOREX_DST_PROFILE_MAP.clear()
    METALS_INDICES_DST_PROFILE_MAP.clear()
    LOG.info("Cache cleared successfully.")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", 
        "http://127.0.0.1:5173", 
        "https://strong-biscuit-0ff55.netlify.app"
    ],
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
        
        # UPDATED: Use appropriate resampling based on market type and timeframe
        market_type = get_market_type(instrument)
        if is_2d_or_above(timeframe):
            # Use daily-based resampling for 2D and above (2D, 3D, W, MO)
            # IMPORTANT: Use FULL dataset for resampling, then filter
            resampled_df = resample_weekly_monthly_via_daily(base_df.copy(), timeframe, instrument)
        elif market_type == 'CRYPTO':
            # Use simple resampling for crypto
            resampled_df = resample_crypto_data(base_df.copy(), timeframe, instrument)
        else:
            # Use DST-aware resampling for forex/metals/indices (sub-daily timeframes)
            resampled_df = resample_data_dst_aware(base_df.copy(), timeframe, instrument)
            
        # Apply limit AFTER resampling
        final_df = resampled_df[resampled_df.index <= target_candle_start_dt].tail(limit)
    else:
        # UPDATED: Use appropriate resampling based on market type and timeframe
        market_type = get_market_type(instrument)
        
        # CRITICAL FIX: Do resampling FIRST with full dataset
        if is_2d_or_above(timeframe):
            # Use daily-based resampling for 2D and above (2D, 3D, W, MO)
            # Pass FULL base_df to resampling function
            resampled_df = resample_weekly_monthly_via_daily(base_df.copy(), timeframe, instrument)
        elif market_type == 'CRYPTO':
            # Use simple resampling for crypto
            resampled_df = resample_crypto_data(base_df.copy(), timeframe, instrument)
        else:
            # Use DST-aware resampling for forex/metals/indices (sub-daily timeframes)
            resampled_df = resample_data_dst_aware(base_df.copy(), timeframe, instrument)
        
        # CRITICAL FIX: Apply filtering and limits AFTER resampling
        if after_date:
            after_datetime = pd.to_datetime(after_date, utc=True)
            final_df = resampled_df[resampled_df.index > after_datetime].head(limit)
        elif end_date:
            end_datetime = pd.to_datetime(end_date, utc=True)
            # FIX applied here: Correctly filter the resampled data first, then apply the tail limit.
            filtered_df = resampled_df[resampled_df.index < end_datetime]
            final_df = filtered_df.tail(limit)
        else:
            # Apply limit AFTER resampling
            final_df = resampled_df.tail(limit)

    if final_df.empty:
         return []

    if timeframe.upper() == '1D':
        debug_df = final_df.copy().reset_index()
        debug_df['DateTime'] = debug_df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        last_20 = debug_df.tail(20).iloc[::-1]

        LOG.info(f"=== DEBUG: Last {len(last_20)} daily candles for {instrument} @ {timeframe} (most recent first) ===")
        for i, row in last_20.iterrows():
            LOG.info(f"Candle: {row['DateTime']} ({pd.to_datetime(row['DateTime']).strftime('%A')}) | O:{row['Open']:.5f} H:{row['High']:.5f} L:{row['Low']:.5f} C:{row['Close']:.5f}")
            LOG.info("=== END DEBUG ===")

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

            # --- UPDATED: Dynamic indicator calculation logic ---
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
    # UPDATED: Organized by market categories
    return {
        "forex": [
            "USDCHF", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
            "EURGBP", "EURJPY", "GBPJPY", "AUDCAD"
        ],
        "metals_indices": [
            "XAUUSD", "XAGUSD", "NAS100USD", "US30USD", "SPX500USD", "UK100GBP"
        ],
        "crypto": [
            "BTCUSD"
        ]
    }

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
