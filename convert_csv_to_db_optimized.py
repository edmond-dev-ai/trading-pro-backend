import pandas as pd
import sqlite3
import os
import glob

# --- CONFIGURATION ---
DATA_DIRECTORY = 'data'
DATABASE_FILE_PATH = os.path.join(DATA_DIRECTORY, 'trading_data_optimized.db')

def convert_all_csv_to_sqlite_optimized():
    """
    Scans the data directory for M1 CSV files, and writes each one to a
    corresponding table in the SQLite database with OPTIMIZED storage.
    
    Key optimizations:
    - DateTime stored as INTEGER (Unix timestamp) instead of TEXT
    - Only keeps 1-minute data tables
    - Optimized data types
    """
    # Find all M1 CSV files in the specified directory
    csv_files = glob.glob(os.path.join(DATA_DIRECTORY, '*_M1_from_*.csv'))

    if not csv_files:
        print(f"No M1 CSV files found in the '{DATA_DIRECTORY}' directory.")
        print("Please make sure your CSV files are named like 'INSTRUMENT_M1_from_YEAR.csv'")
        return

    print(f"🚀 Found {len(csv_files)} CSV files to process with OPTIMIZATION.")
    
    # Get original size if old db exists
    old_db = DATABASE_FILE_PATH.replace('_optimized', '')
    if os.path.exists(old_db):
        old_size = os.path.getsize(old_db) / (1024*1024*1024)
        print(f"📊 Old database size: {old_size:.2f} GB")

    try:
        print(f"🔧 Creating optimized database '{DATABASE_FILE_PATH}'...")
        
        # Remove old optimized db if exists
        if os.path.exists(DATABASE_FILE_PATH):
            os.remove(DATABASE_FILE_PATH)
        
        with sqlite3.connect(DATABASE_FILE_PATH) as con:
            # Set SQLite optimizations
            con.execute("PRAGMA journal_mode = WAL")
            con.execute("PRAGMA synchronous = NORMAL") 
            con.execute("PRAGMA cache_size = 10000")
            
            total_rows = 0
            
            for csv_file_path in csv_files:
                try:
                    # --- 1. Determine Table Name from Filename ---
                    filename = os.path.basename(csv_file_path)
                    instrument_name = filename.split('_M1_from_')[0]
                    table_name = f"{instrument_name}_1m"
                    
                    print(f"\n📈 Processing '{filename}'")
                    print(f"   → Target table: '{table_name}'")

                    # --- 2. Read and OPTIMIZE CSV Data ---
                    print(f"   📖 Reading CSV data...")
                    df = pd.read_csv(csv_file_path)
                    
                    # Check required columns
                    if 'DateTime' not in df.columns:
                        print(f"   ⚠️  'DateTime' column not found. Skipping.")
                        continue
                    
                    original_rows = len(df)
                    print(f"   📊 Found {original_rows:,} records")

                    # --- 3. OPTIMIZE THE DATA ---
                    
                    # Convert DateTime to Unix timestamp (INTEGER)
                    print(f"   🔧 Converting DateTime to Unix timestamps...")
                    df['DateTime'] = pd.to_datetime(df['DateTime'])
                    df['timestamp'] = df['DateTime'].astype('int64') // 10**9  # Convert to Unix timestamp
                    
                    # Drop the original DateTime column (save space!)
                    df = df.drop('DateTime', axis=1)
                    
                    # Optimize numeric columns - round to reasonable precision
                    price_columns = ['Open', 'High', 'Low', 'Close']
                    for col in price_columns:
                        if col in df.columns:
                            # Round to 5 decimal places (more than enough for most instruments)
                            df[col] = df[col].round(5)
                    
                    # Reorder columns: timestamp first, then OHLC
                    column_order = ['timestamp'] + [col for col in df.columns if col != 'timestamp']
                    df = df[column_order]
                    
                    print(f"   ✅ Data optimized: {len(df):,} rows ready")

                    # --- 4. Write to SQLite with Optimized Schema ---
                    print(f"   💾 Writing to database...")
                    
                    # Create table with optimized data types
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        timestamp INTEGER PRIMARY KEY,
                        Open REAL,
                        High REAL,
                        Low REAL,
                        Close REAL
                    )
                    """
                    con.execute(create_table_sql)
                    
                    # Insert data efficiently 
                    df.to_sql(name=table_name, con=con, if_exists='replace', index=False, method='multi')
                    
                    total_rows += len(df)
                    print(f"   ✅ Successfully wrote {len(df):,} rows to '{table_name}'")

                except Exception as e:
                    print(f"   ❌ Error processing '{csv_file_path}': {e}")
                    print("   Skipping to next file...")

            # Final optimization
            print(f"\n🗜️  Running final database optimization...")
            con.execute("VACUUM")
            con.execute("PRAGMA optimize")

        # Check final size
        if os.path.exists(DATABASE_FILE_PATH):
            new_size = os.path.getsize(DATABASE_FILE_PATH)
            print(f"\n🎉 OPTIMIZATION COMPLETE!")
            print(f"📊 Final database size: {new_size / (1024*1024):.0f} MB")
            print(f"📊 Total rows processed: {total_rows:,}")
            
            if 'old_size' in locals():
                savings = (old_size * 1024 - new_size/1024/1024) / old_size * 100
                print(f"💰 Space saved: {savings:.1f}% vs old database")
            
            if new_size < 800*1024*1024:  # Less than 800MB
                print(f"🎊 PERFECT! Database is under 800MB - ideal for 1GB servers!")
            elif new_size < 1024*1024*1024:  # Less than 1GB  
                print(f"🎯 Great! Database fits in 1GB RAM servers!")

        print(f"\n💾 Optimized database saved as: {DATABASE_FILE_PATH}")

    except Exception as e:
        print(f"\n❌ Database connection error: {e}")

def compare_databases():
    """
    Compare old vs new database if both exist
    """
    old_db = DATABASE_FILE_PATH.replace('_optimized', '')
    new_db = DATABASE_FILE_PATH
    
    if os.path.exists(old_db) and os.path.exists(new_db):
        old_size = os.path.getsize(old_db)
        new_size = os.path.getsize(new_db)
        
        print(f"\n📊 DATABASE COMPARISON:")
        print(f"   Old database: {old_size / (1024*1024*1024):.2f} GB")
        print(f"   New database: {new_size / (1024*1024):.0f} MB") 
        print(f"   Space saved:  {(old_size - new_size) / (1024*1024*1024):.2f} GB")
        print(f"   Reduction:    {(old_size - new_size) / old_size * 100:.1f}%")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("🚀 Starting OPTIMIZED CSV to SQLite conversion...")
    print("✨ This will create a much smaller, faster database!")
    
    convert_all_csv_to_sqlite_optimized()
    compare_databases()
    
    print(f"\n🎯 Next steps:")
    print(f"   1. Test the new database with your app")
    print(f"   2. Update your main.py to use 'timestamp' instead of 'DateTime'") 
    print(f"   3. Upload the optimized database (should be ~300-500MB)")