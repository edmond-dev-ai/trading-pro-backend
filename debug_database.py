import sqlite3
import pandas as pd
import os

# Database configuration
DB_FILENAME = "trading_data.db"
DB_PATH = os.path.join("data", DB_FILENAME)

def debug_database():
    """Debug script to check what's actually in your database"""
    
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check what tables exist
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("Available tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Check the XAU_USD_1m table specifically
    table_name = "XAU_USD_1m"
    
    try:
        # Get table info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print(f"\nColumns in {table_name}:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Get total record count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"\nTotal records in {table_name}: {total_count}")
        
        # Get date range
        cursor.execute(f"SELECT MIN(DateTime), MAX(DateTime) FROM {table_name}")
        date_range = cursor.fetchone()
        print(f"Date range: {date_range[0]} to {date_range[1]}")
        
        # Sample some recent data
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY DateTime DESC LIMIT 10")
        recent_data = cursor.fetchall()
        print(f"\nMost recent 10 records:")
        for row in recent_data:
            print(f"  {row}")
        
        # Test the problematic query
        print(f"\nTesting query: DateTime BETWEEN '2020-07-04' AND '2025-07-04'")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE DateTime BETWEEN '2020-07-04' AND '2025-07-04'")
        filtered_count = cursor.fetchone()[0]
        print(f"Records in date range: {filtered_count}")
        
        # Sample some data from the filtered range
        cursor.execute(f"SELECT * FROM {table_name} WHERE DateTime BETWEEN '2020-07-04' AND '2025-07-04' ORDER BY DateTime DESC LIMIT 5")
        filtered_data = cursor.fetchall()
        print(f"Sample filtered data:")
        for row in filtered_data:
            print(f"  {row}")
            
    except Exception as e:
        print(f"Error checking table {table_name}: {e}")
    
    conn.close()

if __name__ == "__main__":
    debug_database()
