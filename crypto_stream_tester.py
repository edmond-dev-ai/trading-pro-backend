import asyncio
from binance import AsyncClient, BinanceSocketManager
import os
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

# API keys are not strictly required for public data streams,
# but it's good practice to include them.
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET')

INSTRUMENT_TO_STREAM = "BTCUSDT"

async def main():
    """
    Connects to the Binance live trade stream and prints each trade to the console.
    """
    print("--- Connecting to Binance Live Trade Stream ---")
    print(f"--- Instrument: {INSTRUMENT_TO_STREAM} ---")
    print("--- Press CTRL+C to stop the script ---")

    # Initialize the client
    client = await AsyncClient.create(BINANCE_API_KEY, BINANCE_API_SECRET)
    
    # Initialize the WebSocket manager
    bsm = BinanceSocketManager(client)

    # ========================================================================
    # THE FIX IS HERE: Changed to the correct method name 'trade_socket'.
    # ========================================================================
    socket = bsm.trade_socket(INSTRUMENT_TO_STREAM)

    # Start listening to the stream
    async with socket as stream:
        while True:
            # Wait for the next message from the stream
            message = await stream.recv()
            
            # The message contains details about the trade
            # 'p' is price, 'q' is quantity, 'T' is timestamp
            price = float(message['p'])
            quantity = float(message['q'])
            trade_time = pd.to_datetime(message['T'], unit='ms')
            
            # Print the formatted trade information
            print(f"Trade Time: {trade_time} | Price: {price:,.2f} | Quantity: {quantity:.6f}")

if __name__ == "__main__":
    # This is required to run the asynchronous code
    try:
        import pandas as pd
        # Run the main asynchronous function
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n--- Stream stopped by user. ---")
        # --- NEW: Cleanly close the client session ---
        # This prevents the "Unclosed client session" message
        asyncio.run(client.close_connection())
    except Exception as e:
        print(f"An error occurred: {e}")

