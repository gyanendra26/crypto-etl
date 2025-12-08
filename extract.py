import requests
import json
import logging
import sys
import pandas as pd
from datetime import datetime

# 1. SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 2. CONFIGURATION
API_URL = "https://api.coingecko.com/api/v3/simple/price"
PARAMS = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "inr",
    "include_market_cap": "true"
}

def fetch_crypto_data():
    """EXTRACT: Fetches data from API"""
    logging.info("Step 1: Extracting data from API...")
    try:
        response = requests.get(API_URL, params=PARAMS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API Error: {response.status_code}")
            return None
    except Exception as e:
        logging.critical(f"Network error: {e}")
        return None

def transform_data(raw_data):
    """TRANSFORM: Converts JSON into a Pandas DataFrame"""
    logging.info("Step 2: Transforming data...")
    
    clean_data_list = []
    current_time = datetime.now()

    for coin_name, coin_stats in raw_data.items():
        row = {
            'coin': coin_name,
            'price_inr': coin_stats['inr'],
            'market_cap_inr': coin_stats['inr_market_cap'],
            'timestamp': current_time
        }
        clean_data_list.append(row)

    df = pd.DataFrame(clean_data_list)
    return df

def load_data(df):
    """LOAD: Saves the DataFrame to a CSV file"""
    logging.info("Step 3: Loading data to CSV...")
    
    filename = "crypto_prices.csv"
    # index=False removes the row numbers (0, 1) from the file
    df.to_csv(filename, index=False)
    
    logging.info(f"Data successfully saved to {filename}")

# 3. MAIN PIPELINE
if __name__ == "__main__":
    # A. Extract
    raw_json = fetch_crypto_data()
    
    if raw_json:
        # B. Transform
        final_df = transform_data(raw_json)
        print(final_df) # Show us the table
        
        # C. Load (The Missing Piece)
        load_data(final_df)
        
    else:
        logging.error("Pipeline failed.")
        sys.exit(1)