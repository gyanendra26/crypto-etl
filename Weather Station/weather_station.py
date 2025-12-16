import pandas as pd
import requests
import json
import logging
import sys
from sqlalchemy import create_engine
from urllib.parse import quote_plus

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)
API_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS={
    "latitude":12.9716,
    "longitude":77.5946,
    "current_weather":"true"
}

def fetch_weather_data():
    """Extract: FEtch data from API..."""
    logging.info("step 1: Exrtacting data from API..")
    try:
        response = requests.get(API_URL, params=PARAMS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API Error: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.critical(f"Network error: {e}")
        return None

def transform_weather_data(raw_data):
    logging.info("Step  2: Transforming data ..")

    current = raw_data['current_weather']

    weather_data= {
        'latitude': raw_data['latitude'],
        'longitude': raw_data ['longitude'],
        'temperature_celsius': current['temperature'],
        'wind_speed': current['windspeed'],
        'timestamp': current['time']
    }

    df = pd.DataFrame([weather_data])

    return df

def load_Weather_data(df):
    
    """Save to SQL (if available) and CSV"""
    logging.info("Step 3: Loading data...")

    # --- PART A: Always save to CSV (For the Cloud Robot) ---
    csv_filename = 'weather_bangalore.csv'
    df.to_csv(csv_filename, index=False)
    logging.info(f"Data saved to {csv_filename}")

    # --- PART B: Try to save to SQL (For Local Dev) ---
    try:
        password = "Shubham@1"
        encoded_pass = quote_plus(password)
        db_connection_str = f'postgresql://postgres:{encoded_pass}@localhost:5432/postgres'
        db_connection = create_engine(db_connection_str)
        
        df.to_sql('weather_bangalore', db_connection, if_exists='append', index=False)
        logging.info("Data successfully loaded into Postgresql!")
        
    except Exception as e:
        # If SQL fails (like on GitHub), we just log it and move on. We don't crash.
        logging.warning(f"Could not save to SQL (This is expected on GitHub): {e}")

    #Main Pipeline
if __name__ == "__main__":

    raw_json = fetch_weather_data()

    if raw_json:
        df =transform_weather_data(raw_json)
        
        logging.info("Data Transformed:")
        print(df)

        load_Weather_data(df)
    else:
        logging.error("pipeline failed. exiting.")
        sys.exit(1)
