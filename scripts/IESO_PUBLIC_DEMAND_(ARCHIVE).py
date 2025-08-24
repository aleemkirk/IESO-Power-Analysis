#downloads data files from the IESO site
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
import logging

#try getting connection details from airflow variables
try:
    from airflow.models import Variable
except ImportError:
    # This handles cases where the script might be run outside of an Airflow environment
    logging.warning("Airflow's Variable model not found. Using dummy variables.")
    # Define a mock Variable class for local testing
    class Variable:
        @staticmethod
        def get(key, default_var=None):
            return os.environ.get(key, default_var)

# --- Database Configuration (UPDATE THESE VALUES IN AIRFLOW UI) ---
# Access database credentials from Airflow Variables for security.
# These variables must be set in the Airflow UI under Admin -> Variables.
try:
    DB_USER = Variable.get('DB_USER')
    DB_PASSWORD = Variable.get('DB_PASSWORD')
    DB_HOST = Variable.get('HOST')
    DB_PORT = Variable.get('DB_PORT', default_var='5432') # Use default_var for a default value
    DB_NAME = Variable.get('DB')
    logging.info("Database credentials loaded from Airflow Variables.")
except Exception as e:
    logging.error(f"Error accessing Airflow Variables: {e}. Please ensure they are set in the Airflow UI.")
    DB_USER = None
    DB_PASSWORD = None
    DB_HOST = None
    DB_NAME = None

# Define the base URL and the filename
base_url = 'https://reports-public.ieso.ca/public/Demand/'
filename = 'PUB_Demand.csv'
url = f"{base_url}{filename}"
local_filename = filename

# --- Download the file ---
try:
    print(f"Attempting to download {url}...")
    # Make a GET request to the URL.
    # We use stream=True for efficiency, especially for large files.
    response = requests.get(url, stream=True)

    # Raise an exception for bad status codes (4xx or 5xx)
    response.raise_for_status()

    # Open the local file in binary write mode
    with open(local_filename, 'wb') as f:
        # Iterate over the response content in chunks
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # Filter out keep-alive new chunks
                f.write(chunk)

    print(f"Successfully downloaded {local_filename}")

except requests.exceptions.RequestException as e:
    # Print an error message if the download fails
    print(f"Error downloading the file: {e}")

# --- Optional: Read the downloaded CSV into a pandas DataFrame ---
try:
    # Check if the file was successfully created
    if os.path.exists(local_filename):
        # Read the CSV file into a DataFrame
        col_names = ['Date', 'Hour', 'Market Demand', 'Ontario Demand']
        df = pd.read_csv(local_filename, header=None, names=col_names)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%Y-%m-%d')
        df.dropna(inplace=True)
        df['Modified_DT'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        print("\nFirst 5 rows of the downloaded data:")
        print(df)
    else:
        print(f"File {local_filename} was not found, cannot read.")
except Exception as e:
    # Handle any errors that might occur while reading the CSV
    print(f"Error reading the CSV file with pandas: {e}")



#Writing to postgresdb
if DB_USER:
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = None
    logging.warning("DATABASE_URL is not set due to missing Airflow Variables.")

if not DATABASE_URL:
    logging.error("Cannot write to database. Database connection URL is not configured.")

try:
    logging.info("Attempting to connect to the PostgreSQL database...")
    # Create a SQLAlchemy engine
    engine = create_engine(DATABASE_URL)

    table_name = 'DEMAND_DATA'
    # Connect to the database
    with engine.connect() as conn:
        logging.info(f"Connection to database successful. Writing data to table '{table_name}'...")

        # Use pandas to_sql to write the DataFrame to the table.
        # if_exists='replace' will drop the table and create a new one.
        # if_exists='append' will add new rows to the existing table.
        # index=False prevents pandas from writing the DataFrame index as a column.
        df.to_sql(
            name=table_name,
            con=conn,
            schema='00_RAW',
            if_exists='replace',  # or 'append'
            index=False
        )
        logging.info(f"Successfully wrote data to table '{table_name}'.")

except Exception as e:
    logging.error(f"Error writing to PostgreSQL database: {e}")
