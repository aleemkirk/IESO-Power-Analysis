# Import the necessary libraries from Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# Define the default arguments for the DAG. These will be inherited by all tasks.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['youremail@example.com'],  # Set your email for alerts
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# --- Python Callable for the Main Task ---
def demand_data_pipeline():
    """
    A single function that orchestrates the entire data pipeline:
    1. Downloads the IESO data file.
    2. Processes the data with pandas.
    3. Writes the cleaned DataFrame to a PostgreSQL database.
    """
    # Set up a logger for this specific task
    logger = logging.getLogger("airflow.task")

    # Access database credentials from Airflow Variables
    try:
        DB_USER = Variable.get('DB_USER')
        DB_PASSWORD = Variable.get('DB_PASSWORD')
        DB_HOST = Variable.get('HOST')
        DB_PORT = Variable.get('DB_PORT', default_var='5432')
        DB_NAME = Variable.get('DB')
        DB_SCHEMA = Variable.get('DB_SCHEMA', default_var='00_RAW')
        logger.info("Database credentials loaded from Airflow Variables.")
    except Exception as e:
        logger.error(f"Error accessing Airflow Variables: {e}. Cannot proceed with DB write.")
        # Raise an exception to fail the task if variables are not set
        raise Exception("Airflow Variables not configured.")

    # Construct the database connection string
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Define file and table names
    base_url = 'https://reports-public.ieso.ca/public/Demand/'
    filename = 'PUB_Demand.csv'
    url = f"{base_url}{filename}"
    local_filename = filename
    table_name = 'IESO_DEMAND'  # Name of the table to write to in PostgreSQL

    # --- 1. Download the file ---
    try:
        logger.info(f"Attempting to download {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        logger.info(f"Successfully downloaded {local_filename}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading the file: {e}")
        raise e  # Re-raise the exception to fail the task

    # --- 2. Process the downloaded CSV into a pandas DataFrame ---
    try:
        if os.path.exists(local_filename):
            col_names = ['Date', 'Hour', 'Market_Demand', 'Ontario_Demand']

            df = pd.read_csv(local_filename, header=None, names=col_names)

            # Data cleaning and type conversion
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%Y-%m-%d')
            df.dropna(subset=['Date'], inplace=True)
            df['Modified_DT'] = pd.Timestamp.now().floor('S')
            #set dtype of other columns
            df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce')
            df['Market_Demand'] = pd.to_numeric(df['Market_Demand'], errors='coerce')
            df['Ontario_Demand'] = pd.to_numeric(df['Ontario_Demand'], errors='coerce')

            logger.info("\nFirst 5 rows of the processed data:")
            logger.info(df.head())

        else:
            logger.error(f"File {local_filename} was not found, cannot read.")
            raise FileNotFoundError(f"File {local_filename} was not found after download.")

    except Exception as e:
        logger.error(f"Error reading and processing the CSV file: {e}")
        raise e  # Re-raise the exception to fail the task

    # --- 3. Write the DataFrame to PostgreSQL ---
    try:
        logger.info("Attempting to connect to the PostgreSQL database...")
        engine = create_engine(DATABASE_URL)

        with engine.connect() as conn:
            logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")

            # The .to_sql method will create the table if it doesn't exist
            df.to_sql(
                name=table_name,
                con=conn,
                schema=DB_SCHEMA,
                if_exists='replace',
                index=False
            )
            logger.info(f"Successfully wrote data to table '{table_name}' in schema '{DB_SCHEMA}'.")

    except Exception as e:
        logger.error(f"Error writing to PostgreSQL database: {e}")
        raise e  # Re-raise the exception to fail the task


# Instantiate the DAG object.
with DAG(
        dag_id='ieso_demand_pipeline_one_time',
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['data-pipeline', 'ieso', 'postgres']
) as dag:
    # Define a single task to run the entire pipeline
    run_pipeline_task = PythonOperator(
        task_id='run_ieso_demand_pipeline',
        python_callable=demand_data_pipeline,
    )