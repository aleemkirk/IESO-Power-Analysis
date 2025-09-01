# Import the necessary libraries from Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import os
import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame
from sqlalchemy import create_engine, create_engine, MetaData, Table, update, select, func, delete
import logging
from typing import Any


@dag (
    dag_id = 'hourly_ieso_zonal_demand_pipeline',
    schedule = "@once",
    start_date = datetime(2020, 1, 1),
    catchup = False,
    tags = ['demand', 'data-pipeline', 'ieso', 'postgres', 'zonal']
)
def ieso_zonal_demand_data_pipeline():
    # Set up a logger for this specific task
    logger = logging.getLogger("airflow.task")

    @task
    def postgres_connection() -> str:
        # Access database credentials from Airflow Variables
        try:
            DB_USER = Variable.get('DB_USER')
            DB_PASSWORD = Variable.get('DB_PASSWORD')
            DB_HOST = Variable.get('HOST')
            DB_PORT = Variable.get('DB_PORT', default_var='5432')
            DB_NAME = Variable.get('DB')

            logger.info("Database credentials loaded from Airflow Variables.")

        except Exception as e:
            logger.error(f"Error accessing Airflow Variables: {e}. Cannot proceed with DB write.")
            # Raise an exception to fail the task if variables are not set
            raise Exception("Airflow Variables not configured.")

        # Construct the database connection string
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        logger.info("Database URL created successfully.")

        return DATABASE_URL



    @task
    def ieso_zonal_demand_data_pull() -> str:

        # Define file and table names
        base_url = 'https://reports-public.ieso.ca/public/DemandZonal/'
        filename = 'PUB_DemandZonal.csv'
        url = f"{base_url}{filename}"
        local_filename = filename


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
            return local_filename

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading the file: {e}")
            raise e  # Re-raise the exception to fail the task


    @task
    def transform_data(local_filename) -> DataFrame:
        # --- 2. Process the downloaded CSV into a pandas DataFrame ---
        try:
            if os.path.exists(local_filename):
                col_names = [
                    'Date',
                    'Hour',
                    'Ontario_Demand',
                    'Northwest',
                    'Northeast',
                    'Ottawa',
                    'East',
                    'Toronto',
                    'Essa',
                    'Bruce',
                    'Southwest',
                    'Niagara',
                    'West',
                    'Zone_Total',
                    'Diff'
                ]

                df = pd.read_csv(local_filename, header=None, names=col_names)

                # Data cleaning and type conversion
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%Y-%m-%d')
                df.dropna(subset=['Date'], inplace=True)
                df['Modified_DT'] = pd.Timestamp.now().floor('S')
                # set dtype of other columns
                df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce')
                df['Ontario_Demand'] = pd.to_numeric(df['Ontario_Demand'], errors='coerce')
                df['Northwest'] = pd.to_numeric(df['Northwest'], errors='coerce')
                df['Northeast'] = pd.to_numeric(df['Northeast'], errors='coerce')
                df['Ottawa'] = pd.to_numeric(df['Ottawa'], errors='coerce')
                df['East'] = pd.to_numeric(df['East'], errors='coerce')
                df['Toronto'] = pd.to_numeric(df['Toronto'], errors='coerce')
                df['Essa'] = pd.to_numeric(df['Essa'], errors='coerce')
                df['Bruce'] = pd.to_numeric(df['Bruce'], errors='coerce')
                df['Southwest'] = pd.to_numeric(df['Southwest'], errors='coerce')
                df['Niagara'] = pd.to_numeric(df['Niagara'], errors='coerce')
                df['West'] = pd.to_numeric(df['West'], errors='coerce')
                df['Zone_Total'] = pd.to_numeric(df['Zone_Total'], errors='coerce')
                df['Diff'] = pd.to_numeric(df['Diff'], errors='coerce')

                logger.info("\nFirst 5 rows of the processed data:")
                logger.info(df.head())

                filtered_data = df[df['Date'] == df['Date'].max()]

                return filtered_data

            else:
                logger.error(f"File {local_filename} was not found, cannot read.")
                raise FileNotFoundError(f"File {local_filename} was not found after download.")

        except Exception as e:
            logger.error(f"Error reading and processing the CSV file: {e}")
            raise e  # Re-raise the exception to fail the task


    @task
    def update_00_ref(df:DataFrame, db_url,  table_name = '00_IESO_ZONAL_DEMAND', db_schema = '00_RAW'):

        try:
            logger.info("Attempting to connect to the PostgreSQL database...")
            engine = create_engine(db_url)
            logger.info("Database connection established.")

        except Exception as e:
            logger.error(f"Error establishing connection to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task

        try:

            metadata = MetaData(schema=db_schema)
            # Reflect the table structure from the database
            table = Table(table_name, metadata, autoload_with=engine)

            max_date = df['Date'].max()

            with engine.connect() as conn:
                logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")

                # delete today's entries
                drop_entries = (
                    delete(table).where(table.c.Date == max_date)
                )
                deleted = conn.execute(drop_entries)
                logger.info(f"Deleted {deleted.rowcount} rows where Date = {max_date}.")

                # The .to_sql method will create the table if it doesn't exist
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='append',
                    index=False
                )
                logger.info(f"Successfully inserted data to table '{table_name}' in schema '{db_schema}'.")

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task


    url = postgres_connection()
    file_name = ieso_zonal_demand_data_pull()
    df = transform_data(file_name)
    update_00_ref(df, url)


ieso_zonal_demand_data_pipeline()