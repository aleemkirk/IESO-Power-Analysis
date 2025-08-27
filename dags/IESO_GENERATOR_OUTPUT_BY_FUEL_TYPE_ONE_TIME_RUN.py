# Import the necessary libraries from Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from lxml import etree
from pandas.core.interchange.dataframe_protocol import DataFrame

@dag (
    dag_id = 'ieso_output_by_fuel_hourly_one_time',
    schedule = "@once",
    start_date = datetime(2020, 1, 1),
    catchup = False,
    tags = ['output', 'fuel-type', 'data-pipeline', 'ieso', 'postgres']
)
def output_generation_by_fuel_type_pipeline():
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
    def ieso_data_pull() -> str:

        # Define file and table names
        base_url = 'https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/'
        filename = 'PUB_GenOutputbyFuelHourly.xml'
        url = f"{base_url}{filename}"
        local_filename = filename

        # --- 1. Download the file ---
        try:
            response = requests.get(url)
            response.raise_for_status()

            with open(local_filename, "wb") as f:
                f.write(response.content)

            return filename

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading the file: {e}")
            raise e  # Re-raise the exception to fail the task

    @task
    def transform_data(local_filename) -> DataFrame:
        try:
            # Parse XML
            tree = etree.parse(local_filename)
            root = tree.getroot()

            # Extract namespace (if any)
            nsmap = root.nsmap
            # Some XML files use a default namespace (None key in nsmap), remap it to "ns"
            if None in nsmap:
                nsmap["ns"] = nsmap.pop(None)

            logger.info(f"Successfully downloaded {local_filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading the file: {e}")
            raise e  # Re-raise the exception to fail the task

        # --- 2. Process the downloaded XML into a pandas DataFrame ---
        try:
            records = []

            # Loop over each day
            for daily in root.xpath(".//ns:DailyData", namespaces=nsmap):
                date = daily.findtext("ns:Day", namespaces=nsmap)

                # Loop over each hour
                for hourly in daily.xpath("./ns:HourlyData", namespaces=nsmap):
                    hour = hourly.findtext("ns:Hour", namespaces=nsmap)

                    # Loop over each fuel type
                    for fuel_total in hourly.xpath("./ns:FuelTotal", namespaces=nsmap):
                        fuel_type = fuel_total.findtext("ns:Fuel", namespaces=nsmap)
                        output = fuel_total.findtext("./ns:EnergyValue/ns:Output", namespaces=nsmap)

                        records.append({
                            "date": date,
                            "hour": int(hour),
                            "fuel_type": fuel_type,
                            "output": output if output == None else int(output)
                        })

            # Create DataFrame
            df = pd.DataFrame(records)

            logger.info("\nFirst 5 rows of the processed data:")
            logger.info(df.head())
            return df

        except Exception as e:
            logger.error(f"Error reading and processing the XML file: {e}")
            raise e  # Re-raise the exception to fail the task


    @task
    def create_00_ref(df:DataFrame, db_url,  table_name = '00_GEN_OUTPUT_BY_FUEL_TYPE_HOURLY', db_schema = '00_RAW'):

        try:
            logger.info("Attempting to connect to the PostgreSQL database...")
            engine = create_engine(db_url)
            logger.info("Database connection established.")

        except Exception as e:
            logger.error(f"Error establishing connection to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task

        try:
            with engine.connect() as conn:
                logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")

                # The .to_sql method will create the table if it doesn't exist
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='replace',
                    index=False
                )
                logger.info(f"Successfully wrote data to table '{table_name}' in schema '{db_schema}'.")

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task


    url = postgres_connection()
    file_name = ieso_data_pull()
    df = transform_data(file_name)
    create_00_ref(df, url)


output_generation_by_fuel_type_pipeline()