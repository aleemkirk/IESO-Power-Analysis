# Import the necessary libraries from Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, create_engine, MetaData, Table, update
import logging
from lxml import etree
from pandas.core.interchange.dataframe_protocol import DataFrame


@dag(
    dag_id = 'ieso_gen_output_capability_report',
    schedule = "@hourly",
    catchup = False,
    start_date=datetime(2020, 1, 1),
    tags = ['generator', 'ouput', 'capability'],
)
def ouput_capability_report_pipeline():
    logger = logging.getLogger("airflow.task")

    @task(retries=2, retry_delay=timedelta(minutes=5))
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


    @task(retries=2, retry_delay=timedelta(minutes=5))
    def ieso_data_pull() -> str:

        # Define file and table names
        base_url = 'https://reports-public.ieso.ca/public/GenOutputCapability/'
        filename = 'PUB_GenOutputCapability.xml'
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
            ns = root.nsmap
            # Some XML files use a default namespace (None key in nsmap), remap it to "ns"
            if None in ns:
                ns["ns"] = ns.pop(None)

            logger.info(f"Successfully loaded xml data {local_filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error loading the file: {e}")
            raise e  # Re-raise the exception to fail the task

        try:
            # Extract date
            report_date = root.find(".//ns:Date", ns).text

            rows = []

            # Loop through each Generator
            for gen in root.findall(".//ns:Generator", ns):
                gen_name = gen.find("ns:GeneratorName", ns).text
                fuel_type = gen.find("ns:FuelType", ns).text

                # Build dictionaries for outputs and capabilities keyed by Hour
                outputs = {
                    o.find("ns:Hour", ns).text: o.find("ns:EnergyMW", ns).text
                    for o in gen.findall("ns:Outputs/ns:Output", ns)
                    if o.find("ns:Hour", ns) is not None and o.find("ns:EnergyMW", ns) is not None
                }
                capabilities = {
                    c.find("ns:Hour", ns).text: c.find("ns:EnergyMW", ns).text
                    for c in gen.findall("ns:Capabilities/ns:Capability", ns)
                    if c.find("ns:Hour", ns) is not None and c.find("ns:EnergyMW", ns) is not None
                }
                avail_capacities = {
                    a.find("ns:Hour", ns).text: a.find("ns:EnergyMW", ns).text
                    for a in gen.findall("ns:Capacities/ns:AvailCapacity", ns)
                    if a.find("ns:Hour", ns) is not None and a.find("ns:EnergyMW", ns) is not None
                }

                # Merge all hours
                hours = set(outputs.keys()) | set(capabilities.keys()) | set(avail_capacities.keys())

                for hr in sorted(hours, key=lambda x: int(x)):
                    rows.append({
                        "Date": report_date,
                        "Hour": int(hr),
                        "GeneratorName": gen_name,
                        "FuelType": fuel_type,
                        "OutputEnergy": float(outputs.get(hr, 0)),
                        "CapabilityEnergy": float(capabilities.get(hr, 0)),
                        "AvailCapacity": float(avail_capacities.get(hr, 0))

                    })

            # Create DataFrame
            df = pd.DataFrame(rows)

            return df

        except Exception as e:
            logger.error(f"Error reading and processing the XML file: {e}")
            raise e  # Re-raise the exception to fail the task

    @task
    def update_00_ref(df: DataFrame, db_url, table_name='00_GEN_OUTPUT_CAPABILITY_HOURLY', db_schema='00_RAW') -> bool:

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

            return True

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task

    @task
    def update_00_table_reg(complete, db_url, table_name='00_GEN_OUTPUT_CAPABILITY_HOURLY', db_schema='00_RAW'):

        if not complete:
            logger.info('00_REF table was not updated.')
            return

        # get the current date time that the table was updated
        update_dt = pd.Timestamp.now(tz="America/Toronto").date() 
        logger.info(f"Updating table '00_TABLE_REGISTER' to date '{update_dt}'.")

        try:
            logger.info("Attempting to connect to the PostgreSQL database...")
            engine = create_engine(db_url)
            logger.info("Database connection established.")

        except Exception as e:
            logger.error(
                f"Error establishing connection to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task
        try:
            # load the table details
            metadata = MetaData(schema='00_REF')
            # Reflect the table structure from the database
            table = Table('00_TABLE_REGISTER', metadata, autoload_with=engine)

            with engine.begin() as conn:
                logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")
                stmt = (
                    update(table)
                    .where((table.c.TABLE_NAME == table_name) &
                           (table.c.TABLE_SCHEMA == db_schema))  # optional filter if needed
                    .values(MODIFIED_DT=update_dt)  # set columns

                )
                result = conn.execute(stmt)

            logger.info(result)

            logger.info(f"Successfully wrote data to table '{table_name}' in schema '{db_schema}'.")
            logger.info(table.c.keys())

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL database: {e}")
            raise e  # Re-raise the exception to fail the task

    url = postgres_connection()
    file_name = ieso_data_pull()
    df = transform_data(file_name)
    comp = update_00_ref(df, url)
    update_00_table_reg(comp, url)

ouput_capability_report_pipeline()