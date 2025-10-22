"""
IESO Generator Output by Fuel Type One-Time Historical Load

This DAG downloads ALL historical generator output data by fuel type from
IESO's public API (XML format) and performs initial bulk load into PostgreSQL.

Schedule: @once (one-time execution)
Target Table: 00_RAW.00_GEN_OUTPUT_BY_FUEL_TYPE_HOURLY
Data Format: XML
Purpose: Initial historical data load
"""

from airflow.sdk import dag, task
from datetime import datetime
import requests
import pandas as pd
import logging
from lxml import etree

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from config.config_loader import get_ieso_url, get_table_name, get_schema_name, get_config

logger = logging.getLogger(__name__)


@dag(
    dag_id='create_ieso_output_by_fuel_hourly_one_time',
    schedule=None,  # One-time run, manual trigger only
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['output', 'fuel-type', 'data-pipeline', 'ieso', 'postgres', 'xml', 'one-time', 'historical', 'refactored']
)
def output_generation_by_fuel_type_pipeline():
    """
    IESO Generator Output by Fuel Type One-Time Historical Load Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download XML file from IESO API
    3. Parse XML and extract ALL fuel type output data
    4. Transform to DataFrame format (all historical data)
    5. Write to PostgreSQL (replace entire table)
    6. Note: Does NOT update table register (one-time load)
    """

    # Load configuration
    table_name = get_table_name('fuel_output')
    schema_name = get_schema_name('raw')
    endpoint_url = get_ieso_url('fuel_output')
    filename = 'PUB_GenOutputbyFuelHourly.xml'
    timeout = get_config('ieso.download_timeout', default=30)

    @task
    def postgres_connection() -> str:
        """
        Get PostgreSQL connection string from Airflow Variables.

        Returns:
            str: Database connection URL
        """
        return get_database_url()

    @task
    def ieso_data_pull() -> str:
        """
        Download FULL historical IESO generator output XML file.

        Downloads ALL available historical generator output by fuel type data.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails
        """
        try:
            logger.info(f"Downloading FULL historical XML data from {endpoint_url}...")
            response = requests.get(endpoint_url, timeout=timeout)
            response.raise_for_status()

            with open(filename, "wb") as f:
                f.write(response.content)

            logger.info(f"Successfully downloaded {filename}")
            return filename

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout downloading {endpoint_url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} downloading {endpoint_url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {endpoint_url}: {e}")
            raise

    @task
    def transform_data(local_filename: str) -> pd.DataFrame:
        """
        Parse XML and transform ALL generator output data.

        Parses the IESO XML file with nested structure and extracts
        ALL historical records (no date filtering).

        Args:
            local_filename: Path to downloaded XML file

        Returns:
            DataFrame with columns: Date, hour, fuel_type, output
            Contains ALL historical records.

        Raises:
            etree.XMLSyntaxError: If XML parsing fails
            KeyError: If expected XML elements are missing
        """
        try:
            # Parse XML
            logger.info(f"Parsing XML file: {local_filename}")
            tree = etree.parse(local_filename)
            root = tree.getroot()

            # Extract namespace
            nsmap = root.nsmap
            if None in nsmap:
                nsmap["ns"] = nsmap.pop(None)
            else:
                default_ns = get_config('xml.namespaces.default')
                nsmap["ns"] = default_ns

            logger.info(f"XML namespace: {nsmap.get('ns', 'No namespace')}")

        except etree.XMLSyntaxError as e:
            logger.error(f"XML parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error opening XML file: {e}")
            raise

        # Parse XML structure
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
                            "Date": pd.to_datetime(date),
                            "hour": int(hour) if hour else None,
                            "fuel_type": fuel_type,
                            "output": int(output) if output is not None else None
                        })

            # Create DataFrame
            df = pd.DataFrame(records)

            if len(df) == 0:
                raise ValueError("No data extracted from XML file")

            logger.info(f"Extracted {len(df)} records from XML (ALL HISTORICAL DATA)")
            logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
            logger.info(f"Fuel types: {df['fuel_type'].unique().tolist()}")
            logger.info(f"First 5 rows:\n{df.head()}")
            logger.info(f"Last 5 rows:\n{df.tail()}")

            # NO FILTERING - return all data for one-time load
            return df

        except etree.XPathEvalError as e:
            logger.error(f"XPath evaluation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing XML data: {e}")
            raise

    @task
    def write_to_database(
        df: pd.DataFrame,
        db_url: str,
        table_name: str = table_name,
        db_schema: str = schema_name
    ) -> bool:
        """
        Write ALL historical generator output data to PostgreSQL.

        Replaces entire table with historical data (if_exists='replace').
        This is a one-time bulk load operation.

        Args:
            df: DataFrame with all historical data to write
            db_url: Database connection string
            table_name: Target table name
            db_schema: Target schema name

        Returns:
            bool: True if write successful

        Raises:
            SQLAlchemyError: If database operation fails
        """
        try:
            logger.info("Connecting to PostgreSQL database...")
            engine = get_engine(db_url)
            logger.info("Database connection established.")

            with engine.connect() as conn:
                # Replace entire table with historical data
                logger.info(f"Writing ALL historical data to {db_schema}.{table_name}...")
                logger.info("This will REPLACE any existing data in the table.")

                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='replace',  # Replace entire table
                    index=False
                )
                logger.info(f"Successfully wrote {len(df)} rows to {db_schema}.{table_name}")
                logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")

            return True

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            raise

    # Define task dependencies
    db_url = postgres_connection()
    filename = ieso_data_pull()
    df = transform_data(filename)
    status = write_to_database(df, db_url)


# Instantiate the DAG
output_generation_by_fuel_type_pipeline()
