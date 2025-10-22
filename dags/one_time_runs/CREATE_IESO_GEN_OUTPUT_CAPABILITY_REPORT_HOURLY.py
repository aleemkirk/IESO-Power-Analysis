"""
IESO Generator Output Capability Report One-Time Historical Load

This DAG downloads ALL historical generator output and capability data from
IESO's public API (XML format) and performs initial bulk load into PostgreSQL.

Schedule: @once (one-time execution)
Target Table: 00_RAW.00_GEN_OUTPUT_CAPABILITY_HOURLY
Data Format: XML (complex nested structure)
Purpose: Initial historical data load
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
from lxml import etree

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from config.config_loader import get_ieso_url, get_table_name, get_schema_name, get_config

logger = logging.getLogger(__name__)


@dag(
    dag_id='create_ieso_gen_output_capability_report',
    schedule=None,  # One-time run, manual trigger only
    catchup=False,
    start_date=datetime(2020, 1, 1),
    tags=['generator', 'output', 'capability', 'data-pipeline', 'ieso', 'postgres', 'xml', 'one-time', 'historical', 'refactored']
)
def output_capability_report_pipeline():
    """
    IESO Generator Output Capability Report One-Time Historical Load Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download XML file from IESO API (with retries)
    3. Parse complex nested XML structure
    4. Extract ALL historical output, capability, and available capacity data
    5. Write to PostgreSQL (replace entire table)
    6. Note: Does NOT update table register (one-time load)
    """

    # Load configuration
    table_name = get_table_name('capability')
    schema_name = get_schema_name('raw')
    endpoint_url = get_ieso_url('capability')
    filename = 'PUB_GenOutputCapability.xml'
    timeout = get_config('ieso.download_timeout', default=30)

    @task
    def postgres_connection() -> str:
        """
        Get PostgreSQL connection string from Airflow Variables.

        Returns:
            str: Database connection URL
        """
        return get_database_url()

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def ieso_data_pull() -> str:
        """
        Download FULL historical IESO generator capability XML file with retries.

        Downloads ALL available historical generator output and capability data.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails after retries
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
        Parse XML and transform ALL generator capability data.

        Parses the IESO XML file with complex nested structure and extracts
        ALL historical records (no date filtering).

        Args:
            local_filename: Path to downloaded XML file

        Returns:
            DataFrame with columns: Date, Hour, GeneratorName, FuelType,
            OutputEnergy, CapabilityEnergy, AvailCapacity
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
            # Extract report date
            report_date = root.find(".//ns:Date", nsmap).text
            logger.info(f"Processing report for date: {report_date}")

            rows = []

            # Loop through each Generator
            for gen in root.findall(".//ns:Generator", nsmap):
                gen_name = gen.find("ns:GeneratorName", nsmap).text
                fuel_type = gen.find("ns:FuelType", nsmap).text

                # Build dictionaries for outputs, capabilities, and available capacities
                outputs = {
                    o.find("ns:Hour", nsmap).text: o.find("ns:EnergyMW", nsmap).text
                    for o in gen.findall("ns:Outputs/ns:Output", nsmap)
                    if o.find("ns:Hour", nsmap) is not None and o.find("ns:EnergyMW", nsmap) is not None
                }
                capabilities = {
                    c.find("ns:Hour", nsmap).text: c.find("ns:EnergyMW", nsmap).text
                    for c in gen.findall("ns:Capabilities/ns:Capability", nsmap)
                    if c.find("ns:Hour", nsmap) is not None and c.find("ns:EnergyMW", nsmap) is not None
                }
                avail_capacities = {
                    a.find("ns:Hour", nsmap).text: a.find("ns:EnergyMW", nsmap).text
                    for a in gen.findall("ns:Capacities/ns:AvailCapacity", nsmap)
                    if a.find("ns:Hour", nsmap) is not None and a.find("ns:EnergyMW", nsmap) is not None
                }

                # Merge all hours
                all_hours = set(outputs.keys()) | set(capabilities.keys()) | set(avail_capacities.keys())

                # Create a row for each hour
                for hr in sorted(all_hours, key=lambda x: int(x)):
                    rows.append({
                        "Date": pd.to_datetime(report_date).date(),
                        "Hour": int(hr),
                        "GeneratorName": gen_name,
                        "FuelType": fuel_type,
                        "OutputEnergy": float(outputs.get(hr, 0)),
                        "CapabilityEnergy": float(capabilities.get(hr, 0)),
                        "AvailCapacity": float(avail_capacities.get(hr, 0))
                    })

            # Create DataFrame
            df = pd.DataFrame(rows)

            if len(df) == 0:
                raise ValueError("No data extracted from XML file")

            logger.info(f"Extracted {len(df)} records from XML (ALL HISTORICAL DATA)")
            logger.info(f"Report date: {report_date}")
            logger.info(f"Number of generators: {df['GeneratorName'].nunique()}")
            logger.info(f"Number of fuel types: {df['FuelType'].nunique()}")
            logger.info(f"Fuel types: {df['FuelType'].unique().tolist()}")
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
        Write ALL historical generator capability data to PostgreSQL.

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
output_capability_report_pipeline()
