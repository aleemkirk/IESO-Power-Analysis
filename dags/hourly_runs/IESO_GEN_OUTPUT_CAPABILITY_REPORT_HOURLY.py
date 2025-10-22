"""
IESO Generator Output Capability Report Hourly Pipeline

This DAG downloads hourly generator output and capability data from IESO's
public API (XML format), parses nested generator/hour structure, and loads
it into PostgreSQL.

Schedule: @hourly
Target Table: 00_RAW.00_GEN_OUTPUT_CAPABILITY_HOURLY
Data Format: XML (complex nested structure)
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import MetaData, Table, delete
import logging
from lxml import etree

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from dags.utils import updates
from config.config_loader import get_ieso_url, get_table_name, get_schema_name, get_config

logger = logging.getLogger(__name__)


@dag(
    dag_id='hourly_ieso_gen_output_capability_report',
    schedule="@hourly",
    catchup=False,
    start_date=datetime(2020, 1, 1),
    tags=['generator', 'output', 'capability', 'data-pipeline', 'ieso', 'postgres', 'xml', 'refactored']
)
def output_capability_report_pipeline():
    """
    IESO Generator Output Capability Report Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download XML file from IESO API (with retries)
    3. Parse complex nested XML structure (generators × hours)
    4. Extract output, capability, and available capacity for each generator/hour
    5. Write to PostgreSQL (delete/replace pattern)
    6. Update table register with metadata
    """

    # Load configuration - capture at DAG definition time
    _table_name = get_table_name('capability')
    _schema_name = get_schema_name('raw')
    _endpoint_url = get_ieso_url('capability')
    _filename = 'PUB_GenOutputCapability.xml'
    _timeout = get_config('ieso.download_timeout', default=30)
    _timezone = get_config('timezone', default='America/Toronto')

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
        Download IESO generator capability XML file with retries.

        Downloads the latest generator output and capability data from
        IESO's public API. Includes automatic retries for reliability.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails after retries
        """
        try:
            logger.info(f"Downloading {_endpoint_url}...")
            response = requests.get(_endpoint_url, timeout=_timeout)
            response.raise_for_status()

            with open(_filename, "wb") as f:
                f.write(response.content)

            logger.info(f"Successfully downloaded {_filename}")
            return _filename

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout downloading {_endpoint_url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} downloading {_endpoint_url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading {_endpoint_url}: {e}")
            raise

    @task
    def transform_data(local_filename: str) -> pd.DataFrame:
        """
        Parse XML and transform generator capability data.

        Parses the IESO XML file with complex nested structure:
        - Date (report date)
        - Generator (multiple)
          - GeneratorName
          - FuelType
          - Outputs → Output (Hour, EnergyMW)
          - Capabilities → Capability (Hour, EnergyMW)
          - Capacities → AvailCapacity (Hour, EnergyMW)

        Flattens into tabular format with one row per generator-hour combination.

        Args:
            local_filename: Path to downloaded XML file

        Returns:
            DataFrame with columns: Date, Hour, GeneratorName, FuelType,
            OutputEnergy, CapabilityEnergy, AvailCapacity

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
                # Use configured namespace if no namespace in file
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
                # keyed by Hour
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

                # Merge all hours from all three data sources
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

            logger.info(f"Extracted {len(df)} records from XML")
            logger.info(f"Report date: {report_date}")
            logger.info(f"Number of generators: {df['GeneratorName'].nunique()}")
            logger.info(f"Number of fuel types: {df['FuelType'].nunique()}")
            logger.info(f"Fuel types: {df['FuelType'].unique().tolist()}")
            logger.info(f"First 5 rows:\n{df.head()}")

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
        local_filename: str,
        table_name: str = _table_name,
        db_schema: str = _schema_name
    ) -> bool:
        """
        Write generator capability data to PostgreSQL.

        Extracts the report date from XML, deletes existing records for that date,
        then inserts new records.

        Args:
            df: DataFrame to write
            db_url: Database connection string
            local_filename: Path to XML file (needed to extract report date)
            table_name: Target table name
            db_schema: Target schema name

        Returns:
            bool: True if write successful

        Raises:
            SQLAlchemyError: If database operation fails
        """
        # Extract report date from XML
        try:
            tree = etree.parse(local_filename)
            root = tree.getroot()

            nsmap = root.nsmap
            if None in nsmap:
                nsmap["ns"] = nsmap.pop(None)
            else:
                default_ns = get_config('xml.namespaces.default')
                nsmap["ns"] = default_ns

            report_date = pd.to_datetime(root.find(".//ns:Date", nsmap).text).date()
            logger.info(f"Report date from XML: {report_date}")

        except Exception as e:
            logger.error(f"Error extracting report date from XML: {e}")
            raise

        # Write to database
        try:
            logger.info("Connecting to PostgreSQL database...")
            engine = get_engine(db_url)
            logger.info("Database connection established.")

            # Reflect table structure
            metadata = MetaData(schema=db_schema)
            table = Table(table_name, metadata, autoload_with=engine)

            with engine.begin() as conn:
                # Delete existing entries for this date
                delete_stmt = delete(table).where(table.c.Date == report_date)
                result = conn.execute(delete_stmt)
                logger.info(f"Deleted {result.rowcount} existing rows for date {report_date}")

                # Insert new data
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='append',
                    index=False
                )
                logger.info(f"Successfully inserted {len(df)} rows to {db_schema}.{table_name}")

            return True

        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            raise

    @task
    def update_table_register(complete: bool, db_url: str) -> None:
        """
        Update table register with metadata.

        Args:
            complete: Whether previous task completed successfully
            db_url: Database connection string
        """
        if complete:
            updates.update_00_table_reg(
                complete=complete,
                logger=logger,
                db_url=db_url,
                table_name=table_name,
                db_schema=schema_name
            )

    # Define task dependencies
    db_url = postgres_connection()
    filename = ieso_data_pull()
    df = transform_data(filename)
    status = write_to_database(df, db_url, filename)
    update_table_register(status, db_url)


# Instantiate the DAG
output_capability_report_pipeline()
