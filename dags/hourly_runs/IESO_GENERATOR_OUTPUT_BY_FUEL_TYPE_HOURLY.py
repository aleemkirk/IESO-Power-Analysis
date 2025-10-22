"""
IESO Generator Output by Fuel Type Hourly Pipeline

This DAG downloads hourly generator output data by fuel type from IESO's
public API (XML format), parses it, and loads it into PostgreSQL.

Schedule: @hourly
Target Table: 00_RAW.00_GEN_OUTPUT_BY_FUEL_TYPE_HOURLY
Data Format: XML
"""

from airflow.sdk import dag, task
from datetime import datetime
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
    dag_id='hourly_ieso_output_by_fuel_hourly',
    schedule="@hourly",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['output', 'fuel-type', 'data-pipeline', 'ieso', 'postgres', 'xml', 'refactored']
)
def output_generation_by_fuel_type_pipeline():
    """
    IESO Generator Output by Fuel Type Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download XML file from IESO API
    3. Parse XML and extract fuel type output data
    4. Transform to DataFrame format
    5. Write to PostgreSQL (delete/replace pattern)
    6. Update table register with metadata
    """

    # Load configuration - capture at DAG definition time
    _table_name = get_table_name('fuel_output')
    _schema_name = get_schema_name('raw')
    _endpoint_url = get_ieso_url('fuel_output')
    _filename = 'PUB_GenOutputbyFuelHourly.xml'
    _timeout = get_config('ieso.download_timeout', default=30)

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
        Download IESO generator output XML file.

        Downloads the latest generator output by fuel type data from
        IESO's public API and saves it to the local filesystem.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails
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
        Parse XML and transform generator output data.

        Parses the IESO XML file with nested structure:
        - DailyData (Date)
          - HourlyData (Hour)
            - FuelTotal (Fuel type and output value)

        Extracts and flattens into tabular format.

        Args:
            local_filename: Path to downloaded XML file

        Returns:
            DataFrame with columns: Date, hour, fuel_type, output
            Filtered to contain only the latest date's records.

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

            logger.info(f"Extracted {len(df)} records from XML")
            logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
            logger.info(f"Fuel types: {df['fuel_type'].unique().tolist()}")
            logger.info(f"First 5 rows:\n{df.head()}")

            # Filter to latest date only
            max_date = df['Date'].max()
            filtered_df = df[df['Date'] == max_date]

            logger.info(f"Filtered to {len(filtered_df)} rows for date {max_date}")

            return filtered_df

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
        table_name: str = _table_name,
        db_schema: str = _schema_name
    ) -> bool:
        """
        Write generator output data to PostgreSQL.

        Deletes existing records for the latest date, then inserts new records.

        Args:
            df: DataFrame to write
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

            # Reflect table structure
            metadata = MetaData(schema=db_schema)
            table = Table(table_name, metadata, autoload_with=engine)

            max_date = df['Date'].max()

            with engine.connect() as conn:
                # Delete existing entries for this date
                delete_stmt = delete(table).where(table.c.Date == max_date)
                result = conn.execute(delete_stmt)
                logger.info(f"Deleted {result.rowcount} existing rows for date {max_date}")

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
    status = write_to_database(df, db_url)
    update_table_register(status, db_url)


# Instantiate the DAG
output_generation_by_fuel_type_pipeline()
