"""
IESO Public Zonal Demand Hourly Pipeline

This DAG downloads hourly Ontario electricity demand data by geographic zone
from IESO's public API, processes it, and loads it into PostgreSQL.

Schedule: @hourly
Target Table: 00_RAW.00_IESO_ZONAL_DEMAND
"""

from airflow.sdk import dag, task
from datetime import datetime
import requests
import os
import pandas as pd
from sqlalchemy import MetaData, Table, delete
import logging

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from dags.utils import updates
from config.config_loader import get_ieso_url, get_table_name, get_schema_name, get_config

logger = logging.getLogger(__name__)


@dag(
    dag_id='hourly_ieso_zonal_demand_pipeline',
    schedule="@hourly",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['demand', 'data-pipeline', 'ieso', 'postgres', 'zonal', 'refactored']
)
def ieso_zonal_demand_data_pipeline():
    """
    IESO Zonal Demand Data Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download zonal demand CSV from IESO API
    3. Transform and filter data to latest date
    4. Write to PostgreSQL (delete/replace pattern)
    5. Update table register with metadata
    """

    # Load configuration
    table_name = get_table_name('zonal_demand')
    schema_name = get_schema_name('raw')
    endpoint_url = get_ieso_url('zonal_demand')
    filename = 'PUB_DemandZonal.csv'
    timeout = get_config('ieso.download_timeout', default=30)
    chunk_size = get_config('ieso.chunk_size', default=8192)
    timezone = get_config('timezone', default='America/Toronto')

    @task
    def postgres_connection() -> str:
        """
        Get PostgreSQL connection string from Airflow Variables.

        Returns:
            str: Database connection URL
        """
        return get_database_url()

    @task
    def ieso_zonal_demand_data_pull() -> str:
        """
        Download IESO zonal demand CSV file.

        Downloads the latest zonal demand data from IESO's public API
        and saves it to the local filesystem.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails
        """
        try:
            logger.info(f"Downloading {endpoint_url}...")
            response = requests.get(endpoint_url, stream=True, timeout=timeout)
            response.raise_for_status()

            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

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
        Parse and transform IESO zonal demand CSV data.

        Reads the CSV file with zone columns, converts data types,
        adds modification timestamp, and filters to only the most
        recent date's records.

        Args:
            local_filename: Path to downloaded CSV file

        Returns:
            DataFrame with columns: Date, Hour, Ontario_Demand, zone columns,
            Zone_Total, Diff, Modified_DT. Filtered to latest date.

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            pd.errors.ParserError: If CSV parsing fails
        """
        if not os.path.exists(local_filename):
            logger.error(f"File {local_filename} not found")
            raise FileNotFoundError(f"File {local_filename} was not found after download.")

        try:
            col_names = [
                'Date', 'Hour', 'Ontario_Demand',
                'Northwest', 'Northeast', 'Ottawa', 'East',
                'Toronto', 'Essa', 'Bruce', 'Southwest',
                'Niagara', 'West', 'Zone_Total', 'Diff'
            ]

            df = pd.read_csv(local_filename, header=None, names=col_names)

            # Data cleaning and type conversion
            date_format = get_config('data_processing.date_format', default='%Y-%m-%d')
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format=date_format)
            df.dropna(subset=['Date'], inplace=True)

            df['Modified_DT'] = pd.Timestamp.now(tz=timezone).floor('S')

            # Convert all numeric columns
            numeric_cols = [
                'Hour', 'Ontario_Demand', 'Northwest', 'Northeast', 'Ottawa', 'East',
                'Toronto', 'Essa', 'Bruce', 'Southwest', 'Niagara', 'West',
                'Zone_Total', 'Diff'
            ]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"Processed {len(df)} total rows")
            logger.info(f"First 5 rows:\n{df.head()}")

            # Filter to latest date only
            max_date = df['Date'].max()
            filtered_df = df[df['Date'] == max_date]

            logger.info(f"Filtered to {len(filtered_df)} rows for date {max_date}")

            return filtered_df

        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            raise

    @task
    def write_to_database(
        df: pd.DataFrame,
        db_url: str,
        table_name: str = table_name,
        db_schema: str = schema_name
    ) -> bool:
        """
        Write zonal demand data to PostgreSQL.

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
    filename = ieso_zonal_demand_data_pull()
    df = transform_data(filename)
    status = write_to_database(df, db_url)
    update_table_register(status, db_url)


# Instantiate the DAG
ieso_zonal_demand_data_pipeline()
