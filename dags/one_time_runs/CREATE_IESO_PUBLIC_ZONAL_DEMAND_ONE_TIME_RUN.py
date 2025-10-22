"""
IESO Public Zonal Demand One-Time Historical Load

This DAG downloads ALL historical Ontario zonal electricity demand data from
IESO's public API and performs initial bulk load into PostgreSQL.

Schedule: @once (one-time execution)
Target Table: 00_RAW.00_IESO_ZONAL_DEMAND
Purpose: Initial historical data load
"""

from airflow.sdk import dag, task
from datetime import datetime
import requests
import os
import pandas as pd
from sqlalchemy import MetaData, Table
import logging

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from config.config_loader import get_ieso_url, get_table_name, get_schema_name, get_config

logger = logging.getLogger(__name__)


@dag(
    dag_id='create_ieso_zonal_demand_pipeline_one_time',
    schedule=None,  # One-time run, manual trigger only
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['demand', 'data-pipeline', 'ieso', 'postgres', 'zonal', 'one-time', 'historical', 'refactored']
)
def ieso_zonal_demand_data_pipeline():
    """
    IESO Zonal Demand Data One-Time Historical Load Pipeline

    Pipeline Steps:
    1. Get database connection string
    2. Download FULL historical zonal demand CSV from IESO API
    3. Transform ALL data (no date filtering)
    4. Write to PostgreSQL (replace entire table)
    5. Note: Does NOT update table register (one-time load)
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
        Download FULL historical IESO zonal demand CSV file.

        Downloads ALL available historical zonal demand data from IESO's public API
        and saves it to the local filesystem.

        Returns:
            str: Path to downloaded file

        Raises:
            requests.exceptions.RequestException: If download fails
        """
        try:
            logger.info(f"Downloading FULL historical zonal data from {endpoint_url}...")
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
        Parse and transform ALL IESO zonal demand CSV data.

        Reads the CSV file with zone columns, converts data types,
        adds modification timestamp.
        DOES NOT filter to latest date - loads all historical data.

        Args:
            local_filename: Path to downloaded CSV file

        Returns:
            DataFrame with columns: Date, Hour, Ontario_Demand, zone columns,
            Zone_Total, Diff, Modified_DT. Contains ALL historical records.

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

            logger.info(f"Processed {len(df)} total rows (ALL HISTORICAL DATA)")
            logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
            logger.info(f"First 5 rows:\n{df.head()}")
            logger.info(f"Last 5 rows:\n{df.tail()}")

            # NO FILTERING - return all data for one-time load
            return df

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
        Write ALL historical zonal demand data to PostgreSQL.

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
    filename = ieso_zonal_demand_data_pull()
    df = transform_data(filename)
    status = write_to_database(df, db_url)


# Instantiate the DAG
ieso_zonal_demand_data_pipeline()
