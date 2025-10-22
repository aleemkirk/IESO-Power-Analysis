"""
IESO Zonal Demand Transformation Pipeline (00_RAW → 01_PRI)

This DAG reads zonal demand data from 00_RAW schema, transforms it from
wide format to normalized (melted) format, and writes it to 01_PRI schema.

Schedule: @hourly
Source Table: 00_RAW.00_IESO_ZONAL_DEMAND
Target Table: 01_PRI.01_IESO_ZONAL_DEMAND
"""

from airflow.sdk import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pandas as pd
from sqlalchemy import MetaData, Table, select
import logging

# Import shared utilities
from dags.utils.database import get_database_url, get_engine
from dags.utils import updates
from config.config_loader import get_table_name, get_schema_name

logger = logging.getLogger(__name__)


@dag(
    dag_id='01_hourly_ieso_zonal_demand_pipeline',
    schedule='@hourly',
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['demand', 'data-pipeline', 'ieso', 'postgres', 'zonal', 'transformation', 'refactored']
)
def ieso_zonal_demand_01_data_pipeline():
    """
    IESO Zonal Demand Transformation Pipeline

    Pipeline Steps:
    1. Wait for upstream DAG (hourly_ieso_zonal_demand_pipeline) to complete
    2. Read data from 00_RAW.00_IESO_ZONAL_DEMAND
    3. Transform from wide format (zone columns) to long format (Zone, Value columns)
    4. Replace data in 01_PRI.01_IESO_ZONAL_DEMAND
    5. Update table register with metadata
    """

    # Load configuration
    source_table = get_table_name('zonal_demand')
    source_schema = get_schema_name('raw')
    target_table = get_table_name('zonal_demand_normalized')
    target_schema = get_schema_name('primary')

    # External task sensor - wait for upstream DAG
    wait_for_00_zonal = ExternalTaskSensor(
        task_id="wait_for_00_zonal",
        external_dag_id="hourly_ieso_zonal_demand_pipeline",
        external_task_id=None,  # Wait for entire DAG to complete
        poke_interval=60,
        timeout=3600,  # 1 hour timeout
        mode="poke"
    )

    @task
    def postgres_connection() -> str:
        """
        Get PostgreSQL connection string from Airflow Variables.

        Returns:
            str: Database connection URL
        """
        return get_database_url()

    @task
    def read_from_raw_schema(
        db_url: str,
        db_schema: str = source_schema,
        table_name: str = source_table
    ) -> pd.DataFrame:
        """
        Read zonal demand data from 00_RAW schema.

        Reads all data from the raw zonal demand table in wide format
        (with separate columns for each zone).

        Args:
            db_url: Database connection string
            db_schema: Source schema name (00_RAW)
            table_name: Source table name (00_IESO_ZONAL_DEMAND)

        Returns:
            DataFrame with wide-format zonal demand data

        Raises:
            SQLAlchemyError: If database read fails
        """
        try:
            logger.info(f"Connecting to database to read from {db_schema}.{table_name}...")
            engine = get_engine(db_url)
            logger.info("Database connection established.")

            # Reflect table structure
            metadata = MetaData(schema=db_schema)
            table = Table(table_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                logger.info(f"Reading data from {db_schema}.{table_name}...")
                query = select(table)
                result = conn.execute(query)

                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.info(f"Successfully read {len(df)} rows from {db_schema}.{table_name}")
                logger.info(f"Columns: {list(df.columns)}")

            return df

        except Exception as e:
            logger.error(f"Error reading from {db_schema}.{table_name}: {e}")
            raise

    @task
    def transform_to_normalized_format(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform zonal demand data from wide to long (normalized) format.

        Melts the DataFrame to convert zone columns (Northwest, Northeast, etc.)
        into (Zone, Value) key-value pairs for normalized storage.

        Args:
            df: Wide-format DataFrame with zone columns

        Returns:
            Long-format DataFrame with Zone and Value columns

        Raises:
            KeyError: If expected columns are missing
        """
        try:
            logger.info("Transforming data from wide to long format...")

            # Identify columns
            id_vars = ['Date', 'Hour', 'Modified_DT', 'Ontario_Demand', 'Zone_Total', 'Diff']

            # Verify required columns exist
            missing_cols = set(id_vars) - set(df.columns)
            if missing_cols:
                raise KeyError(f"Missing required columns: {missing_cols}")

            # Melt DataFrame - convert zone columns to rows
            df_melted = pd.melt(
                df,
                id_vars=id_vars,
                var_name='Zone',
                value_name='Value',
            )

            logger.info(f"Transformed {len(df)} rows (wide) → {len(df_melted)} rows (long)")
            logger.info(f"Sample transformed data:\n{df_melted.head(10)}")

            return df_melted

        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise

    @task
    def write_to_primary_schema(
        df: pd.DataFrame,
        db_url: str,
        db_schema: str = target_schema,
        table_name: str = target_table
    ) -> bool:
        """
        Write normalized zonal demand data to 01_PRI schema.

        Replaces all data in the target table with the new transformed data.

        Args:
            df: Normalized (long-format) DataFrame to write
            db_url: Database connection string
            db_schema: Target schema name (01_PRI)
            table_name: Target table name (01_IESO_ZONAL_DEMAND)

        Returns:
            bool: True if write successful

        Raises:
            SQLAlchemyError: If database write fails
        """
        try:
            logger.info(f"Connecting to database to write to {db_schema}.{table_name}...")
            engine = get_engine(db_url)
            logger.info("Database connection established.")

            with engine.connect() as conn:
                # Replace all data in target table
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='replace',
                    index=False
                )
                logger.info(f"Successfully replaced data in {db_schema}.{table_name}")
                logger.info(f"Total rows written: {len(df)}")

            return True

        except Exception as e:
            logger.error(f"Error writing to {db_schema}.{table_name}: {e}")
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
                table_name=target_table,
                db_schema=target_schema
            )

    # Define task dependencies
    db_url = postgres_connection()
    raw_data = read_from_raw_schema(db_url)
    normalized_data = transform_to_normalized_format(raw_data)
    status = write_to_primary_schema(normalized_data, db_url)
    update_table_register(status, db_url)

    # Set upstream dependency - wait for raw data to be loaded
    wait_for_00_zonal >> db_url


# Instantiate the DAG
ieso_zonal_demand_01_data_pipeline()
