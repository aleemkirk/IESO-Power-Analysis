# Import the necessary libraries from Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime
import requests
import os
import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame
from sqlalchemy import create_engine, create_engine, MetaData, Table, update, select, func, delete
import logging
from dags.utils import updates
from airflow.sensors.external_task import ExternalTaskSensor


@dag (
    dag_id = '01_hourly_ieso_zonal_demand_pipeline',
    schedule = '@hourly',
    start_date = datetime(2020, 1, 1),
    catchup = False,
    tags = ['demand', 'data-pipeline', 'ieso', 'postgres', 'zonal']
)
def ieso_zonal_demand_01_data_pipeline():
    # Set up global variables
    logger = logging.getLogger("airflow.task")
    table = '01_IESO_ZONAL_DEMAND'
    schema = '01_PRI'

    wait_for_00_zonal = ExternalTaskSensor(
        task_id="wait_for_00_zonal",
        external_dag_id="hourly_ieso_zonal_demand_pipeline",  # dag_id of DAG1
        external_task_id=None,  # None means "wait for whole DAG1"
        poke_interval=60,  # check every 60s
        timeout=60 * 60,  # 1 hour before failing
        mode="poke"  # or "reschedule" to free worker slots
    )

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
    def ieso_zonal_demand_data_pull(db_url, db_schema, table_name) -> DataFrame:


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
            db_table = Table(table_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")

                # delete today's entries
                query = select(db_table)
                result = conn.execute(query)
                logger.info(f"Selected {result.rowcount} rows from table '{table_name}'.")

                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                return df

        except Exception as e:
            logger.error(f"Error Reading data from table '{table_name}': {e}")
            raise e  # Re-raise the exception to fail the task


    @task
    def transform_data (df) -> DataFrame:

        try:
            logger.info("Attempting to transform data...")

            df_melted = pd.melt(
                df,
                id_vars=['Date', 'Hour', 'Modified_DT', 'Ontario_Demand', 'Zone_Total', 'Diff'],
                var_name='Zone',
                value_name='Value',
            )

            return df_melted

        except Exception as e:
            raise e
    @task
    def update_01_pri(df, db_url, db_schema, table_name) -> bool:

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
            # db_table = Table(table_name, metadata, autoload_with=engine)

            with engine.connect() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=db_schema,
                    if_exists='replace',
                    index=False
                )
                logger.info(f"Successfully replaced data in table '{table_name}' in schema '{db_schema}'.")
                return True

        except Exception as e:
            raise e

    @task
    def update_00_table_reg(complete, logger, db_url, table_name, db_schema):
        updates.update_00_table_reg(complete, logger, db_url, table_name, db_schema)

    wait_for_00_zonal >> postgres_connection()
    url = postgres_connection()
    data = ieso_zonal_demand_data_pull(url, '00_RAW', '00_IESO_ZONAL_DEMAND')
    trans_data = transform_data(data)
    status = update_01_pri(trans_data, url, schema, table)

    update_00_table_reg(status, logger, url, table, schema)




ieso_zonal_demand_01_data_pipeline()