import logging
import pandas as pd
from sqlalchemy import create_engine, create_engine, MetaData, Table, update, select, func, delete, insert


def update_00_table_reg(complete, logger: logging.Logger, db_url, table_name, db_schema):

    if not complete:
        logger.info('00_REF table was not updated.')
        return

    # get the current date time that the table was updated
    update_dt = pd.Timestamp.now(tz="America/Toronto").date()
    update_timestamp = pd.Timestamp.now(tz="America/Toronto")
    logger.info(f"Updating table '00_TABLE_REGISTER' to date '{update_dt}' at timestamp '{update_timestamp}'.")

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
        query_table = Table(table_name, MetaData(schema=db_schema), autoload_with=engine)

        with engine.begin() as conn:
            logger.info(f"Connection to database successful. Writing data to table '{table_name}'...")

            row_cnt_stmt = (
                select(func.count()).select_from(query_table)
            )
            result = conn.execute(row_cnt_stmt)
            row_count = result.scalar()

            stmt = insert(table).values(
                TABLE_NAME=table_name,
                TABLE_SCHEMA=db_schema,
                MODIFIED_DT=update_dt,
                MODIFIED_TIME=update_timestamp,
                ROW_COUNT=row_count
            )
            result = conn.execute(stmt)

        logger.info(result)

        logger.info(f"Successfully wrote data to table '{table_name}' in schema '{db_schema}'.")
        logger.info(table.c.keys())

    except Exception as e:
        logger.error(f"Error writing to PostgreSQL database: {e}")
        raise e  # Re-raise the exception to fail the task