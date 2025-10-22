"""
Database connection utilities for IESO data pipeline.

This module provides shared functions for database connectivity and operations,
eliminating code duplication across DAG files.
"""

import logging
from typing import Optional
from sqlalchemy import create_engine, Engine
from airflow.models import Variable

logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """
    Build PostgreSQL connection string from Airflow Variables.

    Retrieves database credentials from Airflow Variables and constructs
    a SQLAlchemy-compatible connection string.

    Returns:
        str: Database URL in format postgresql://user:pass@host:port/db

    Raises:
        ValueError: If required Airflow Variables are not configured

    Example:
        >>> url = get_database_url()
        >>> 'postgresql://user:pass@host:5432/dbname'
    """
    try:
        db_user = Variable.get('DB_USER')
        db_password = Variable.get('DB_PASSWORD')
        db_host = Variable.get('HOST')
        db_port = Variable.get('DB_PORT', default_var='5432')
        db_name = Variable.get('DB')

        logger.info("Database credentials loaded from Airflow Variables.")

        # Construct the database connection string
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        logger.info("Database URL created successfully.")

        return database_url

    except Exception as e:
        logger.error(f"Error accessing Airflow Variables: {e}")
        raise ValueError(
            "Database Airflow Variables not configured. "
            "Please set DB_USER, DB_PASSWORD, HOST, DB_PORT, and DB."
        ) from e


def get_engine(db_url: Optional[str] = None, **kwargs) -> Engine:
    """
    Create SQLAlchemy engine with connection pooling.

    Args:
        db_url: Database connection string. If None, retrieves from Airflow Variables.
        **kwargs: Additional arguments to pass to create_engine()

    Returns:
        Engine: SQLAlchemy engine instance

    Example:
        >>> engine = get_engine()
        >>> with engine.connect() as conn:
        ...     result = conn.execute("SELECT 1")
    """
    if db_url is None:
        db_url = get_database_url()

    # Default engine configuration for production use
    default_kwargs = {
        'pool_pre_ping': True,  # Verify connections before using
        'pool_size': 5,
        'max_overflow': 10,
        'pool_recycle': 3600,  # Recycle connections after 1 hour
    }

    # Merge with user-provided kwargs
    engine_kwargs = {**default_kwargs, **kwargs}

    logger.info("Creating database engine with connection pooling.")
    return create_engine(db_url, **engine_kwargs)
