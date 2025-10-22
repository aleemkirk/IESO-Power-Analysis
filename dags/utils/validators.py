"""
Data validation utilities for IESO data pipeline.

This module provides functions to validate DataFrame structure and data quality
before writing to the database.
"""

import pandas as pd
import logging
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Raised when data validation fails."""
    pass


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: List[str],
    check_nulls: bool = True,
    check_duplicates: bool = False,
    min_rows: int = 1
) -> pd.DataFrame:
    """
    Validate DataFrame structure and data quality.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        check_nulls: Whether to log warning for null values
        check_duplicates: Whether to log warning for duplicate rows
        min_rows: Minimum number of rows required (default: 1)

    Returns:
        Validated DataFrame (same as input if validation passes)

    Raises:
        DataValidationError: If validation fails
    """
    # Check DataFrame is not None
    if df is None:
        raise DataValidationError("DataFrame is None")

    # Check minimum row count
    if len(df) < min_rows:
        raise DataValidationError(
            f"DataFrame has {len(df)} rows, but {min_rows} required"
        )

    # Check required columns exist
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise DataValidationError(
            f"Missing required columns: {sorted(missing_cols)}"
        )

    # Check for nulls
    if check_nulls:
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            null_cols = null_counts[null_counts > 0]
            logger.warning(
                f"Null values detected: {null_cols.to_dict()}"
            )

    # Check for duplicates
    if check_duplicates:
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            logger.warning(f"Found {dup_count} duplicate rows")

    logger.info(
        f"Validation passed: {len(df)} rows, {len(df.columns)} columns"
    )

    return df


def validate_demand_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate IESO demand data specifically.

    Checks for required columns, valid hour range, and positive demand values.

    Args:
        df: Demand DataFrame to validate

    Returns:
        Validated DataFrame

    Raises:
        DataValidationError: If validation fails
    """
    # Basic structure validation
    df = validate_dataframe(
        df,
        required_columns=['Date', 'Hour', 'Market_Demand', 'Ontario_Demand'],
        check_nulls=True
    )

    # Validate Hour range (1-24)
    if 'Hour' in df.columns:
        invalid_hours = df[(df['Hour'] < 1) | (df['Hour'] > 24)]
        if len(invalid_hours) > 0:
            raise DataValidationError(
                f"Hour values must be between 1 and 24. "
                f"Found {len(invalid_hours)} invalid values."
            )

    # Validate demand values are non-negative
    if 'Market_Demand' in df.columns and (df['Market_Demand'] < 0).any():
        raise DataValidationError("Market_Demand values cannot be negative")

    if 'Ontario_Demand' in df.columns and (df['Ontario_Demand'] < 0).any():
        raise DataValidationError("Ontario_Demand values cannot be negative")

    logger.info("Demand data validation passed")
    return df


def validate_zonal_demand_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate IESO zonal demand data.

    Args:
        df: Zonal demand DataFrame to validate

    Returns:
        Validated DataFrame

    Raises:
        DataValidationError: If validation fails
    """
    df = validate_dataframe(
        df,
        required_columns=['Date', 'Hour'],
        check_nulls=True
    )

    # Validate Hour range
    if 'Hour' in df.columns:
        invalid_hours = df[(df['Hour'] < 1) | (df['Hour'] > 24)]
        if len(invalid_hours) > 0:
            raise DataValidationError(
                f"Hour values must be between 1 and 24. "
                f"Found {len(invalid_hours)} invalid values."
            )

    logger.info("Zonal demand data validation passed")
    return df


def validate_generator_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate IESO generator output data.

    Args:
        df: Generator data DataFrame to validate

    Returns:
        Validated DataFrame

    Raises:
        DataValidationError: If validation fails
    """
    df = validate_dataframe(
        df,
        required_columns=['Date', 'Fuel_Type'],
        check_nulls=True,
        min_rows=1
    )

    # Check that fuel types are not empty strings
    if 'Fuel_Type' in df.columns:
        empty_fuel = df[df['Fuel_Type'].str.strip() == '']
        if len(empty_fuel) > 0:
            logger.warning(f"Found {len(empty_fuel)} rows with empty Fuel_Type")

    logger.info("Generator data validation passed")
    return df


def log_dataframe_summary(df: pd.DataFrame, name: str = "DataFrame") -> None:
    """
    Log summary statistics for a DataFrame.

    Args:
        df: DataFrame to summarize
        name: Name to use in log messages
    """
    logger.info(f"{name} Summary:")
    logger.info(f"  Rows: {len(df)}")
    logger.info(f"  Columns: {len(df.columns)}")
    logger.info(f"  Column names: {list(df.columns)}")
    logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Log null counts
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.info(f"  Null counts: {null_counts[null_counts > 0].to_dict()}")

    # Log dtypes
    logger.info(f"  Data types:\n{df.dtypes}")
