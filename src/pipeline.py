"""
pipeline.py

This module contains functions for processing tables in the data pipeline.
It includes functions for loading data, checking for missing values and inconsistencies,
handling duplicates, and enforcing data types.
"""

from pyspark.sql import SparkSession
from src.data_preparation import load_data, handle_duplicates, enforce_data_types, check_missing_values, check_format_inconsistencies
from src.utils import get_logger

logger = get_logger(__name__)

def process_table(spark: SparkSession, config: dict, table_name: str):
    """
    Process a table by loading data, checking for missing values and inconsistencies,
    handling duplicates, and enforcing data types.

    Args:
        spark (SparkSession): The Spark session.
        config (dict): The configuration dictionary.
        table_name (str): The name of the table to process.

    Returns:
        DataFrame: The processed DataFrame.
    """
    try:
        # Loading data
        df = load_data(spark, config[table_name])
        
        # Check for missing values and inconsistencies
        check_missing_values(df, config[table_name])
        check_format_inconsistencies(df, config[table_name])
        
        # Handle duplicates and enforce data types
        df = handle_duplicates(df, config[table_name])
        df = enforce_data_types(df, config[table_name])
        
        return df
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        raise