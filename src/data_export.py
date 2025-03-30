"""
data_export.py

This module is responsible for exporting the transformed data for the challenge.
It includes functions to save the enriched dataset in Parquet format and the store_id-level revenue insights in CSV format.

Functions:
- save_enriched_data: Save the enriched dataset in Parquet format, partitioned by category and transaction_date.
- save_revenue_insights: Save the store_id-level revenue insights in CSV format.
"""

from pyspark.sql import DataFrame
from src.utils import get_logger

logger = get_logger(__name__)

def save_enriched_data(enriched_df: DataFrame, output_path: str) -> None:
    """
    Save the enriched dataset in Parquet format, partitioned by category and transaction_date.

    Args:
        enriched_df (DataFrame): The enriched DataFrame.
        output_path (str): The output path for the Parquet file.
    """
    logger.info(f"Saving enriched dataset to {output_path} in Parquet format, partitioned by category and transaction_date")
    enriched_df.write.mode("overwrite").partitionBy("category", "transaction_date").parquet(output_path)
    return None

def save_revenue_insights(revenue_df: DataFrame, output_path: str) -> None:
    """
    Save the store_id-level revenue insights in CSV format.

    Args:
        revenue_df (DataFrame): The revenue DataFrame.
        output_path (str): The output path for the CSV file.
    """
    logger.info(f"Saving store_id-level revenue insights to {output_path} in CSV format")
    # revenue_df.write.mode("overwrite").csv(output_path, header=True)
    revenue_df.write.format("csv").mode("overwrite").option("header", True).save(output_path)
    return None

