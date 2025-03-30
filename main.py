"""
main.py

This script orchestrates the entire data processing pipeline.
It loads the data, processes it, and saves the results.

Steps:
1. Process tables (load data, check missing values and inconsistencies, handle duplicates, enforce data types)
2. Calculate total revenue
3. Calculate monthly sales insights
4. Enrich data
5. Add price category
6. Save enriched data
7. Save revenue insights
"""

from pyspark.sql import SparkSession
from src.utils import load_config, get_logger
from src.data_transformation import calculate_total_revenue, calculate_monthly_sales, enrich_data, add_price_category
from src.data_export import save_enriched_data, save_revenue_insights
from src.pipeline import process_table
import sys

# Initialize Spark session and logger
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()
logger = get_logger(__name__)

# Load configurations (tables and outputs)
config = load_config("config/tables_config.json")
output_config = load_config("config/output_config.json")

def main():
    try:
        # Process tables
        sales_df = process_table(spark, config, "sales")
        products_df = process_table(spark, config, "products")
        stores_df = process_table(spark, config, "stores")

        # Calculate total revenue
        total_revenue_df = calculate_total_revenue(sales_df, products_df)

        # Calculate monthly sales insights
        monthly_sales_df = calculate_monthly_sales(sales_df, products_df)

        # Enrich data
        enriched_df = enrich_data(sales_df, products_df, stores_df)

        # Add price category
        enriched_df_with_price_categorized = add_price_category(enriched_df)

        # Save enriched data
        save_enriched_data(enriched_df, output_config["enriched_data"]["path"])

        # Save revenue insights
        save_revenue_insights(total_revenue_df, output_config["revenue_insights"]["path"])

        logger.info("Data processing pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in data processing pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

    spark.stop()