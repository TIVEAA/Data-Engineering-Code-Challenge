"""
main.py

This script orchestrates the entire data processing pipeline.
It loads the data, processes it, and saves the results.

Steps:
1. Load data
2. Handle duplicates and enforce data types
3. Calculate total revenue
4. Calculate monthly sales insights
5. Enrich data
6. Add price category
7. Save enriched data
8. Save revenue insights
"""

from pyspark.sql import SparkSession
from src.utils import load_config, get_logger
from src.data_preparation import load_data, handle_duplicates, enforce_data_types
from src.data_transformation import calculate_total_revenue, calculate_monthly_sales, enrich_data, add_price_category
from src.data_export import save_enriched_data, save_revenue_insights

# Initialize Spark session and logger
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()
logger = get_logger(__name__)

# Load configuration
config = load_config("config/tables_config.json")

def main():
    # Step 1: Load data
    sales_df = load_data(spark, config["sales"])
    products_df = load_data(spark, config["products"])
    stores_df = load_data(spark, config["stores"])

    # Step 2: Handle duplicates and enforce data types
    sales_df = handle_duplicates(sales_df, config["sales"])
    sales_df = enforce_data_types(sales_df, config["sales"])
    products_df = handle_duplicates(products_df, config["products"])
    products_df = enforce_data_types(products_df, config["products"])
    stores_df = handle_duplicates(stores_df, config["stores"])
    stores_df = enforce_data_types(stores_df, config["stores"])

    # Step 3: Calculate total revenue
    total_revenue_df = calculate_total_revenue(sales_df, products_df)

    # Step 4: Calculate monthly sales insights
    monthly_sales_df = calculate_monthly_sales(sales_df, products_df)

    # Step 5: Enrich data
    enriched_df = enrich_data(sales_df, products_df, stores_df)

    # Step 6: Add price category
    enriched_df = add_price_category(enriched_df)

    # Step 7: Save enriched data
    save_enriched_data(enriched_df, "output/enriched_data")

    # Step 8: Save revenue insights
    save_revenue_insights(total_revenue_df, "output/revenue_insights")

    logger.info("Data processing pipeline completed successfully")

if __name__ == "__main__":
    main()

    # Stop the Spark session
    spark.stop()