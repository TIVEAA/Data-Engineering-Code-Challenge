# Data Engineering Challenge - Source Code

This directory contains the source code for the data engineering challenge. The code is organized into several modules, each responsible for different aspects of the data processing pipeline.

## Modules

### `data_preparation.py`

This module is responsible for preparing the data for the challenge. It includes functions to load data, check for missing values, check for format inconsistencies, handle duplicates, and enforce data types and constraints.

#### Functions:
- `load_data(spark: SparkSession, config: dict) -> DataFrame`: Load data from the specified path in the configuration file.
- `check_missing_values(df: DataFrame, config: dict) -> DataFrame`: Check for missing values in the DataFrame.
- `check_format_inconsistencies(df: DataFrame, config: dict) -> DataFrame`: Check for inconsistencies in the data format.
- `handle_duplicates(df: DataFrame, config: dict) -> DataFrame`: Handle duplicates in the DataFrame by dropping rows with duplicate primary keys.
- `enforce_constraints(df: DataFrame, column: str, constraints: dict) -> DataFrame`: Enforce constraints for a specific column in the DataFrame.
- `enforce_data_types(df: DataFrame, config: dict) -> DataFrame`: Enforce data types and constraints for each column in the DataFrame.

### `data_transformation.py`

This module is responsible for transforming the data for the challenge. It includes functions to calculate total revenue, calculate monthly sales insights, enrich data by combining multiple datasets, and categorize products based on price ranges.

#### Functions:
- `calculate_total_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame`: Calculate the total revenue for each store and each product category.
- `calculate_monthly_sales(sales_df: DataFrame, products_df: DataFrame) -> DataFrame`: Calculate the total quantity sold for each product category, grouped by month.
- `enrich_data(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame`: Combine the sales, products, and stores datasets into a single enriched dataset.
- `categorized_price(price: float) -> str`: Categorize products based on price ranges.
- `add_price_category(enriched_df: DataFrame) -> DataFrame`: Add a price category column to the enriched dataset.

### `data_export.py`

This module is responsible for exporting the transformed data for the challenge. It includes functions to save the enriched dataset in Parquet format and the store_id-level revenue insights in CSV format.

#### Functions:
- `save_enriched_data(enriched_df: DataFrame, output_path: str) -> None`: Save the enriched dataset in Parquet format, partitioned by category and transaction_date.
- `save_revenue_insights(revenue_df: DataFrame, output_path: str) -> None`: Save the store_id-level revenue insights in CSV format.

### `pipeline.py`

This module contains functions for processing tables in the data pipeline. It includes functions for loading data, checking for missing values and inconsistencies, handling duplicates, and enforcing data types.

#### Functions:
- `process_table(spark: SparkSession, config: dict, table_name: str) -> DataFrame`: Process a table by loading data, checking for missing values and inconsistencies, handling duplicates, and enforcing data types.

### `utils.py`

This module contains utility functions for the data pipeline. It includes functions to load configuration files and set up logging.

#### Functions:
- `load_config(config_path: str) -> dict`: Load configuration from a JSON file.
- `get_logger(name: str) -> logging.Logger`: Get a logger with the specified name.