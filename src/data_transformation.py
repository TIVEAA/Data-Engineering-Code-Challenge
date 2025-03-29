
from pyspark.sql import SparkSession, DataFrame
from utils import load_config, get_logger
from data_preparation import load_data, handle_duplicates, enforce_data_types
from pyspark.sql.functions import col, sum
from pyspark.sql.types import StringType

# Initialize Spark session and logger
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()
logger = get_logger(__name__)

# Load configuration
config = load_config("config/tables_config.json")
    
def calculate_total_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculate the total revenue for each store and each product category.

    Args:
        sales_df (DataFrame): The sales DataFrame.

    Returns:
        DataFrame: DataFrame with store_id, category, and total_revenue.
    """
    logger.info("Calculating total revenue for each store and product category")
    sales_with_category_df = sales_df.join(products_df, "product_id")
    total_revenue_df = sales_with_category_df.groupBy("store_id", "category").agg(sum(col("price") * col("quantity")).alias("total_revenue"))  
    return total_revenue_df


sales_df = load_data(spark, config['sales'])
sales_df = handle_duplicates(sales_df, config['sales'])
sales_df = enforce_data_types(sales_df, config['sales'])

products_df = load_data(spark, config['products'])
products_df = handle_duplicates(products_df, config['products'])
products_df = enforce_data_types(products_df, config['products'])

total_revenue = calculate_total_revenue(sales_df, products_df)
total_revenue.show()



