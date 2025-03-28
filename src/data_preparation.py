from utils import load_config, get_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, length, to_date

spark = SparkSession.builder.appName("DataPreparation").getOrCreate()
logger = get_logger(__name__)

config = load_config("config/tables_config.json")

def load_data(spark: SparkSession, config: dict) -> DataFrame:
    """
    Load data from the specified path in the configuration file.

    Args:
        spark (SparkSession): The Spark session.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The loaded DataFrame.
    """
    logger.info(f"Loading data from {config['path']}")
    return spark.read.format(config['format']).option("header", "true").load(config['path'])

def check_missing_values(df: DataFrame, config: dict) -> DataFrame:
    """
    Check for missing values in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to check.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: A DataFrame showing the count of missing values for each column.
    """
    logger.info(f"Checking for missing values in table {config['table_name']}")
    missing_values = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    logger.info(f"Missing values:\n{missing_values.collect()}")
    return missing_values

def check_format_inconsistencies(df: DataFrame, config: dict) -> DataFrame:
    """
    Check for inconsistencies in the data format.

    Args:
        df (DataFrame): The DataFrame to check.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: A DataFrame showing the count of format inconsistencies for each column.
    """
    logger.info(f"Checking for format inconsistencies in table {config['table_name']}")
    format_inconsistencies = []
    for column, metadata in config['columns'].items():
        if metadata['type'] == 'string':
            logger.info(f"Checking for empty strings in column {column}")
            format_inconsistencies.append(count(when(length(col(column)) == 0, column)).alias(column))
        elif metadata['type'] == 'date':
            logger.info(f"Checking for invalid dates in column {column}")
            date_format = metadata['format']
            format_inconsistencies.append(count(when(to_date(col(column), date_format).isNull(), column)).alias(column))
        elif metadata['type'] == 'integer':
            logger.info(f"Checking for invalid integers in column {column}")
            format_inconsistencies.append(count(when(col(column).cast('integer').isNull(), column)).alias(column))
        elif metadata['type'] == 'float':
            logger.info(f"Checking for invalid floats in column {column}")
            format_inconsistencies.append(count(when(col(column).cast('float').isNull(), column)).alias(column))
    inconsistencies_df = df.select(format_inconsistencies)
    logger.info(f"Format inconsistencies:\n{inconsistencies_df.collect()}")
    return inconsistencies_df

def handle_duplicates(df: DataFrame, config: dict) -> DataFrame:
    logger.info(f"Handling duplicates in table {config['table_name']}")
    initial_count = df.count()
    df = df.drop_duplicates(subset=config['primary_keys'])
    final_count = df.count()
    duplicates_dropped = initial_count - final_count
    logger.info(f"Dropped {duplicates_dropped} duplicates rows from table {config['table_name']}")
    return df
        
sales_df = load_data(spark, config['sales'])
products_df = load_data(spark, config['products'])
stores_df = load_data(spark, config['stores'])

# products_df.show()

missing_values = check_missing_values(sales_df, config['sales'])
missing_values.show()

format_inconsistencies = check_format_inconsistencies(sales_df, config['sales'])
format_inconsistencies.show()

sales_df = handle_duplicates(sales_df, config['sales'])

spark.stop()
