from utils import load_config, get_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, when, count, length, to_date, udf, to_timestamp
from dateutil import parser

spark = SparkSession.builder.appName("DataPreparation").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
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
    """
    Handle duplicates in the DataFrame by dropping rows with duplicate primary keys.

    Args:
        df (DataFrame): The DataFrame to process.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The DataFrame with duplicates removed.
    """
    logger.info(f"Handling duplicates in table {config['table_name']}")
    initial_count = df.count()
    df = df.drop_duplicates(subset=config['primary_keys'])
    final_count = df.count()
    duplicates_dropped = initial_count - final_count
    logger.info(f"Dropped {duplicates_dropped} duplicates rows from table {config['table_name']}")
    return df

def detect_date_format(date_str: str) -> str:
    """
    Detects and parses the date string into a standard format.

    Args:
        date_str (str): The date string to parse.

    Returns:
        str: The parsed date in ISO format or None if parsing fails.
    """
    try:
        return parser.parse(date_str).isoformat()
    except ValueError:
        return None

def enforce_constraints(df: DataFrame, column: str, constraints: dict) -> DataFrame:
    """
    Enforces constraints for a specific column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to process.
        column (str): The column to enforce constraints on.
        constraints (dict): The constraints dictionary for the column.

    Returns:
        DataFrame: The DataFrame with enforced constraints.
    """
    if 'not_null' in constraints and constraints['not_null']:
        logger.info(f"Enforcing not_null constraint on column {column}")
        df = df.filter(col(column).isNotNull())
    if 'not_empty' in constraints and constraints['not_empty']:
        logger.info(f"Enforcing not_empty constraint on column {column}")
        df = df.filter(col(column) != "")
    if 'min' in constraints:
        logger.info(f"Enforcing min constraint on column {column}: {constraints['min']}")
        df = df.filter(col(column) >= constraints['min'])
    return df

def enforce_data_types(df: DataFrame, config: dict) -> DataFrame:
    """
    Enforces data types and constraints for each column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to process.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The DataFrame with enforced data types and constraints.
    """
    logger.info(f"Enforcing data types for table {config['table_name']}")
    for column, metadata in config['columns'].items():
        data_type = metadata['type']
        logger.info(f"Enforcing data type for column {column}: {data_type}")
        if data_type == 'date':
            logger.info(f"Detecting and converting date formats for column {column}")
            df = df.withColumn(column, detect_date_format_udf(col(column)))
            date_format = str(metadata['format'])
            df = df.withColumn(column, to_date(col(column), date_format))
        else:
            df = df.withColumn(column, col(column).cast(data_type))

        if 'constraints' in metadata:
            df = enforce_constraints(df, column, metadata['constraints'])
    return df

# sales_df = load_data(spark, config['sales'])
# products_df = load_data(spark, config['products'])
# stores_df = load_data(spark, config['stores'])

# sales_df = handle_duplicates(sales_df, config['sales'])
# detect_date_format_udf = udf(detect_date_format, StringType())
# sales_df = enforce_data_types(sales_df, config['sales'])
# sales_df.printSchema()
# sales_df.show()
# print(sales_df.count())

spark.stop()
