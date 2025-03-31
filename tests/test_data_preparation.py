import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.data_preparation import load_data, check_missing_values, check_format_inconsistencies, handle_duplicates, enforce_data_types
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()

def test_load_data(spark):
    config = {
        "path": "data/sales_uuid.csv",
        "format": "csv"
    }
    df = load_data(spark, config)
    assert df.count() > 0

def test_check_missing_values(spark):
    data = [("1", "2", None), ("4", None, "6")]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    config = {
        "table_name": "test_table",
        "columns": {
            "col1": {"type": "string"},
            "col2": {"type": "string"},
            "col3": {"type": "string"}
        }
    }
    missing_values_df = check_missing_values(df, config)
    expected_data = [(0, 1, 1)]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2", "col3"])
    assert_df_equality(missing_values_df, expected_df, ignore_nullable=True)

def test_check_format_inconsistencies(spark):
    data = [("2024-07-04", "2024/11/04"), ("2024-03-12", "03-08-2024")]
    df = spark.createDataFrame(data, ["col1", "col2"])
    config = {
        "table_name": "test_table",
        "columns": {
            "col1": {"type": "date", "format": "yyyy-MM-dd"},
            "col2": {"type": "date", "format": "yyyy-MM-dd"}
        }
    }
    inconsistencies_df = check_format_inconsistencies(df, config)
    expected_data = [(0, 2)]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    assert_df_equality(inconsistencies_df, expected_df, ignore_nullable=True)

def test_handle_duplicates(spark):
    data = [("1", "2"), ("1", "2"), ("3", "4")]
    df = spark.createDataFrame(data, ["col1", "col2"])
    config = {
        "table_name": "test_table",
        "primary_keys": ["col1", "col2"]
    }
    df_no_duplicates = handle_duplicates(df, config)
    expected_data = [("1", "2"), ("3", "4")]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    assert_df_equality(df_no_duplicates, expected_df, ignore_nullable=True)

def test_enforce_data_types(spark):
    data = [("1", "2024-07-04"), ("2", "2024-03-12")]
    df = spark.createDataFrame(data, ["col1", "col2"])
    config = {
        "table_name": "test_table",
        "columns": {
            "col1": {"type": "integer"},
            "col2": {"type": "date", "format": "yyyy-MM-dd"}
        }
    }
    df_enforced = enforce_data_types(df, config)
    expected_data = [(1, "2024-07-04"), (2, "2024-03-12")]
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    expected_df = expected_df.withColumn("col1", col("col1").cast(IntegerType()))
    expected_df = expected_df.withColumn("col2", to_date(col("col2"), "yyyy-MM-dd"))
    assert_df_equality(df_enforced, expected_df, ignore_nullable=True)

# def test_enforce_data_types(spark):
#     data = [("1", "2024-07-04"), ("2", "2024-03-12")]
#     df = spark.createDataFrame(data, ["col1", "col2"])
#     config = {
#         "table_name": "test_table",
#         "columns": {
#             "col1": {"type": "integer"},
#             "col2": {"type": "date", "format": "yyyy-MM-dd"}
#         }
#     }
#     df_enforced = enforce_data_types(df, config)
#     expected_data = [(1, "2024-07-04"), (2, "2024-03-12")]
#     expected_schema = StructType([
#         StructField("col1", IntegerType(), True),
#         StructField("col2", DateType(), True)
#     ])
#     expected_df = spark.createDataFrame(expected_data, expected_schema)
#     assert_df_equality(df_enforced, expected_df, ignore_nullable=True)