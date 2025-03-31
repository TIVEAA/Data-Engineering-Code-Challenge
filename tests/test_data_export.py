import os
import shutil
import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import LongType, DoubleType, StringType
from src.data_export import save_enriched_data, save_revenue_insights

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()

@pytest.fixture(scope="function", autouse=True)
def clean_output_folder():
    # Setup: Ensure the output folder is clean before each test
    test_output_path = "test_output"
    if os.path.exists(test_output_path):
        shutil.rmtree(test_output_path)
    os.makedirs(test_output_path)
    
    yield
    
    # Teardown: Clean up the output folder after each test
    if os.path.exists(test_output_path):
        shutil.rmtree(test_output_path)

def test_save_enriched_data(spark: SparkSession):
    enriched_data = [("1", "Store1", "Location1", "Product1", "Category1", 2, "2024-07-04", 10.0)]
    enriched_df = spark.createDataFrame(enriched_data, ["transaction_id", "store_name", "location", "product_name", "category", "quantity", "transaction_date", "price"])
    
    output_path = "test_output/enriched_data"
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    save_enriched_data(enriched_df, output_path)

    saved_df = spark.read.parquet(output_path)
    
    expected_data = [("1", "Store1", "Location1", "Product1", 2, 10.0, "Category1", "2024-07-04")]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "store_name", "location", "product_name", "quantity", "price", "category", "transaction_date"])
    expected_df = expected_df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    expected_df = expected_df.withColumn("quantity", col("quantity").cast(LongType()))
    expected_df = expected_df.withColumn("price", col("price").cast(DoubleType()))
    
    saved_df = saved_df.orderBy("transaction_id")
    expected_df = expected_df.orderBy("transaction_id")
    
    assert_df_equality(saved_df, expected_df, ignore_nullable=True)

def test_save_revenue_insights(spark: SparkSession):
    revenue_data = [("1", "Category1", 100.0), ("2", "Category2", 200.0)]
    revenue_df = spark.createDataFrame(revenue_data, ["store_id", "category", "total_revenue"])
    
    output_path = "test_output/revenue_insights"
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    save_revenue_insights(revenue_df, output_path)
    
    saved_df = spark.read.format("csv").option("header", True).load(output_path)
    
    expected_data = [("1", "Category1", 100.0), ("2", "Category2", 200.0)]
    expected_df = spark.createDataFrame(expected_data, ["store_id", "category", "total_revenue"])
    expected_df = expected_df.withColumn("total_revenue", col("total_revenue").cast(StringType()))
    
    saved_df = saved_df.orderBy("store_id")
    expected_df = expected_df.orderBy("store_id")
    
    assert_df_equality(saved_df, expected_df, ignore_nullable=True)