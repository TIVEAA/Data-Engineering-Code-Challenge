import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.data_transformation import calculate_total_revenue, calculate_monthly_sales, enrich_data, add_price_category
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()

def test_calculate_total_revenue(spark):
    sales_data = [("1", "1", 2, 10.0), ("2", "1", 3, 20.0)]
    products_data = [("1", "Category1"), ("2", "Category2")]
    sales_df = spark.createDataFrame(sales_data, ["product_id", "store_id", "quantity", "price"])
    products_df = spark.createDataFrame(products_data, ["product_id", "category"])
    total_revenue_df = calculate_total_revenue(sales_df, products_df)
    
    expected_data = [("1", "Category1", 20.0), ("1", "Category2", 60.0)]
    expected_df = spark.createDataFrame(expected_data, ["store_id", "category", "total_revenue"])

    total_revenue_df = total_revenue_df.orderBy("store_id", "category")
    expected_df = expected_df.orderBy("store_id", "category")
    
    assert_df_equality(total_revenue_df, expected_df, ignore_nullable=True)

def test_calculate_monthly_sales(spark):
    sales_data = [("1", "2024-07-04", 2), ("2", "2024-07-04", 3)]
    products_data = [("1", "Category1"), ("2", "Category2")]
    sales_df = spark.createDataFrame(sales_data, ["product_id", "transaction_date", "quantity"])
    products_df = spark.createDataFrame(products_data, ["product_id", "category"])
    monthly_sales_df = calculate_monthly_sales(sales_df, products_df)
    
    expected_data = [(2024, 7, "Category1", 2), (2024, 7, "Category2", 3)]
    expected_df = spark.createDataFrame(expected_data, ["year", "month", "category", "total_quantity_sold"])
    expected_df = expected_df.withColumn("year", col("year").cast(IntegerType()))
    expected_df = expected_df.withColumn("month", col("month").cast(IntegerType()))
    
    monthly_sales_df = monthly_sales_df.orderBy("year", "month", "category")
    expected_df = expected_df.orderBy("year", "month", "category")
    
    assert_df_equality(monthly_sales_df, expected_df, ignore_nullable=True)

def test_enrich_data(spark):
    sales_data = [("1", "1", "1", "2024-07-04", 2, 10.0)]
    products_data = [("1", "Product1", "Category1")]
    stores_data = [("1", "Store1", "Location1")]

    sales_df = spark.createDataFrame(sales_data, ["transaction_id", "product_id", "store_id", "transaction_date", "quantity", "price"])
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category"])
    stores_df = spark.createDataFrame(stores_data, ["store_id", "store_name", "location"])

    enriched_df = enrich_data(sales_df, products_df, stores_df)
    expected_data = [("1", "Store1", "Location1", "Product1", "Category1", 2, "2024-07-04", 10.0)]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "store_name", "location", "product_name", "category", "quantity", "transaction_date", "price"])

    assert_df_equality(enriched_df, expected_df, ignore_nullable=True)

def test_add_price_category(spark):
    data = [("1", "Store1", "Location1", "Product1", "Category1", 2, "2024-07-04", 10.0)]

    enriched_df = spark.createDataFrame(data, ["transaction_id", "store_name", "location", "product_name", "category", "quantity", "transaction_date", "price"])
    enriched_df = add_price_category(enriched_df)

    expected_data = [("1", "Store1", "Location1", "Product1", "Category1", 2, "2024-07-04", 10.0, "Low")]
    expected_df = spark.createDataFrame(expected_data, ["transaction_id", "store_name", "location", "product_name", "category", "quantity", "transaction_date", "price", "price_category"])

    assert_df_equality(enriched_df, expected_df, ignore_nullable=True)