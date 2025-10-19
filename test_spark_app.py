"""
Test module for Spark application
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from chispa import assert_df_equality

from spark_app import create_spark_session, process_sales_data, filter_high_value_sales


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark_session = SparkSession.builder \
        .appName("TestApp") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_data(spark):
    """Create sample data for testing."""
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("date", StringType(), True)
    ])
    
    data = [
        ("P001", "Electronics", 1500.0, 2, "2024-01-01"),
        ("P002", "Electronics", 800.0, 1, "2024-01-02"),
        ("P003", "Clothing", 1200.0, 3, "2024-01-03"),
        ("P004", "Clothing", 500.0, 1, "2024-01-04"),
        ("P005", "Electronics", 2000.0, 1, "2024-01-05"),
        ("P006", "Books", 300.0, 5, "2024-01-06"),
    ]
    
    return spark.createDataFrame(data, schema)


def test_create_spark_session():
    """Test Spark session creation."""
    spark = create_spark_session("TestSession")
    assert spark is not None
    assert spark.conf.get("spark.app.name") == "TestSession"
    spark.stop()


def test_process_sales_data(spark, sample_data, tmp_path):
    """Test sales data processing function."""
    # Save sample data to temporary file
    sample_data.write.mode("overwrite").csv(str(tmp_path / "test_sales.csv"), header=True)
    
    # Process the data
    result = process_sales_data(spark, str(tmp_path / "test_sales.csv"))
    
    # Check that we get the expected number of categories
    assert result.count() == 3  # Electronics, Clothing, Books
    
    # Check that results are ordered by total_sales descending
    rows = result.collect()
    assert rows[0]["category"] == "Electronics"  # Highest total sales
    assert rows[1]["category"] == "Clothing"
    assert rows[2]["category"] == "Books"  # Lowest total sales
    
    # Check Electronics aggregation
    electronics_row = [row for row in rows if row["category"] == "Electronics"][0]
    assert electronics_row["total_transactions"] == 3
    assert electronics_row["total_quantity"] == 4
    assert electronics_row["total_sales"] == 4300.0  # 1500 + 800 + 2000


def test_filter_high_value_sales(spark, sample_data, tmp_path):
    """Test high-value sales filtering function."""
    # Save sample data to temporary file
    sample_data.write.mode("overwrite").csv(str(tmp_path / "test_sales.csv"), header=True)
    
    # Filter high-value sales
    result = filter_high_value_sales(spark, str(tmp_path / "test_sales.csv"), threshold=1000.0)
    
    # Should have 3 high-value transactions
    assert result.count() == 3
    
    # Check that all results are >= threshold
    rows = result.collect()
    for row in rows:
        assert row["sales_amount"] >= 1000.0
    
    # Check that results are ordered by sales_amount descending
    assert rows[0]["sales_amount"] == 2000.0  # Highest
    assert rows[1]["sales_amount"] == 1500.0
    assert rows[2]["sales_amount"] == 1200.0  # Lowest


def test_filter_high_value_sales_custom_threshold(spark, sample_data, tmp_path):
    """Test high-value sales filtering with custom threshold."""
    # Save sample data to temporary file
    sample_data.write.mode("overwrite").csv(str(tmp_path / "test_sales.csv"), header=True)
    
    # Filter with custom threshold
    result = filter_high_value_sales(spark, str(tmp_path / "test_sales.csv"), threshold=1500.0)
    
    # Should have 2 transactions >= 1500
    assert result.count() == 2
    
    rows = result.collect()
    assert rows[0]["sales_amount"] == 2000.0
    assert rows[1]["sales_amount"] == 1500.0


def test_empty_data_handling(spark, tmp_path):
    """Test handling of empty data."""
    # Create empty DataFrame
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("date", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.mode("overwrite").csv(str(tmp_path / "empty_sales.csv"), header=True)
    
    # Process empty data
    result = process_sales_data(spark, str(tmp_path / "empty_sales.csv"))
    
    # Should return empty result
    assert result.count() == 0
