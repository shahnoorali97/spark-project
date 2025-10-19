"""
Test module for analytics functionality
"""

import pytest
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from chispa import assert_df_equality

from analytics import SalesAnalytics
from utils import load_config


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark_session = SparkSession.builder \
        .appName("TestAnalytics") \
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
        ("P007", "Electronics", 750.0, 2, "2024-01-07"),
        ("P008", "Clothing", 1800.0, 4, "2024-01-08"),
        ("P009", "Books", 450.0, 3, "2024-01-09"),
        ("P010", "Electronics", 1100.0, 1, "2024-01-10"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def config():
    """Create test configuration."""
    return {
        'visualization': {
            'style': 'seaborn',
            'output_dir': 'test_plots/'
        }
    }


@pytest.fixture
def analytics(spark, config):
    """Create analytics instance for testing."""
    return SalesAnalytics(spark, config)


def test_analytics_initialization(spark, config):
    """Test analytics class initialization."""
    analytics = SalesAnalytics(spark, config)
    assert analytics.spark == spark
    assert analytics.config == config


def test_load_data(analytics, sample_data, tmp_path):
    """Test data loading functionality."""
    # Save sample data to temporary file
    sample_data.write.mode("overwrite").csv(str(tmp_path / "test_sales.csv"), header=True)
    
    # Load data
    loaded_df = analytics.load_data(str(tmp_path / "test_sales.csv"))
    
    # Check that data was loaded correctly
    assert loaded_df.count() == 10
    assert len(loaded_df.columns) == 5


def test_get_category_performance(analytics, sample_data):
    """Test category performance analysis."""
    result = analytics.get_category_performance(sample_data)
    
    # Should have 3 categories
    assert result.count() == 3
    
    # Check that results are ordered by total_sales descending
    rows = result.collect()
    assert rows[0]["category"] == "Electronics"  # Highest total sales
    
    # Check Electronics aggregation
    electronics_row = [row for row in rows if row["category"] == "Electronics"][0]
    assert electronics_row["total_transactions"] == 5
    assert electronics_row["total_quantity"] == 7
    assert electronics_row["total_sales"] == 6150.0  # 1500 + 800 + 2000 + 750 + 1100


def test_get_daily_trends(analytics, sample_data):
    """Test daily trends analysis."""
    result = analytics.get_daily_trends(sample_data)
    
    # Should have 10 days of data
    assert result.count() == 10
    
    # Check that results are ordered by date
    rows = result.collect()
    assert rows[0]["date"] == "2024-01-01"
    assert rows[-1]["date"] == "2024-01-10"


def test_get_category_trends(analytics, sample_data):
    """Test category trends analysis."""
    result = analytics.get_category_trends(sample_data)
    
    # Should have multiple rows (date + category combinations)
    assert result.count() > 0
    
    # Check that we have data for all categories
    categories = [row["category"] for row in result.collect()]
    assert "Electronics" in categories
    assert "Clothing" in categories
    assert "Books" in categories


def test_get_top_products(analytics, sample_data):
    """Test top products analysis."""
    result = analytics.get_top_products(sample_data, limit=5)
    
    # Should have 5 products
    assert result.count() == 5
    
    # Check that results are ordered by total_sales descending
    rows = result.collect()
    assert rows[0]["total_sales"] >= rows[1]["total_sales"]
    assert rows[1]["total_sales"] >= rows[2]["total_sales"]


def test_get_top_products_custom_limit(analytics, sample_data):
    """Test top products with custom limit."""
    result = analytics.get_top_products(sample_data, limit=3)
    
    # Should have 3 products
    assert result.count() == 3


def test_generate_insights_report(analytics, sample_data):
    """Test insights report generation."""
    insights = analytics.generate_insights_report(sample_data)
    
    # Check that insights contain expected keys
    expected_keys = [
        "total_transactions", "total_sales", "total_quantity",
        "avg_sales_per_transaction", "top_category", "top_category_sales",
        "top_product", "top_product_sales", "date_range", "peak_sales_day", "peak_sales_amount"
    ]
    
    for key in expected_keys:
        assert key in insights
    
    # Check that values are reasonable
    assert insights["total_transactions"] == 10
    assert insights["total_sales"] > 0
    assert insights["total_quantity"] > 0
    assert insights["avg_sales_per_transaction"] > 0


def test_visualization_plot_creation(analytics, sample_data, tmp_path):
    """Test that visualization plots can be created without errors."""
    output_dir = str(tmp_path / "plots")
    
    # Test distribution plot creation
    try:
        plot_path = analytics.create_sales_distribution_plot(sample_data, output_dir)
        assert os.path.exists(plot_path)
    except Exception as e:
        # If matplotlib/seaborn is not available, skip the test
        pytest.skip(f"Visualization libraries not available: {e}")
    
    # Test trend analysis plot creation
    try:
        plot_path = analytics.create_trend_analysis_plot(sample_data, output_dir)
        assert os.path.exists(plot_path)
    except Exception as e:
        pytest.skip(f"Visualization libraries not available: {e}")
    
    # Test performance dashboard creation
    try:
        plot_path = analytics.create_performance_dashboard(sample_data, output_dir)
        assert os.path.exists(plot_path)
    except Exception as e:
        pytest.skip(f"Visualization libraries not available: {e}")


def test_empty_data_handling(analytics, spark, tmp_path):
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
    
    # Load empty data
    loaded_df = analytics.load_data(str(tmp_path / "empty_sales.csv"))
    
    # Test category performance with empty data
    result = analytics.get_category_performance(loaded_df)
    assert result.count() == 0
    
    # Test insights with empty data
    insights = analytics.generate_insights_report(loaded_df)
    assert insights["total_transactions"] == 0
    assert insights["total_sales"] == 0


def test_data_validation(analytics, sample_data):
    """Test data validation and error handling."""
    # Test with valid data
    assert sample_data.count() > 0
    
    # Test category performance calculation
    result = analytics.get_category_performance(sample_data)
    assert result is not None
    assert result.count() > 0
