"""
Spark Application for Data Processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def create_spark_session(app_name="SparkApp"):
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def process_sales_data(spark, data_path):
    """
    Process sales data and return aggregated results.
    
    Args:
        spark: SparkSession instance
        data_path: Path to the data file
        
    Returns:
        DataFrame with aggregated sales data
    """
    # Define schema for sales data
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("date", StringType(), True)
    ])
    
    # Read the data
    df = spark.read.csv(data_path, header=True, schema=schema)
    
    # Perform aggregations
    aggregated_df = df.groupBy("category") \
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("sales_amount").alias("total_sales"),
            avg("sales_amount").alias("avg_sales_amount")
        ) \
        .orderBy(col("total_sales").desc())
    
    return aggregated_df


def filter_high_value_sales(spark, data_path, threshold=1000.0):
    """
    Filter sales data for high-value transactions.
    
    Args:
        spark: SparkSession instance
        data_path: Path to the data file
        threshold: Minimum sales amount threshold
        
    Returns:
        DataFrame with filtered high-value sales
    """
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("date", StringType(), True)
    ])
    
    df = spark.read.csv(data_path, header=True, schema=schema)
    
    # Filter for high-value sales
    high_value_df = df.filter(col("sales_amount") >= threshold) \
        .orderBy(col("sales_amount").desc())
    
    return high_value_df


def main():
    """Main function to run the Spark application."""
    spark = create_spark_session("SalesDataProcessor")
    
    try:
        # Process sales data
        print("Processing sales data...")
        aggregated_data = process_sales_data(spark, "data/sales_data.csv")
        
        print("Sales Summary by Category:")
        aggregated_data.show()
        
        # Filter high-value sales
        print("\nHigh-Value Sales (>= $1000):")
        high_value_sales = filter_high_value_sales(spark, "data/sales_data.csv")
        high_value_sales.show()
        
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
