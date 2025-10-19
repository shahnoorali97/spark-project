"""
Advanced Analytics Module for Sales Data
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, year, month, dayofmonth, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
from datetime import datetime


class SalesAnalytics:
    """Advanced analytics class for sales data processing and visualization."""
    
    def __init__(self, spark_session, config):
        self.spark = spark_session
        self.config = config
        self.setup_plotting()
    
    def setup_plotting(self):
        """Setup matplotlib and seaborn for plotting."""
        try:
            plt.style.use('seaborn-v0_8')
        except OSError:
            # Fallback to default style if seaborn style is not available
            plt.style.use('default')
        sns.set_palette("husl")
        
    def load_data(self, data_path):
        """Load sales data with proper schema."""
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sales_amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("date", StringType(), True)
        ])
        
        df = self.spark.read.csv(data_path, header=True, schema=schema)
        
        # Convert date string to date type
        df = df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
        
        return df
    
    def get_category_performance(self, df):
        """Get detailed category performance metrics."""
        return df.groupBy("category") \
            .agg(
                count("*").alias("total_transactions"),
                spark_sum("quantity").alias("total_quantity"),
                spark_sum("sales_amount").alias("total_sales"),
                avg("sales_amount").alias("avg_sales_amount"),
                avg("quantity").alias("avg_quantity_per_transaction")
            ) \
            .orderBy(col("total_sales").desc())
    
    def get_daily_trends(self, df):
        """Analyze daily sales trends."""
        return df.groupBy("date") \
            .agg(
                count("*").alias("daily_transactions"),
                spark_sum("quantity").alias("daily_quantity"),
                spark_sum("sales_amount").alias("daily_sales")
            ) \
            .orderBy("date")
    
    def get_category_trends(self, df):
        """Analyze sales trends by category over time."""
        return df.groupBy("date", "category") \
            .agg(
                count("*").alias("transactions"),
                spark_sum("sales_amount").alias("sales")
            ) \
            .orderBy("date", "category")
    
    def get_top_products(self, df, limit=10):
        """Get top performing products by sales amount."""
        return df.groupBy("product_id", "category") \
            .agg(
                count("*").alias("transactions"),
                spark_sum("quantity").alias("total_quantity"),
                spark_sum("sales_amount").alias("total_sales"),
                avg("sales_amount").alias("avg_sales")
            ) \
            .orderBy(col("total_sales").desc()) \
            .limit(limit)
    
    def create_sales_distribution_plot(self, df, output_dir="plots/"):
        """Create sales distribution visualization."""
        # Convert to Pandas for plotting
        pandas_df = df.toPandas()
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Sales Data Distribution Analysis', fontsize=16, fontweight='bold')
        
        # Sales amount distribution by category
        sns.boxplot(data=pandas_df, x='category', y='sales_amount', ax=axes[0, 0])
        axes[0, 0].set_title('Sales Amount Distribution by Category')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Quantity distribution
        sns.histplot(data=pandas_df, x='quantity', bins=20, ax=axes[0, 1])
        axes[0, 1].set_title('Quantity Distribution')
        
        # Sales amount histogram
        sns.histplot(data=pandas_df, x='sales_amount', bins=20, ax=axes[1, 0])
        axes[1, 0].set_title('Sales Amount Distribution')
        
        # Category sales pie chart
        category_sales = pandas_df.groupby('category')['sales_amount'].sum()
        axes[1, 1].pie(category_sales.values, labels=category_sales.index, autopct='%1.1f%%')
        axes[1, 1].set_title('Sales by Category (Pie Chart)')
        
        plt.tight_layout()
        
        # Save plot
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f"{output_dir}/sales_distribution.png", dpi=300, bbox_inches='tight')
        plt.savefig(f"{output_dir}/sales_distribution.pdf", bbox_inches='tight')
        plt.show()
        
        return f"{output_dir}/sales_distribution.png"
    
    def create_trend_analysis_plot(self, df, output_dir="plots/"):
        """Create trend analysis visualization."""
        # Get daily trends
        daily_trends = self.get_daily_trends(df)
        daily_pandas = daily_trends.toPandas()
        daily_pandas['date'] = pd.to_datetime(daily_pandas['date'])
        
        # Get category trends
        category_trends = self.get_category_trends(df)
        category_pandas = category_trends.toPandas()
        category_pandas['date'] = pd.to_datetime(category_pandas['date'])
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Sales Trend Analysis', fontsize=16, fontweight='bold')
        
        # Daily sales trend
        axes[0, 0].plot(daily_pandas['date'], daily_pandas['daily_sales'], marker='o')
        axes[0, 0].set_title('Daily Sales Trend')
        axes[0, 0].set_xlabel('Date')
        axes[0, 0].set_ylabel('Sales Amount')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Daily transactions trend
        axes[0, 1].plot(daily_pandas['date'], daily_pandas['daily_transactions'], marker='s', color='orange')
        axes[0, 1].set_title('Daily Transactions Trend')
        axes[0, 1].set_xlabel('Date')
        axes[0, 1].set_ylabel('Number of Transactions')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Category sales over time
        for category in category_pandas['category'].unique():
            cat_data = category_pandas[category_pandas['category'] == category]
            axes[1, 0].plot(cat_data['date'], cat_data['sales'], marker='o', label=category)
        axes[1, 0].set_title('Sales by Category Over Time')
        axes[1, 0].set_xlabel('Date')
        axes[1, 0].set_ylabel('Sales Amount')
        axes[1, 0].legend()
        axes[1, 0].tick_params(axis='x', rotation=45)
        
        # Category transactions over time
        for category in category_pandas['category'].unique():
            cat_data = category_pandas[category_pandas['category'] == category]
            axes[1, 1].plot(cat_data['date'], cat_data['transactions'], marker='s', label=category)
        axes[1, 1].set_title('Transactions by Category Over Time')
        axes[1, 1].set_xlabel('Date')
        axes[1, 1].set_ylabel('Number of Transactions')
        axes[1, 1].legend()
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f"{output_dir}/trend_analysis.png", dpi=300, bbox_inches='tight')
        plt.savefig(f"{output_dir}/trend_analysis.pdf", bbox_inches='tight')
        plt.show()
        
        return f"{output_dir}/trend_analysis.png"
    
    def create_performance_dashboard(self, df, output_dir="plots/"):
        """Create a comprehensive performance dashboard."""
        # Get performance metrics
        category_perf = self.get_category_performance(df)
        top_products = self.get_top_products(df, 5)
        
        # Convert to Pandas
        category_pandas = category_perf.toPandas()
        products_pandas = top_products.toPandas()
        
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Sales Performance Dashboard', fontsize=16, fontweight='bold')
        
        # Total sales by category (bar chart)
        axes[0, 0].bar(category_pandas['category'], category_pandas['total_sales'])
        axes[0, 0].set_title('Total Sales by Category')
        axes[0, 0].set_ylabel('Total Sales ($)')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # Average sales by category
        axes[0, 1].bar(category_pandas['category'], category_pandas['avg_sales_amount'])
        axes[0, 1].set_title('Average Sales Amount by Category')
        axes[0, 1].set_ylabel('Average Sales ($)')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Total transactions by category
        axes[0, 2].bar(category_pandas['category'], category_pandas['total_transactions'])
        axes[0, 2].set_title('Total Transactions by Category')
        axes[0, 2].set_ylabel('Number of Transactions')
        axes[0, 2].tick_params(axis='x', rotation=45)
        
        # Top products by sales
        axes[1, 0].barh(products_pandas['product_id'], products_pandas['total_sales'])
        axes[1, 0].set_title('Top 5 Products by Sales')
        axes[1, 0].set_xlabel('Total Sales ($)')
        
        # Quantity vs Sales scatter plot
        original_pandas = df.toPandas()
        sns.scatterplot(data=original_pandas, x='quantity', y='sales_amount', 
                       hue='category', ax=axes[1, 1])
        axes[1, 1].set_title('Quantity vs Sales Amount')
        axes[1, 1].set_xlabel('Quantity')
        axes[1, 1].set_ylabel('Sales Amount ($)')
        
        # Average quantity per transaction by category
        axes[1, 2].bar(category_pandas['category'], category_pandas['avg_quantity_per_transaction'])
        axes[1, 2].set_title('Average Quantity per Transaction')
        axes[1, 2].set_ylabel('Average Quantity')
        axes[1, 2].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f"{output_dir}/performance_dashboard.png", dpi=300, bbox_inches='tight')
        plt.savefig(f"{output_dir}/performance_dashboard.pdf", bbox_inches='tight')
        plt.show()
        
        return f"{output_dir}/performance_dashboard.png"
    
    def generate_insights_report(self, df):
        """Generate a comprehensive insights report."""
        category_perf = self.get_category_performance(df)
        top_products = self.get_top_products(df, 5)
        daily_trends = self.get_daily_trends(df)
        
        # Convert to Pandas for analysis
        category_pandas = category_perf.toPandas()
        products_pandas = top_products.toPandas()
        daily_pandas = daily_trends.toPandas()
        
        total_transactions = df.count()
        total_sales = category_pandas['total_sales'].sum() if not category_pandas.empty else 0
        total_quantity = category_pandas['total_quantity'].sum() if not category_pandas.empty else 0
        
        insights = {
            "total_transactions": total_transactions,
            "total_sales": total_sales,
            "total_quantity": total_quantity,
            "avg_sales_per_transaction": total_sales / total_transactions if total_transactions > 0 else 0,
            "top_category": category_pandas.iloc[0]['category'] if not category_pandas.empty else "N/A",
            "top_category_sales": category_pandas.iloc[0]['total_sales'] if not category_pandas.empty else 0,
            "top_product": products_pandas.iloc[0]['product_id'] if not products_pandas.empty else "N/A",
            "top_product_sales": products_pandas.iloc[0]['total_sales'] if not products_pandas.empty else 0,
            "date_range": {
                "start": daily_pandas['date'].min() if not daily_pandas.empty else "N/A",
                "end": daily_pandas['date'].max() if not daily_pandas.empty else "N/A"
            },
            "peak_sales_day": daily_pandas.loc[daily_pandas['daily_sales'].idxmax(), 'date'] if not daily_pandas.empty else "N/A",
            "peak_sales_amount": daily_pandas['daily_sales'].max() if not daily_pandas.empty else 0
        }
        
        return insights
