"""
Command Line Interface for Spark Sales Analytics
"""

import argparse
import yaml
import logging
import os
from datetime import datetime
from spark_app import create_spark_session
from analytics import SalesAnalytics


def setup_logging(config):
    """Setup logging configuration."""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    log_file = log_config.get('file', 'logs/spark_app.log')
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def load_config(config_path="config.yaml"):
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Configuration file {config_path} not found. Using default settings.")
        return {}


def export_data(df, output_path, format_type="csv"):
    """Export DataFrame to various formats."""
    os.makedirs(output_path, exist_ok=True)
    
    if format_type.lower() == "csv":
        df.write.mode("overwrite").csv(f"{output_path}/sales_data", header=True)
    elif format_type.lower() == "json":
        df.write.mode("overwrite").json(f"{output_path}/sales_data")
    elif format_type.lower() == "parquet":
        df.write.mode("overwrite").parquet(f"{output_path}/sales_data")
    else:
        raise ValueError(f"Unsupported format: {format_type}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Advanced Sales Data Analytics with Apache Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 cli.py --analyze                    # Run full analysis
  python3 cli.py --visualize                  # Generate visualizations
  python3 cli.py --export json               # Export data as JSON
  python3 cli.py --threshold 1500            # Filter high-value sales
  python3 cli.py --config custom_config.yaml # Use custom config
        """
    )
    
    parser.add_argument('--config', '-c', default='config.yaml',
                       help='Configuration file path (default: config.yaml)')
    parser.add_argument('--data', '-d', default='data/sales_data.csv',
                       help='Input data file path (default: data/sales_data.csv)')
    parser.add_argument('--output', '-o', default='output/',
                       help='Output directory (default: output/)')
    parser.add_argument('--plots', '-p', default='plots/',
                       help='Plots output directory (default: plots/)')
    
    # Analysis options
    parser.add_argument('--analyze', '-a', action='store_true',
                       help='Run comprehensive analysis')
    parser.add_argument('--visualize', '-v', action='store_true',
                       help='Generate data visualizations')
    parser.add_argument('--export', choices=['csv', 'json', 'parquet'],
                       help='Export processed data in specified format')
    parser.add_argument('--threshold', type=float, default=1000.0,
                       help='High-value sales threshold (default: 1000.0)')
    parser.add_argument('--top-products', type=int, default=10,
                       help='Number of top products to show (default: 10)')
    
    # Visualization options
    parser.add_argument('--distribution', action='store_true',
                       help='Generate distribution plots')
    parser.add_argument('--trends', action='store_true',
                       help='Generate trend analysis plots')
    parser.add_argument('--dashboard', action='store_true',
                       help='Generate performance dashboard')
    
    # Output options
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress output messages')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Setup logging
    logger = setup_logging(config)
    
    if not args.quiet:
        print("🚀 Starting Spark Sales Analytics...")
        print(f"📊 Data source: {args.data}")
        print(f"📁 Output directory: {args.output}")
        print(f"🎨 Plots directory: {args.plots}")
        print("-" * 50)
    
    # Create Spark session
    spark_config = config.get('spark', {})
    spark = create_spark_session(
        spark_config.get('app_name', 'SalesAnalytics')
    )
    
    try:
        # Initialize analytics
        analytics = SalesAnalytics(spark, config)
        
        # Load data
        logger.info(f"Loading data from {args.data}")
        df = analytics.load_data(args.data)
        
        if not args.quiet:
            print(f"✅ Loaded {df.count()} records")
        
        # Run analysis if requested
        if args.analyze or not any([args.visualize, args.export, args.distribution, args.trends, args.dashboard]):
            logger.info("Running comprehensive analysis...")
            
            # Category performance
            category_perf = analytics.get_category_performance(df)
            print("\n📈 Category Performance:")
            category_perf.show()
            
            # Top products
            top_products = analytics.get_top_products(df, args.top_products)
            print(f"\n🏆 Top {args.top_products} Products:")
            top_products.show()
            
            # High-value sales
            high_value = df.filter(df.sales_amount >= args.threshold)
            print(f"\n💰 High-Value Sales (≥ ${args.threshold}):")
            high_value.show()
            
            # Generate insights
            insights = analytics.generate_insights_report(df)
            print("\n📊 Key Insights:")
            print(f"   • Total Transactions: {insights['total_transactions']}")
            print(f"   • Total Sales: ${insights['total_sales']:,.2f}")
            print(f"   • Average Sales per Transaction: ${insights['avg_sales_per_transaction']:,.2f}")
            print(f"   • Top Category: {insights['top_category']} (${insights['top_category_sales']:,.2f})")
            print(f"   • Top Product: {insights['top_product']} (${insights['top_product_sales']:,.2f})")
            print(f"   • Peak Sales Day: {insights['peak_sales_day']} (${insights['peak_sales_amount']:,.2f})")
        
        # Generate visualizations
        if args.visualize or args.distribution or args.trends or args.dashboard:
            logger.info("Generating visualizations...")
            
            if args.distribution or args.visualize:
                analytics.create_sales_distribution_plot(df, args.plots)
                if not args.quiet:
                    print("✅ Generated distribution plots")
            
            if args.trends or args.visualize:
                analytics.create_trend_analysis_plot(df, args.plots)
                if not args.quiet:
                    print("✅ Generated trend analysis plots")
            
            if args.dashboard or args.visualize:
                analytics.create_performance_dashboard(df, args.plots)
                if not args.quiet:
                    print("✅ Generated performance dashboard")
        
        # Export data
        if args.export:
            logger.info(f"Exporting data as {args.export.upper()}...")
            export_data(df, args.output, args.export)
            if not args.quiet:
                print(f"✅ Data exported to {args.output}/sales_data.{args.export}")
        
        if not args.quiet:
            print("\n🎉 Analysis completed successfully!")
            print(f"📁 Check {args.plots} for visualizations")
            print(f"📁 Check {args.output} for exported data")
    
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        print(f"❌ Error: {e}")
        return 1
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")
    
    return 0


if __name__ == "__main__":
    exit(main())
