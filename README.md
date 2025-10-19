# 🚀 Advanced Spark Sales Analytics

A comprehensive PySpark application for processing and analyzing sales data with advanced analytics, visualizations, and a powerful CLI interface.

## 📁 Project Structure

```
spark-project/
├── data/
│   └── sales_data.csv          # Sample sales data
├── analytics.py               # Advanced analytics and visualization
├── cli.py                     # Command-line interface
├── config.yaml                # Configuration file
├── spark_app.py               # Core Spark application
├── test_spark_app.py          # Core functionality tests
├── test_analytics.py          # Analytics functionality tests
├── utils.py                   # Utility functions
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## ✨ Features

### 🔧 Core Functionality
- **Data Processing**: Aggregates sales data by category with advanced metrics
- **High-Value Filtering**: Filters transactions above specified thresholds
- **Trend Analysis**: Daily and category-wise sales trends
- **Top Products**: Identifies best-performing products
- **Comprehensive Testing**: Unit tests using pytest and chispa

### 📊 Advanced Analytics
- **Performance Metrics**: Detailed category performance analysis
- **Insights Generation**: Automated insights and recommendations
- **Data Export**: Export to CSV, JSON, and Parquet formats
- **Configurable Settings**: YAML-based configuration system

### 🎨 Data Visualization
- **Distribution Plots**: Sales amount and quantity distributions
- **Trend Analysis**: Time-series analysis with multiple charts
- **Performance Dashboard**: Comprehensive performance overview
- **Interactive Charts**: Seaborn and Matplotlib visualizations

### 🖥️ Command-Line Interface
- **Flexible Commands**: Multiple analysis and export options
- **Customizable Parameters**: Thresholds, limits, and output formats
- **Verbose Logging**: Detailed logging and progress tracking
- **Help System**: Comprehensive help and examples

## Installation

1. Install dependencies:
```bash
pip3 install -r requirements.txt
```

## 🚀 Usage

### Quick Start

```bash
# Run comprehensive analysis
python3 cli.py --analyze

# Generate visualizations
python3 cli.py --visualize

# Export data as JSON
python3 cli.py --export json

# Run with custom threshold
python3 cli.py --analyze --threshold 1500
```

### Command-Line Interface

The CLI provides extensive options for data analysis:

```bash
# Basic analysis
python3 cli.py --analyze

# Generate specific visualizations
python3 cli.py --distribution --trends --dashboard

# Export data in different formats
python3 cli.py --export csv
python3 cli.py --export json
python3 cli.py --export parquet

# Custom parameters
python3 cli.py --analyze --threshold 2000 --top-products 15

# Use custom configuration
python3 cli.py --config custom_config.yaml --analyze

# Verbose output
python3 cli.py --analyze --verbose
```

### Legacy Application

You can still run the original application:

```bash
python3 spark_app.py
```

### Running Tests

```bash
# Run all tests
python3 -m pytest

# Run with verbose output
python3 -m pytest -v

# Run specific test file
python3 -m pytest test_analytics.py -v
```

## Functions

### `create_spark_session(app_name)`
Creates a Spark session with the specified application name.

### `process_sales_data(spark, data_path)`
Processes sales data and returns aggregated results by category:
- Total transactions per category
- Total quantity sold
- Total sales amount
- Average sales amount

### `filter_high_value_sales(spark, data_path, threshold)`
Filters sales data for transactions above the specified threshold.

## Sample Data

The included `data/sales_data.csv` contains sample sales data with:
- Product IDs
- Categories (Electronics, Clothing, Books)
- Sales amounts
- Quantities
- Dates

## Dependencies

- **pyspark[sql]**: Apache Spark Python API with SQL support
- **pytest**: Testing framework
- **chispa**: DataFrame testing utilities for PySpark
- **prettytable**: Table formatting

## Testing

The test suite includes:
- Spark session creation tests
- Data processing function tests
- High-value filtering tests
- Edge case handling (empty data)
- Custom threshold testing

All tests use temporary files to avoid modifying the original data.
