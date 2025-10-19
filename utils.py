"""
Utility functions for the Spark Sales Analytics project
"""

import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
import yaml


def setup_logger(name: str, log_file: Optional[str] = None, level: str = "INFO") -> logging.Logger:
    """
    Setup a logger with both file and console handlers.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Warning: Configuration file {config_path} not found. Using defaults.")
        return {}
    except yaml.YAMLError as e:
        print(f"Error parsing configuration file: {e}")
        return {}


def save_results(results: Dict[str, Any], output_path: str, filename: str = "analysis_results.json"):
    """
    Save analysis results to JSON file.
    
    Args:
        results: Results dictionary to save
        output_path: Output directory path
        filename: Output filename
    """
    os.makedirs(output_path, exist_ok=True)
    
    file_path = os.path.join(output_path, filename)
    
    # Add timestamp
    results['timestamp'] = datetime.now().isoformat()
    
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"Results saved to {file_path}")


def create_output_directories(base_path: str, subdirs: list = None):
    """
    Create output directories structure.
    
    Args:
        base_path: Base output directory
        subdirs: List of subdirectories to create
    """
    if subdirs is None:
        subdirs = ['plots', 'data', 'logs', 'reports']
    
    for subdir in subdirs:
        os.makedirs(os.path.join(base_path, subdir), exist_ok=True)


def format_currency(amount: float) -> str:
    """
    Format currency amount for display.
    
    Args:
        amount: Amount to format
        
    Returns:
        Formatted currency string
    """
    return f"${amount:,.2f}"


def format_percentage(value: float, total: float) -> str:
    """
    Format percentage for display.
    
    Args:
        value: Value to calculate percentage for
        total: Total value
        
    Returns:
        Formatted percentage string
    """
    if total == 0:
        return "0.00%"
    return f"{(value / total) * 100:.2f}%"


def print_separator(char: str = "=", length: int = 50):
    """
    Print a separator line.
    
    Args:
        char: Character to use for separator
        length: Length of separator line
    """
    print(char * length)


def print_header(title: str, char: str = "=", length: int = 50):
    """
    Print a formatted header.
    
    Args:
        title: Header title
        char: Character to use for border
        length: Length of header line
    """
    print_separator(char, length)
    print(f" {title} ".center(length))
    print_separator(char, length)


def validate_data_path(path: str) -> bool:
    """
    Validate that data file exists and is readable.
    
    Args:
        path: Path to data file
        
    Returns:
        True if valid, False otherwise
    """
    if not os.path.exists(path):
        print(f"Error: Data file {path} does not exist")
        return False
    
    if not os.access(path, os.R_OK):
        print(f"Error: Data file {path} is not readable")
        return False
    
    return True


def get_file_size_mb(path: str) -> float:
    """
    Get file size in megabytes.
    
    Args:
        path: File path
        
    Returns:
        File size in MB
    """
    if os.path.exists(path):
        return os.path.getsize(path) / (1024 * 1024)
    return 0.0


def print_system_info():
    """Print system information for debugging."""
    import platform
    import sys
    
    print("\n🖥️  System Information:")
    print(f"   • Platform: {platform.platform()}")
    print(f"   • Python: {sys.version}")
    print(f"   • Architecture: {platform.architecture()[0]}")
    print(f"   • Processor: {platform.processor()}")


def print_analysis_summary(insights: Dict[str, Any]):
    """
    Print a formatted analysis summary.
    
    Args:
        insights: Insights dictionary from analysis
    """
    print_header("📊 Analysis Summary")
    
    print(f"📈 Total Transactions: {insights.get('total_transactions', 0):,}")
    print(f"💰 Total Sales: {format_currency(insights.get('total_sales', 0))}")
    print(f"📦 Total Quantity: {insights.get('total_quantity', 0):,}")
    print(f"📊 Average per Transaction: {format_currency(insights.get('avg_sales_per_transaction', 0))}")
    
    print(f"\n🏆 Top Category: {insights.get('top_category', 'N/A')}")
    print(f"   Sales: {format_currency(insights.get('top_category_sales', 0))}")
    
    print(f"\n⭐ Top Product: {insights.get('top_product', 'N/A')}")
    print(f"   Sales: {format_currency(insights.get('top_product_sales', 0))}")
    
    print(f"\n📅 Analysis Period:")
    print(f"   Start: {insights.get('date_range', {}).get('start', 'N/A')}")
    print(f"   End: {insights.get('date_range', {}).get('end', 'N/A')}")
    
    print(f"\n🎯 Peak Performance:")
    print(f"   Date: {insights.get('peak_sales_day', 'N/A')}")
    print(f"   Sales: {format_currency(insights.get('peak_sales_amount', 0))}")
    
    print_separator()
