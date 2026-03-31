"""
Configuration file for PySpark transaction processing pipeline
"""

import os
from dotenv import load_dotenv
from typing import Dict

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Load environment variables
load_dotenv(dotenv_path=ENV_PATH)

class Config:
    """Configuration class containing all necessary settings"""

    # AWS S3 Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

    # S3 Paths
    S3_BUCKET = os.getenv("S3_BUCKET", "daily-transaction-store1")
    S3_RAW_PATH = f"s3a://{S3_BUCKET}/raw/transaction_type=purchase/"
    S3_PROCESSED_PATH = f"s3a://{S3_BUCKET}/processed/"
    S3_SUMMARY_PATH = f"s3a://{S3_BUCKET}/summaries/"

    LOCAL_RAW_PATH = os.path.join(BASE_DIR, "data_files/raw_files")
    LOCAL_PROCESSED_PATH = "/home/prince-neezhel-sarahan/Documents/DE_Project1/data_files/processed"
    LOCAL_SUMMARY_PATH   = "/home/prince-neezhel-sarahan/Documents/DE_Project1/data_files/summaries"

    # MySQL Configuration
    MYSQL_CONFIG = {
        'host': os.getenv('MYSQL_HOST', '192.168.56.1'),
        'port': int(os.getenv('MYSQL_PORT', '3306')),
        'database': os.getenv('MYSQL_DATABASE', 'catalyst'),
        'user': os.getenv('MYSQL_USER', 'de-root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'driver': 'com.mysql.cj.jdbc.Driver'
    }

    # Data validation rules
    VALIDATION_RULES = {
        "required_fields": [
            "transaction_id",
            "merchant_id",
            "card_number",
            "amount",
            "timestamp",
            "customer_id",
            "currency",
            "location"
        ],
        "amount": {
            "min": 0.01,
            "max": 100000.00
        },
        "card_number_length": 16
    }

    # Masking configuration
    MASKING_CONFIG = {
        'card_number': {
            'method': 'mask',
            'mask_char': 'X',       # character used for masking
            'visible_digits': 4     # number of digits to keep visible
        },
        'customer_id': {
            'method': 'replace',
            'replacement': 'XXXXXX'
        },
        'merchant': {
            'method': 'truncate',
            'max_length': 10
        }
    }

    # Spark Configuration
    SPARK_CONFIG = {
        'app_name': 'CardCatalyst',
        'master': os.getenv('SPARK_MASTER', 'local[*]'),
        'config': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.hadoop.fs.s3a.access.key': AWS_ACCESS_KEY_ID,
            'spark.hadoop.fs.s3a.secret.key': AWS_SECRET_ACCESS_KEY,
            'spark.hadoop.fs.s3a.endpoint': f's3.{AWS_REGION}.amazonaws.com',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.defaultFS': 'file:///',  # Force local FS
        }
    }

def get_mysql_jdbc_url() -> str:
    """Generate MySQL JDBC URL from configuration"""
    mysql_config = Config.MYSQL_CONFIG
    return f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"

def get_mysql_properties() -> Dict[str, str]:
    """Get MySQL connection properties for Spark"""
    mysql_config = Config.MYSQL_CONFIG
    return {
        'user': mysql_config['user'],
        'password': mysql_config['password'],
        'driver': mysql_config['driver']
    }
