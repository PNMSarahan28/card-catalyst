"""
Data Reader module for reading transaction data from S3 or Local
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from utilities.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataReader:
    """Handles reading transaction data from S3 or Local"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = self._get_transaction_schema()

    def _get_transaction_schema(self) -> StructType:
        """Define the schema for transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("card_number", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("customer_id", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("location", StringType(), False)
        ])

    def _read_json(self, path: str) -> DataFrame:
        """Generic JSON reader with schema and options"""
        return (self.spark
                .read
                .schema(self.schema)
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
                .json(path))

    def read_transactions_from_s3(self, s3_path: str = None) -> DataFrame:
        """Read transaction data from S3"""
        try:
            if s3_path is None:
                s3_path = Config.S3_RAW_PATH
            logger.info(f"Reading transaction data from: {s3_path}")
            df = self._read_json(s3_path)
            logger.info(f"Successfully read {df.count()} records from S3")
            return df
        except Exception as e:
            logger.error(f"Error reading data from S3: {str(e)}")
            raise

    def read_transactions_from_local(self, local_path: str = None) -> DataFrame:
        """Read transaction data from Local File System"""
        try:
            if local_path is None:
                local_path = Config.LOCAL_RAW_PATH
            logger.info(f"Reading transaction data from: {local_path}")
            df = self._read_json(local_path)
            logger.info(f"Successfully read {df.count()} records from Local")
            return df
        except Exception as e:
            logger.error(f"Error reading data from Local: {str(e)}")
            raise

    def read_transactions_by_date(self, year: str, month: str, day: str, mode: str) -> DataFrame:
        """Read transaction data for a specific date"""
        if mode == "s3":
            data_path = f"{Config.S3_RAW_PATH}year={year}/month={month}/day={day}/"
            return self.read_transactions_from_s3(data_path)
        elif mode == "local":
            data_path = f"file://{Config.LOCAL_RAW_PATH}/{year}_{month}_{day}.json"
            return self.read_transactions_from_local(data_path)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def read_transactions_date_range(self, start_date: str, end_date: str) -> DataFrame:
        """Read transaction data for a date range"""
        df = self.read_transactions_from_s3()
        return (df.filter(df.timestamp.between(start_date, end_date)).cache())

    def validate_data_availability(self, s3_path: str = None) -> bool:
        """Check if data is available at the specified S3 path"""
        try:
            if s3_path is None:
                s3_path = Config.S3_RAW_PATH
            df = (self.spark.read.option("multiline", "true").json(s3_path).limit(1))
            return df.count() > 0
        except Exception as e:
            logger.warning(f"Data not available at {s3_path}: {str(e)}")
            return False
