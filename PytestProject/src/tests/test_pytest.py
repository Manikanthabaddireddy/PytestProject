import pytest
import logging
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, BooleanType
from pyspark.sql import SparkSession as ss
from src.config_shiba.config import SparkConfig, FileConfig

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestPytest:
    @pytest.fixture
    def read_file(self):
        logging.info('Starting to read file')
        """Increase the Spark network timeout property: By increasing the network timeout parameter of Spark,
                you are allowing the Spark executor more time to connect to the driver.
                Try adding the following line in your SparkSession builder"""
        spark = ss.builder.appName(SparkConfig.PYTEST_APP_NAME).config("spark.network.timeout","800s").getOrCreate()

        if FileConfig.FILE_TYPE == 'csv':
            df = spark.read.csv(FileConfig.FILE_PATH,sep=";", **FileConfig.OPTIONS['csv'])
        elif FileConfig.FILE_TYPE == 'parquet':
            df = spark.read.parquet(FileConfig.FILE_PATH, **FileConfig.OPTIONS['parquet'])
        elif FileConfig.FILE_TYPE == 'orc':
            df = spark.read.orc(FileConfig.FILE_PATH, **FileConfig.OPTIONS.get('orc', {}))
        elif FileConfig.FILE_TYPE == 'avro':
            df = spark.read.format('avro').load(FileConfig.FILE_PATH, **FileConfig.OPTIONS.get('avro', {}))
        else:
            logging.error(f"Unsupported file type: {FileConfig.FILE_TYPE}")
            raise ValueError(f"Unsupported file type: {FileConfig.FILE_TYPE}")
        logging.info('Finished reading file')
        return df

    def convert_product_cost(self, df):
        logging.info('Starting to convert product_cost')
        df = df.withColumn("product_cost", col("product_cost").cast(FloatType()))
        logging.info('Finished converting product_cost')
        return df

    def test_convert_product_cost(self, read_file):
        logging.info('Starting test_convert_product_cost')
        df = read_file
        converted_df = self.convert_product_cost(df)
        assert str(converted_df.schema["product_cost"].dataType) == 'FloatType()', 'Data type conversion failed'
        logging.info('Finished test_convert_product_cost')

    def test_column_should_lower(self, read_file):
        logging.info('Starting test_column_should_lower')
        df = read_file
        for col in df.columns:
            assert col.islower(), f"Upper case column found: {col}"
        logging.info('Finished test_column_should_lower')



    def test_column_starts_with_alpha(self, read_file):
        logging.info('Starting test_column_starts_with_alpha')
        df = read_file
        cols_to_check = ["_product_id", "product_name", "branch_branch_id", "product_cost", "entry_date"]
        for col in cols_to_check:
            assert col[0].isalpha() == True or col[
                0] == "_", f"Column name {col} can only starts with alphabet or underScore"
        logging.info('Finished test_column_starts_with_alpha')
