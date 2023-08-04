import pytest
import logging
from datetime import datetime
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.types import FloatType, BooleanType, StructType, StructField, StringType, IntegerType, DoubleType
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
            df = spark.read.csv(FileConfig.FILE_PATH,sep=";",header=True, inferSchema=True, **FileConfig.OPTIONS['csv'])
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

    class TestGX:

        def setup_method(self, method):
            """
            setup_method is a special method inside pytest test classes.
            This will run before every test method that belongs to this class.
            """
            spark = ss.builder.appName("Pytest").getOrCreate()
            self.df = spark.read.csv("C:\\Users\\shiba\\Downloads\\staging_products.csv", sep=";",header=True, inferSchema=True)
            self.expected_columns = ["product_id", "product_name", "branch_branch_id", "product_cost", "entry_date"]

        def test_column_count(self):
            """
            Test method to check the number of columns in the dataframe.
            """
            assert len(self.df.columns) == 5, "Unexpected column count"

        def test_column_existence(self):
            """
            Test method to check the existence of specific columns in the dataframe.
            """
            for col_name in self.expected_columns:
                assert col_name in self.df.columns, f"Column {col_name} does not exist"

        def test_no_null_values(self):
            """
            Test method to check that no columns in the dataframe contain null values.
            """
            for col_name in self.expected_columns:
                assert self.df.filter(
                    self.df[col_name].isNull()).count() == 0, f"Column {col_name} contains null values"

        # def test_unique_values(self):
        #     """
        #     Test method to check that all values in specific columns of the dataframe are unique.
        #     """
        #     for col_name in self.expected_columns:
        #         assert self.df.select(
        #             col_name).distinct().count() == self.df.count(), f"Column {col_name} contains non-unique values"
