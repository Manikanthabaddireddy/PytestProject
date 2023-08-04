class SparkConfig:
    PYTEST_APP_NAME = "Pytest"
    GE_APP_NAME = "GX"

class FileConfig:
    FILE_PATH = r"C:\Users\manib\Downloads\staging_productss.csv"
    FILE_TYPE = "csv"  # could be "csv", "parquet", "orc", "avro"
    OPTIONS = {
        'csv': {
            'header': 'true',
            'inferSchema': 'true'
        },
        'parquet': {
            'mergeSchema': 'true'
        },
        'orc': {},
        'avro': {},
        # add options for other file types as needed
    }
