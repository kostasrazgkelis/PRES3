import socket
import random
import hashlib
import jellyfish
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import SPARK_DISTRIBUTED_FILE_SYSTEM, NAME_OF_CLUSTER


class ThesisSparkClassETLModel:
    """
        This is the ETL class model that will be used in backend for our application.
        The ETL pipeline is rensponsible for the excraction, transformation and loading
        the data.
    """

    def __init__(self,
                 file_name: str,
                 columns: list,
                 matching_field: str,
                 noise: int):
        self.matching_field = matching_field
        self.noise = noise
        self.file_name = file_name
        self.columns = columns
        self.dataframe = None
        self.df_with_noise = None

        spark_driver_host = socket.gethostname()
        self.spark_conf = SparkConf() \
            .setAll([
            ('spark.master', 'spark://master:7077'),
            ('spark.driver.bindAddress', '0.0.0.0'),
            ('spark.driver.host', spark_driver_host),
            ('spark.app.name', NAME_OF_CLUSTER),
            ('spark.submit.deployMode', 'client'),
            ('spark.ui.showConsoleProgress', 'true'),
            ('spark.eventLog.enabled', 'false'),
            ('spark.logConf', 'false'),
            ('spark.cores.max', "4"),
            ("spark.executor.memory", "1g"),
            ('spark.driver.memory', '15g'),
        ])

        self.spark = SparkSession.builder \
            .appName("pyspark-notebook") \
            .master("spark://master:7077") \
            .config("spark.executor.memory", "1700m") \
            .enableHiveSupport() \
            .getOrCreate()
        self.spark.sparkContext.accumulator(0)

    @staticmethod
    @udf(StringType())
    def jelly(data):
        return jellyfish.soundex(data)

    @staticmethod
    @udf(StringType())
    def hash_sha256(data):
        return hashlib.sha256(data.encode()).hexdigest()

    def create_alp(self) -> str:
        return str(chr(random.randrange(65, 90))) + str(chr(random.randrange(48, 54))) + str(
            chr(random.randrange(48, 54))) + str(chr(random.randrange(48, 54)))

    def extract_data(self):
        self.dataframe = self.spark.read.csv(
            SPARK_DISTRIBUTED_FILE_SYSTEM + f"{NAME_OF_CLUSTER}_pretransformed_data/" + self.file_name, header=True)

    def transform_data(self):
        self.dataframe = self.dataframe.na.drop('any')
        columns = self.dataframe.columns
        size = int(self.dataframe.count() * (self.noise / 100))
        data = [{colum: (self.create_alp() if not colum == self.matching_field else 'Fake Index') for colum in columns}
                for _ in range(0, size)]

        # create noise
        df_with_noise = self.spark.createDataFrame(data, columns)

        # add noise
        self.dataframe = self.dataframe.union(df_with_noise)

        # apply Soundex + SHA256
        self.dataframe = self.dataframe.select([self.hash_sha256(self.jelly(col(column))).alias(column)
                                                if not column == self.matching_field else col(column).alias(column)
                                                for column in columns])

        # sort by a random column
        self.dataframe = self.dataframe.sort(random.choice(columns))

    def load_data(self):
        self.dataframe.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save(
            SPARK_DISTRIBUTED_FILE_SYSTEM + f'{NAME_OF_CLUSTER}_transformed_data', header='true')

    def start_etl(self):
        self.extract_data()
        self.transform_data()
        self.load_data()
        self.spark.stop()
