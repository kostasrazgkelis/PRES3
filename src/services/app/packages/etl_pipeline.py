import os
import socket
import random
import hashlib
import jellyfish
import requests
import json
from pyspark.context import SparkContext as sc
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from settings import SPARK_DISTRIBUTED_FILE_SYSTEM, NAME_OF_CLUSTER
from connector import HDFSConnector as HDFS

EXTRACT_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + "input/"
LOAD_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'{NAME_OF_CLUSTER}_pretransformed_data'
MATCHED_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'{NAME_OF_CLUSTER}_matched_data'
JOINED_DIRECTORY = SPARK_DISTRIBUTED_FILE_SYSTEM + f'joined_data'


class ThesisSparkClassETLModel:
    """
        This is the ETL class model that will be used in backend for our application.
        The ETL pipeline is responsible for the extraction, transformation and loading
        the data.
    """

    def __init__(self,
                 hdfs: HDFS,
                 filename: str,
                 matching_field: str,
                 columns: list = None,
                 noise: int = None):
        self.hdfs_obj = hdfs
        self.matching_field = matching_field
        self.filename = filename
        self.noise = noise
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
        self.dataframe = self.spark.read.csv(EXTRACT_DIRECTORY + self.filename, header=True)

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
        """
        Save the files in a specific directory in the HDFS
        Returns:

        """
        self.dataframe.coalesce(1).write.format('com.databricks.spark.csv'). \
            mode('overwrite'). \
            save(LOAD_DIRECTORY, header='true')

    def start_etl(self):
        self.extract_data()
        self.transform_data()
        self.load_data()
        self.spark.stop()


class ThesisSparkClassCheckFake(ThesisSparkClassETLModel):

    def __init__(self, hdfs: HDFS, filename: str, matching_field: str, joined_data_filename: str):
        super().__init__(hdfs, filename, matching_field)
        self.joined_data_filename = joined_data_filename
        self.dataframe_joined_data = None
        self.matched_data = None

    def extract_data(self):
        self.dataframe = self.spark.read.csv(os.path.join(LOAD_DIRECTORY, self.filename), header=True)
        self.dataframe_joined_data = self.spark.read.csv(os.path.join(JOINED_DIRECTORY, self.joined_data_filename),
                                                         header=True)

    def transform_data(self):
        self.dataframe_joined_data = self.dataframe_joined_data.withColumnRenamed(self.matching_field, "MatchingField")
        self.dataframe = self.dataframe.withColumnRenamed(self.matching_field, "MatchingField")
        self.dataframe = self.dataframe.na.drop('any')

        self.matched_data = self.dataframe.join(other=self.dataframe_joined_data, on="MatchingField", how='left') \
            .select('*') \
            .where('MatchingField!="Fake Index"')

    def load_data(self):
        self.matched_data.coalesce(1).write.format('com.databricks.spark.csv'). \
            mode('overwrite'). \
            save(MATCHED_DIRECTORY, header='true')
