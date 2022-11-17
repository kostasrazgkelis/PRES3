import socket
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import col

from settings import SPARK_DISTRIBUTED_FILE_SYSTEM


class ThesisSparkClass:

    def __init__(self,
                 project_name: str,
                 file_a: str,
                 file_b: str,
                 prediction_size: float):

        self.project_name = project_name
        self.file_a = file_a
        self.file_b = file_b
        self.prediction_size = prediction_size

        self.matched_data = None
        self.metrics_dict = None
        self.df_1 = None
        self.df_2 = None
        
        spark_driver_host = socket.gethostname()
        self.spark_conf = SparkConf() \
            .setAll([
            ('spark.master', f'spark://spark-master:7077'),
            ('spark.driver.bindAddress', '0.0.0.0'),
            ('spark.driver.host', spark_driver_host),
            ('spark.app.name', self.project_name),
            ('spark.submit.deployMode', 'client'),
            ('spark.ui.showConsoleProgress', 'true'),
            ('spark.eventLog.enabled', 'false'),
            ('spark.logConf', 'false'),
            ('spark.cores.max', "4"),
            ("spark.executor.memory", "1g"),
            ('spark.driver.memory', '15g'),
        ])

        self.set_metrics()
        self.spark = SparkSession.builder \
                                 .config(conf=self.spark_conf)\
                                .enableHiveSupport() \
                                 .getOrCreate()

    def set_metrics(self):
        self.metrics_dict = {
            "size": int,
            "prediction": float,
            "precision": float,
            'recall': float,
            'TP': int,
            'FP': int,
            'total_matches': int,
            'noise': int,
        }

    def get_metrics(self):
        return self.metrics_dict

    def set_matching_field(self):
        import re
        matching_field = [name for name, value in self.df_1.take(1)[0].asDict().items() if not re.match("[0-9a-f]{64}", value)]
        matching_field.remove("_c0")
        self.matching_field = str(matching_field[0])

    def get_matching_field(self):
        return self.matching_field

    def read_csv(self, file_name: str) -> DataFrame:
        return self.spark.read.csv(path=f"{SPARK_DISTRIBUTED_FILE_SYSTEM}/{file_name}", sep=",", header=True)

    def extract_data(self):
        self.df_1 = self.read_csv(file_name=f'pretransformed_data/{self.file_a}')
        self.df_2 = self.read_csv(file_name=f'pretransformed_data/{self.file_b}')

    def transform_data(self):
        self.set_matching_field()

        condition = self.df_1.columns
        condition.remove(self.matching_field)

        self.df_1 = self.df_1.withColumnRenamed(self.matching_field, "MatchingFieldDF1")
        self.df_2 = self.df_2.withColumnRenamed(self.matching_field, "MatchingFieldDF2")

        self.matched_data = self.df_1.join(other=self.df_2, on=condition, how='inner').select('*')

        self.matched_data = self.matched_data.drop(col("MatchingFieldDF2"))
        self.matched_data = self.matched_data.withColumnRenamed("MatchingFieldDF1", self.matching_field)

    def load_data(self):
        # self.matched_data.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save(
        #     SPARK_DISTRIBUTED_FILE_SYSTEM + 'joined_data', header='true')
        directory = os.path.join(SPARK_DISTRIBUTED_FILE_SYSTEM + 'joined_data', f'{self.project_name.lower()}')
        if not os.path.exists(directory):
            os.mkdir(directory)
        path = os.path.join(directory, 'results.csv')
        self.matched_data.toPandas().to_csv(path, index=False)

    def start_etl(self):
        self.extract_data()
        self.transform_data()
        self.load_data()
        self.spark.stop()
