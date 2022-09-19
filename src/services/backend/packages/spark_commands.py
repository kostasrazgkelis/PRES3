from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import col

from settings import SPARK_DISTRIBUTED_FILE_SYSTEM
import socket
import os

class ThesisSparkClass:

    def __init__(self, 
                file_a: str, 
                file_b: str, 
                matching_field: str, 
                prediction_size: float,
                noise: int):

        self.file_a = file_a
        self.file_b = file_b
        self.matching_field = matching_field
        self.prediction_size = prediction_size
        self.noise = noise

        self.matched_data = None
        self.metrics_dict = None
        self.df_1 = None
        self.df_2 = None
        
        spark_driver_host = socket.gethostname()
        self.spark_conf = SparkConf() \
            .setAll([
            ('spark.master', f'spark://master:7077'),
            ('spark.driver.bindAddress', '0.0.0.0'),
            ('spark.driver.host', spark_driver_host),
            ('spark.app.name', 'cluster_c'),
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
            .appName("pyspark-notebook-C") \
            .master("spark://master:7077") \
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

    def read_csv(self, file_name: str) -> DataFrame:
        return self.spark.read.csv(path=f"{SPARK_DISTRIBUTED_FILE_SYSTEM}/{file_name}", sep=",", header=True)

    def extract_data(self):
        # return self.spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(**options).load()
        # # return self.read_csv(file_name=file_name)
        self.df_1 = self.read_csv(file_name=f'cluster_a_pretransformed_data/{self.file_a}')
        self.df_2 = self.read_csv(file_name=f'cluster_b_pretransformed_data/{self.file_b}')

    def transform_data(self):

        condition = self.df_1.columns
        condition.remove(self.matching_field)

        self.df_1 = self.df_1.withColumnRenamed(self.matching_field, "MatchingFieldDF1")
        self.df_2 = self.df_2.withColumnRenamed(self.matching_field, "MatchingFieldDF2")

        self.matched_data = self.df_1.join(
            other=self.df_2, on=condition, how='inner').select('*')

        size = int(100 * self.df_1.count() / (int(self.noise) + 100))
        total_matches = int(self.matched_data.count())
        true_positives = self.matched_data.\
            where("MatchingFieldDF1==MatchingFieldDF2 and (MatchingFieldDF1 != 'Fake Index' and MatchingFieldDF2 != 'Fake Index')").\
            select('*').\
            count()

        self.matched_data = self.matched_data.drop(col("MatchingFieldDF2"))
        self.matched_data = self.matched_data.withColumnRenamed("MatchingFieldDF1", self.matching_field)

        false_positives = int(total_matches - true_positives)
        precision = true_positives / (true_positives + false_positives)
        recall = true_positives / (float(self.prediction_size) * size)

        self.metrics_dict['size'] = size
        self.metrics_dict['total_matches'] = total_matches
        self.metrics_dict['prediction'] = self.prediction_size
        self.metrics_dict['precision'] = precision
        self.metrics_dict['recall'] = recall
        self.metrics_dict['TP'] = true_positives
        self.metrics_dict['FP'] = false_positives
        self.metrics_dict['noise'] = self.noise

    def load_data(self):
        self.matched_data.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save(
            SPARK_DISTRIBUTED_FILE_SYSTEM + 'joined_data', header='true')

    def start_etl(self):
        self.extract_data()
        self.transform_data()
        self.load_data()
        self.spark.stop()
