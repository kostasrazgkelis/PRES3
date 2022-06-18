from pyspark.sql import SparkSession, DataFrame
from settings import SPARK_DISTRIBUTED_FILE_SYSTEM


class ThesisSparkClass:

    def __init__(self):
        self.metrics_dict = None

        self.set_metrics()
        self.spark = SparkSession.builder \
            .appName("pyspark-notebook") \
            .master("spark://master:7077") \
            .config("spark.executor.memory", "2g") \
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

    def main(self, matching_field, prediction_size, noise):
        df_1 = self.read_csv(file_name='cluster-a_download.csv')
        df_2 = self.read_csv(file_name='cluster-b_download.csv')

        condition = df_1.columns
        condition.remove(matching_field)
        condition.remove("birth_age")

        df_1 = df_1.withColumnRenamed(matching_field, "MatchingFieldDF1")
        df_2 = df_2.withColumnRenamed(matching_field, "MatchingFieldDF2")

        matched_data = df_1.join(other=df_2, on=condition, how='inner').select('*')

        size = int(100 * df_1.count() / (noise + 100))
        total_matches = matched_data.count()
        true_positives = matched_data.\
            where("MatchingFieldDF1==MatchingFieldDF2 and (MatchingFieldDF1 != 'Fake Index' and MatchingFieldDF2 != 'Fake Index')").\
            select('*').\
            count()

        false_positives = total_matches - true_positives
        precision = true_positives / (true_positives + false_positives)
        recall = true_positives / (prediction_size * size)

        self.metrics_dict['size'] = size
        self.metrics_dict['total_matches'] = total_matches
        self.metrics_dict['prediction'] = prediction_size
        self.metrics_dict['precision'] = precision
        self.metrics_dict['recall'] = recall
        self.metrics_dict['TP'] = true_positives
        self.metrics_dict['FP'] = false_positives
        self.metrics_dict['noise'] = noise
