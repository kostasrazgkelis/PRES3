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
        select_condition = condition.copy()

        column_1 = f'df_1.{matching_field} as mf_1'
        column_2 = f'df_2.{matching_field} as mf_2'
        select_condition.append(column_1)
        select_condition.append(column_2)

        matched_data = df_1.join(other=df_2, on=condition, how='inner').selectExpr(select_condition)

        size = df_1.count()
        total_matches = matched_data.count()
        true_positives = matched_data.\
            where("mf_1==mf_2 and (mf_1 != 'Fake Index' and mf_2 != 'Fake Index' ").\
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
