"""
Converts the raw CSV form to a Parquet form with just the columns we want
"""


from __future__ import print_function

import requests
import tempfile
import os
import zipfile
import pyspark
import mlflow


def etl_data():
    with mlflow.start_run() as mlrun:
        # define DBFS path for csv input - persistent
        ratings_csv = '/mlflow/ricardo/multistep/csv/ratings.csv'

        # define the DBFS path which we will write parquet to
        ratings_parquet_dir = '/mlflow/ricardo/multistep/parquet/ratings'

        #spark = pyspark.sql.SparkSession.builder.getOrCreate()
        print("Converting ratings CSV %s to Parquet %s" % (ratings_csv, ratings_parquet_dir))
        ratings_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ratings_csv) \
            .drop("timestamp")  # Drop unused column
        ratings_df.show()
        ratings_df.write.parquet(ratings_parquet_dir)

        print("Uploading Parquet ratings: %s" % ratings_parquet_dir)
        mlflow.log_artifacts(ratings_parquet_dir, "ratings-parquet-dir")


if __name__ == '__main__':
    etl_data()
