"""
Converts the raw CSV form to a Parquet form with just the columns we want
"""

import mlflow
import pyspark

def etl_data():
    from from sys import version_info
    print(version_info)
    with mlflow.start_run() as mlrun:
        # define DBFS path for csv input - persistent
        ratings_csv = '/mlflow/ricardo/multistep/csv/ratings.csv'

        # define the DBFS path which we will write parquet to
        ratings_parquet_dir = '/mlflow/ricardo/multistep/parquet/ratings'
        import os
        print(os.environ)
        print(os.environ["MASTER"])
        print(pyspark.sql.SparkSession.builder._options)
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        print('spark context -> ', spark.sparkContext.master) 
        print("Converting ratings CSV %s to Parquet %s" % (ratings_csv, ratings_parquet_dir))
        ratings_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(ratings_csv) \
            .drop("timestamp")  # Drop unused column
        ratings_df.show()
        ratings_df.write.mode('overwrite').parquet(ratings_parquet_dir)

        print("Uploading Parquet ratings: %s" % ratings_parquet_dir)
        mlflow.log_artifacts(ratings_parquet_dir, "ratings-parquet-dir")


if __name__ == '__main__':
    etl_data()
