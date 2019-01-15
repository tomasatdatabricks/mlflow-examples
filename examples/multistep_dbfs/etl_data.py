"""
Converts the raw CSV form to a Parquet form with just the columns we want
"""
import os
import mlflow
import pyspark

def etl_data():
    from sys import version_info
    print(version_info)
    with mlflow.start_run() as mlrun:
        # define DBFS path for csv input - persistent
        ratings_csv = '/mlflow/ricardo/multistep/csv/ratings.csv'

        # define the DBFS path which we will write parquet to
        ratings_parquet_dir = '/mlflow/ricardo/multistep/parquet/ratings'
        print(os.environ)
        print(os.environ["MASTER"])
        print([x for x in os.environ.items() if "SPARK" in x[0]])
        print(pyspark.sql.SparkSession.builder._options)
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        print('spark context -> ', spark.sparkContext.master) 
        do_init = pyspark.context.SparkContext._do_init
       
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

def my_init(self, conf, **kwargs):
    # java gateway must have been launched at this point.
    if conf is not None and conf._jconf is not None:
        print("*** _conf = existing conf ***")
        # conf has been initialized in JVM properly, so use conf directly. This represent the
        # scenario that JVM has been launched before SparkConf is created (e.g. SparkContext is
        # created and then stopped, and we create a new SparkConf and new SparkContext again)
        self._conf = conf
    else:
        print("*** _conf = new conf ***")
        print("old conf = ", conf)
        

if __name__ == '__main__':
    print('running main!')
    do_init = pyspark.context.SparkContext._do_init
    def my_init(self,  *args):
      conf = args[7]

      # java gateway must have been launched at this point.
      if conf is not None and conf._jconf is not None:
        print("*** _conf = existing conf ***")
        # conf has been initialized in JVM properly, so use conf directly. This represent the
        # scenario that JVM has been launched before SparkConf is created (e.g. SparkContext is
        # created and then stopped, and we create a new SparkConf and new SparkContext again)
        self._conf = conf
      else:
        print("*** _conf = new conf ***")
        print("*** old conf =", conf, "***")
      do_init(self, *args)
    pyspark.context.SparkContext._do_init = my_init
    etl_data()
