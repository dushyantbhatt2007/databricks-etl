from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
dbutils = DBUtils(spark.sparkContext)
dbutils.secrets.setToken("")
