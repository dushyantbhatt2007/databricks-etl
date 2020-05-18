from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
dbutils = DBUtils(spark.sparkContext)
dbutils.secrets.setToken("")

'''
// command to get DBUtils token
displayHTML(
  "<b>Privileged DBUtils token (expires in 48 hours): </b>" +
  dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>"))
'''
