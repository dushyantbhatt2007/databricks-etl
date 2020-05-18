from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import uuid

from adb_etl.configs.etl_config import *


def get_dbutils(correlation_id: uuid.UUID = uuid.uuid4()):
    spark = SparkSession.builder.appName(correlation_id).getOrCreate()
    try:
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def start_spark(app_name: uuid.UUID = uuid.uuid4(), jar_packages: list = [], spark_config: dict = {}) -> SparkSession:
    spark_builder = (SparkSession.builder.appName(app_name))
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)
    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)
    # create session
    spark_sess = spark_builder.getOrCreate()

    return spark_sess


def get_data_source_access(spark: SparkSession, config: Config) -> None:
    dqs_scope = config.SCOPE_NAME
    tenant_id = config.TENANT_ID
    dbutils = get_dbutils(uuid.uuid4())
    spark.conf.set('dfs.adls.oauth2.access.token.provider.type', 'ClientCredential')
    spark.conf.set('dfs.adls.oauth2.access.token.provider',
                   'org.apache.hadoop.fs.adls.oauth2.ConfCredentialBasedAccessTokenProvider')
    spark.conf.set('dfs.adls.oauth2.client.id', dbutils.secrets.get(scope=dqs_scope, key=config.CLIENT_ID))
    spark.conf.set('dfs.adls.oauth2.credential', dbutils.secrets.get(scope=dqs_scope, key=config.SECRET))
    spark.conf.set('dfs.adls.oauth2.refresh.url',
                   'https://login.microsoftonline.com/' + tenant_id + '/oauth2/token')
