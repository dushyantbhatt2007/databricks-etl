from pyspark.sql import Row, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, concat_ws, lit

from adb_etl.dependencies.spark_utility import *
from adb_etl.configs.etl_config import *


def main(config: Config) -> None:
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = start_spark()
    get_data_source_access(spark=spark, config=config)
    # execute ETL pipeline
    data = extract_data(spark, config)
    data.show()
    data_transformed = transform_data(data, config.STEPS_PER_FLOOR)
    data_transformed.show()
    load_data(data_transformed, config)

    # spark.stop()
    return None


def extract_data(spark, config):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :param config: config object
    :return: Spark DataFrame.
    """
    df = spark.read.parquet(config.DATA_LAKE + '/test_data/employees/')
    return df


def transform_data(df: SparkDataFrame, steps_per_floor_: int) -> SparkDataFrame:
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = df.select(col('id'), concat_ws(' ', col('first_name'), col('last_name')).alias('name'),
                               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk'))
    return df_transformed


def load_data(df: SparkDataFrame, config: Config) -> None:
    """Write the csv output to data lake.
    :param df: DataFrame to write.
    :param config: Config object
    :return: None
    """
    df.coalesce(1).write.csv(config.DATA_LAKE + '/test_data/loaded_data/', mode='overwrite', header=True)
    return None


def create_test_data(spark: SparkSession, config: Config) -> None:
    """Create test data.
    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', last_name='Germain', floor=1),
        Row(id=2, first_name='Dan', last_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', last_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', last_name='Lai', floor=2),
        Row(id=5, first_name='Stu', last_name='White', floor=3),
        Row(id=6, first_name='Mark', last_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', last_name='Bird', floor=4),
        Row(id=8, first_name='Kim', last_name='Suter', floor=1)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    df.coalesce(1).write.parquet('tests/test_data/employees', mode='overwrite')

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    df_tf.coalesce(1).write.parquet('tests/test_data/employees_report', mode='overwrite')
    return None
