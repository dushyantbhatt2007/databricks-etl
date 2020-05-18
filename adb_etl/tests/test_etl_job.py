import unittest
from pyspark.sql.functions import mean

from adb_etl.jobs.etl_job import *


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark = start_spark()
        config = Config()
        get_data_source_access(spark=self.spark, config=config)
        self.test_data_path = config.DATA_LAKE + '/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        # self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble

        input_data = self.spark.read.parquet(self.test_data_path + 'employees')
        expected_data = self.spark.read.parquet(self.test_data_path + 'employees_report')
        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = (
            expected_data.agg(mean('steps_to_desk').alias('avg_steps_to_desk')).collect()[0]['avg_steps_to_desk'])

        # act
        data_transformed = transform_data(input_data, 21)

        cols = len(data_transformed.columns)
        rows = data_transformed.count()
        avg_steps = (
            data_transformed.agg(mean('steps_to_desk').alias('avg_steps_to_desk')).collect()[0]['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns for col in data_transformed.columns])


if __name__ == '__main__':
    # unittest.main(defaultTest='suite')
    suite = unittest.TestLoader().loadTestsFromTestCase(SparkETLTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
