import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from src.etl.utils import read_transac , read_user

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    

    def test_datafile_loading(self):
        sample_df = read_transac(self.spark)
        result_count = sample_df.count()
        self.assertEqual(result_count , 10 , "record count should be 10")

    def test_user_loading(self):
        sample_df = read_user(self.spark)
        result_count = sample_df.count()
        self.assertEqual(result_count, 10, "record count should be ")


if __name__ =="__main__":
    unittest.main()

