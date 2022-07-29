from math import comb
import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
from src.etl.utils import *

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
        exepected_output = 10
        self.assertEqual(result_count , exepected_output , "record count should be 10")

    def test_user_loading(self):
        sample_df = read_user(self.spark)
        result_count = sample_df.count()
        exepected_output = 10
        self.assertEqual(result_count, exepected_output, "record count should be ")



if __name__ =="__main__":
    unittest.main()

