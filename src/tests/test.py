
import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from src.etl.utils import *

class Assignment1(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[3]").appName("PySpark-unit-test").getOrCreate()
        self.transac = self.spark.read.option('inferSchema', 'True').option('header', 'True').csv(r'data\transaction.csv')
        self.user = self.spark.read.option('inferSchema', 'True').option('header', 'True').csv(r'data\user.csv')
        col = 'userid'
        replace_it_with = 'user_id'
        self.new_transac = self.transac.withColumnRenamed(col, replace_it_with)
        self.combined_df = self.new_transac.join(self.user, on=['user_id'], how='inner')
        #self.combined_df.to_csv()


    def test_transac_count(self):
        result_count = self.transac.count()
        exepected_output = 10
        self.assertEqual(result_count , exepected_output , "record count should be 10")

    def test_user_loading(self):
        result_count = self.user.count()
        exepected_output = 10
        self.assertEqual(result_count, exepected_output, "record count should be 10")


    def test_max_expense(self):
        val = max_expense(self.combined_df)
        expected_val = 66000
        self.assertTrue(val, expected_val)
        

if __name__ =="__main__":
    unittest.main()

