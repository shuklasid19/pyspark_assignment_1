# importing sys
#import sys
#sys.path.insert(0, r'C:\Users\sid\Desktop\current\New folder\src\etl')
#import os
#location = r'C:\Users\sid\Desktop\current\New folder\data/'

from math import comb
from pyspark.sql import *
from pyspark import *
from src.etl.utils import *
from pyspark.sql import functions as F


#calling the driver
spark = spark_driver()
print("spark_session started", spark)

#reading and showing the csv file transactions.csv
transc = read_transac(spark)
#transc.show()
print("count of transaction row numbers ", transc.count())

#reading and showing the csv
user = read_user(spark)
print("user csv file has number of rows ", user.count())


#new variable for saving changed column name 
new_transac = change_column(transc, 'userid', 'user_id')
new_transac.show()

#combined the old and new csv file 
combined_df = combined_csv(spark, new_transac, user)
print("the combined csv files count in row ", combined_df.count())
print(" ")
print("the combined csv files transac and users ")
combined_df.show()

#unique locations
unique_loc = unique_location(combined_df)
print("number of unique locations ", unique_loc.count())
print("unique locations ")
unique_loc.show()

#product bought by each user
product_boght = product_bought(combined_df)

print("product bought by each user ")
product_boght.show()


#all product expenses
product_exp = product_expenses(combined_df)
print("product expense ")
product_exp.show()


allmax_expenses = max_expense(combined_df)
print(allmax_expenses)
