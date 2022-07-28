from pyspark.sql import *
from pyspark import *
from src.etl.utils import change_column, read_transac, spark_driver , read_user, combined_csv, unique_location, product_expenses , product_bought


# importing sys
#import sys

#sys.path.insert(0, r'C:\Users\sid\Desktop\current\New folder\src\etl')


#import os
#location = r'C:\Users\sid\Desktop\current\New folder\data/'

#calling the driver
spark = spark_driver()
print(spark)

#reading and showing the csv file transactions.csv
transc = read_transac(spark)
#transc.show()
print(transc.count())

#reading and showing the csv
user = read_user(spark)
user.show()
print(user.count())


#new variable for saving changed column name 
new_transac = change_column(transc, 'userid', 'user_id')
new_transac.show()
print(new_transac.count())

#combined the old and new csv file 
combined_df = combined_csv(spark, new_transac, user)
combined_df.show()

#
unique_loc = unique_location(combined_df)
unique_loc.show()

product_boght = product_bought(combined_df)
product_boght.show()


product_exp = product_expenses(combined_df)
product_exp.show()





