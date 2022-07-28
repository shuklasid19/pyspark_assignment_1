import os
import pandas  as pd
print(os.getcwd())

df = pd.read_csv('transaction.csv')
print(df.head())