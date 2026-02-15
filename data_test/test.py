import pandas as pd 
df = pd.read_parquet("data_test/yellow_tripdata_2024-01.parquet")
# print(df.head())
print(df.dtypes)