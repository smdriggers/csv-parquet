"""
This .py file is meant to ingest a csv and output a parquet file.
"""

# I imagine this could be reduced to something along the lines of "from pyspark.sql
# import SparkSession" but I haven't managed to refine that yet
import pyspark

# this pandas import is only needed as a test
import pandas as pd


spark = pyspark.sql.SparkSession.builder \
    .master('local') \
    .appName('parquet_example') \
    .getOrCreate()


df = spark.read.csv("practice_data.csv", header=True)
df.repartition(1).write.mode('overwrite').parquet('spark.parquet')


"""
This section was meant to serve as a sort of test, to see if I could return
from the parquet file to a csv. So far it doesn't work.
"""
# reconstruct = pd.read_parquet("spark.parquet")
# reconstruct.to_csv('reconstructed_data.csv', index=False)
# print(reconstruct.head(10))
