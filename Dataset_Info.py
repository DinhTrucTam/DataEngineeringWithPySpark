import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()
df_pyspark = spark.read.csv('data_generated.csv', header=True, inferSchema=True)
df_pyspark.printSchema()