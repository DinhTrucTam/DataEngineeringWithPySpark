from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/F:/PostgreSQL/JDBC/postgresql-42.7.2.jar") \
    .getOrCreate()
    
pg_url = "jdbc:postgresql://localhost:5432/DW"

pg_properties = {
    "user": "postgres",
    "password": "tructam2992",
    "driver": "org.postgresql.Driver"
}

# Load CSV file into PySpark DataFrame
df = spark.read.csv('/F:/DE/DataEngineeringWithPySpark/final_output/part-00000-f759246b-ec60-450c-8c6e-7d836a127f62-c000.csv', header=True, inferSchema=True)
df.printSchema()


# Selecting a single record for verification
df_sample = df.limit(1)

# Mapping the DataFrame to PostgreSQL Table
# Example: Loading Ambient Conditions
df_ambient = df_sample.select(
    monotonically_increasing_id().alias("ambient_id"),  # Generate unique ID
    col("AmbientConditions_AmbientHumidity_U_Actual").alias("ambient_humidity"),
    col("AmbientConditions_AmbientTemperature_U_Actual").alias("ambient_temperature")
)

df_ambient.write.jdbc(pg_url, "Dim_Ambient_Conditions", mode="append", properties=pg_properties)

print("Single record inserted into PostgreSQL for verification.")