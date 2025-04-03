from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, lit, round
from pyspark.sql.types import TimestampType
import sys

# Redirect stdout and stderr to a log file
sys.stdout = open("execution_log.txt", "w")
sys.stderr = sys.stdout

# Initialize Spark session with increased memory
spark = SparkSession.builder \
    .appName("DataProfiling") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Adjust Spark settings
spark.conf.set("spark.sql.debug.maxToStringFields", "100")

print("Data Loading...")

# Load dataset
file_path = "data_generated.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Check if DataFrame is empty
if df.isEmpty():
    print("Dataset is empty. Exiting...")
    sys.exit()

# Replace dots in column names with underscores
df = df.toDF(*[c.replace(".", "_") for c in df.columns])

# Convert time_stamp column to TimestampType
df = df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

# Handle missing values
numeric_columns = [c for c, t in df.dtypes if t in ('int', 'double')]

# Replace negative values with NULL
for column in numeric_columns:
    df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

# Handling Outliers using Z-score method
for column in numeric_columns:
    stats = df.select(mean(col(column)).alias("mean"), stddev(col(column)).alias("stddev")).collect()
    mean_val = stats[0]["mean"]
    stddev_val = stats[0]["stddev"]

    if stddev_val is not None and stddev_val > 0.01:  # Adjust threshold
        df = df.withColumn(column, when((col(column) - lit(mean_val)) / lit(stddev_val) > 3, lit(None)).otherwise(col(column)))


# Compute mean values for numeric columns
mean_values = {col_name: df.select(mean(col(col_name))).collect()[0][0] for col_name in numeric_columns}
mean_values = {k: v for k, v in mean_values.items() if v is not None}

# Apply fillna only if there are valid mean values
if mean_values:
    df = df.fillna(mean_values)

# Round real number values to 2 decimal places
for column in numeric_columns:
    df = df.withColumn(column, round(col(column), 2))

# Force execution to avoid lazy evaluation issues
df.cache()
df.count()

# Save cleaned data to CSV
output_path = "final_output"
df.write.csv(output_path, header=True, mode="overwrite")

print("Process Completed Successfully.")

# Close logging file
sys.stdout.close()