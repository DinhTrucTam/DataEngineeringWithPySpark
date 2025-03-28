from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan, mean, stddev, lit, round
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

# Load dataset
file_path = "generated_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Replace dots in column names with underscores
df = df.toDF(*[c.replace(".", "_") for c in df.columns])

# Data Profiling
print("Data Schema:")
df.printSchema()

print("Data Summary:")
df.describe().show()

# Count missing values, excluding timestamp columns
missing_counts = df.select([
    count(when(col(f"`{c}`").isNull(), c)).alias(c) if t != 'time_stamp' else count(when(col(f"`{c}`").isNull(), c)).alias(c)
    for c, t in df.dtypes
])
print("Missing Values:")
missing_counts.show()

# Data Cleaning
# Convert time_stamp to TimestampType
df = df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

# Replace negative values with null
numeric_columns = [c for c, t in df.dtypes if t in ('int', 'double')]
for column in numeric_columns:
    df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

# Handling Outliers using Z-score method
for column in numeric_columns:
    stats = df.select(mean(col(column)).alias("mean"), stddev(col(column)).alias("stddev")).collect()
    mean_val = stats[0]["mean"]
    stddev_val = stats[0]["stddev"]

    # Skip columns where stddev is None (to avoid division by None)
    if stddev_val is not None and stddev_val != 0:
        df = df.withColumn(column, when((col(column) - lit(mean_val)) / lit(stddev_val) > 3, lit(None)).otherwise(col(column)))


# Compute mean values for numeric columns, ensuring None values are handled
mean_values = {col_name: df.select(mean(col(col_name))).collect()[0][0] for col_name in numeric_columns}

# Remove any None values from the dictionary
mean_values = {k: v for k, v in mean_values.items() if v is not None}

# Apply fillna only if there are valid mean values
if mean_values:
    df = df.fillna(mean_values)

# Round real number values to 2 decimal places
for column in numeric_columns:
    df = df.withColumn(column, round(col(column), 2))

# Data Validation
print("Cleaned Data Schema:")
df.printSchema()

print("Sample Data After Cleaning:")
df.show(5)

# Save cleaned data
df.write.csv("cleaned_dataset.csv", header=True, mode="overwrite")
