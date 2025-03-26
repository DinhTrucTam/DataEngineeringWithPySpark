import pyspark
from pyspark.sql import SparkSession

# Part 1:
# Start the Spark session
# spark=SparkSession.builder.appName('Practise').getOrCreate()

# df_pyspark = spark.read.csv('test1.csv')
# df_pyspark.show()

# spark.read.option('header', 'true').csv('test1.csv').show()
# df_pyspark = spark.read.option('header', 'true').csv('test1.csv')
# print(type(df_pyspark))

# df_pyspark.head()
# df_pyspark.printSchema()

# Part 2:
# PySpark Dataframe
# Reading the Dataset: remember to add header=True and inferSchema=True
# Checking the Datatypes of the column (Schema): using .printSchema() function
# Selecting columns and indexing
# Checking describe option similar to Pandas
# Adding columns
# Dropping columns
spark = SparkSession.builder.appName('Dataframe').getOrCreate()

### Read the dataset.
## 1st way: 
#   By default, PySpark reads every datatype as STRING
# df_pyspark = spark.read.option('header', 'true').csv('test1.csv')
# df_pyspark.printSchema()
##  Put it the paramter 'inferSchema'=True to ensure datatypes are read correctly
# df_pyspark = spark.read.option('header', 'true').csv('test1.csv', inferSchema=True)
# df_pyspark.printSchema()

## 2nd way:
df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema=True)
# df_pyspark.printSchema()

## Datatypes: Dataframe
print('Datatype: ', type(df_pyspark))

### Selecting columns and indexing
## Selecting columns
print(df_pyspark.columns)
print(df_pyspark.head(3))

df_pyspark.show()
df_pyspark.select('Name').show()

df_pyspark.select(['Name', 'Experience']).show()

df_pyspark.select(['Age', 'Experience']).show()

print(df_pyspark.dtypes)

df_pyspark.describe().show()


### Adding columns and dropping columns
## Adding columns
df_pyspark.withColumn('Experience After 2 Years', df_pyspark['Experience'] + 2).show()
## Dropping columns
df_pyspark.drop('Experience After 2 Years')

## Renaming the columns
df_pyspark.withColumnRenamed('Name', 'New Name')


# Part 3: Handling Missing Values
# Dropping Columns
# Dropping Rows containing NULL values: 
#   .na.drop(how="all"/"any"): all means all values of rows should be NULL to get deleted
#   .na.drop(how="any", thresh=2): at least 2 NON-NULL values in the row.
#   .na.drop(how="any", subset=['Experience'])
# Various Parameter in Dropping functionalities
# Handling Missing values by Mean, MEdian and Mode

