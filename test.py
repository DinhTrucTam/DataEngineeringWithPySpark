import pandas as pd
import re

# Load the CSV file
file_path = r'continuous_factory_process.csv'  # Replace with your file path
df = pd.read_csv(file_path)
# Define the regular expression pattern to match the column format
pattern = r"^Stage\d+\.Output\.Measurement\d+\.U\.Actual$"

# Find all columns that match the pattern
matching_columns = [column for column in df.columns if re.match(pattern, column)]

# Print the matching columns and the count
print("Matching columns:")
print(matching_columns)
print("Number of matching columns:", len(matching_columns))
# 1. List all columns
print("Columns in the dataset:")
print(df.columns.tolist())
print('Number of columns: ', df.columns.size)
count = 0

# 2. Find the smallest and largest values in each column and print only if smallest value < 0
for column in df.columns:
    if pd.api.types.is_numeric_dtype(df[column]):  # Check if the column is numeric
        min_value = df[column].min()
        max_value = df[column].max()

        # Only print if the minimum value is less than 0
        if min_value == max_value:
            count += 1
            print(f"Column: {column}")
            print(f"  Smallest value: {min_value}")
            print(f"  Largest value: {max_value}")
    else:
        print(f"Column: {column} is non-numeric, skipping min/max calculation.")
print("No of columns having smallest values < 0: ", count)