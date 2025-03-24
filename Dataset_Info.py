import pandas as pd

# Load the dataset
df = pd.read_excel("generated_data.xlsx")

# Display basic information
print(df.info())

# Show the first few rows
print(df.head())
