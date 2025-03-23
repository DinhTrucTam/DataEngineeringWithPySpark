import pandas as pd

def summarize_dataset(file_path):
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Initialize the min_max_values dictionary
    min_max_values = {}

    # List all columns
    print("Columns in the dataset:")
    print(df.columns.tolist())
    print('Number of columns: ', df.columns.size)

    # Find the smallest and largest values in each column and store them in min_max_values
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):  # Check if the column is numeric
            min_value = df[column].min()
            max_value = df[column].max()
            min_max_values[column] = (min_value, max_value)  # Store min/max as a tuple
        else:
            print(f"Column: {column} is non-numeric, skipping min/max calculation.")

    # Return the min_max_values dictionary
    return min_max_values