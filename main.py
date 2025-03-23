from Summarizing_Current_Dataset import summarize_dataset

# Define the file path
file_path = r'C:\Users\HP\Downloads\continuous_factory_process.csv'  # Replace with your file path

# Call the function and get the min/max values
min_max_values = summarize_dataset(file_path)

# Print the min_max_values for verification
print("\nMin/Max values for each numeric column:")
print(min_max_values)