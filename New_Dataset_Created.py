import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from Summarizing_Current_Dataset import summarize_dataset

# Step 1: Create new time_stamp values from "3/6/2019 00:00:00 AM" to "3/6/2019 11:59:59 PM"
start_time = datetime(2019, 3, 6, 0, 0, 0)  # "3/6/2019 00:00:00 AM"
end_time = datetime(2019, 3, 6, 23, 59, 59)  # "3/6/2019 11:59:59 PM"
# Generate the time stamps every second
time_stamps = pd.date_range(start=start_time, end=end_time, freq='S')
# Define the file path
file_path = r'C:\Users\HP\Downloads\continuous_factory_process.csv'


# Step 2: Generate new data for each column based on min/max values
# Call the function and get the min/max values
min_max_values = summarize_dataset(file_path)

# # Step 3: Create synthetic data for each column with adjusted distribution
# new_data = {}
#
# for i, (column, (min_val, max_val)) in enumerate(min_max_values.items()):
#     # Check if the column follows the Stage<x>.Output.Measurement<y>.U.Actual pattern
#     if re.match(r"^Stage\d+\.Output\.Measurement\d+\.U\.Actual$", column):
#
#         # Find the "Setpoint" column that follows the current column
#         if i + 1 < len(min_max_values):
#             next_column = list(min_max_values.keys())[i + 1]
#             if "Setpoint" in next_column:  # Confirm that it's a Setpoint column
#                 setpoint_max_val = min_max_values[next_column][1]  # Get max value of the next Setpoint column
#             else:
#                 setpoint_max_val = max_val  # Default to current column's max value if no Setpoint column is found
#         else:
#             setpoint_max_val = max_val  # Default to current column's max value if it's the last column
#
#         # Define the number of values based on the length of time_stamps
#         total_values = len(time_stamps)
#
#         # 95% of values will be in the range (setpoint_max_val - 0.5, setpoint_max_val + 0.5)
#         num_majority = int(0.95 * total_values)
#         majority_values = np.random.uniform(setpoint_max_val - 0.5, setpoint_max_val + 0.5, num_majority)
#
#         # 1% of values will be 0s
#         num_zeros = int(0.01 * total_values)
#         zeros = np.zeros(num_zeros)
#
#         # 2% of values will be approximately equal to the max value of the current column
#         num_max_value = int(0.02 * total_values)
#         max_value_approx = np.full(num_max_value, max_val)
#
#         # 2% of values will be in the range (min_val, 0)
#         num_negative = int(0.02 * total_values)
#         negative_values = np.random.uniform(min_val, 0, num_negative)
#
#         # Combine all values
#         combined_values = np.concatenate([majority_values, zeros, max_value_approx, negative_values])
#
#         # Shuffle the combined values to distribute them randomly
#         np.random.shuffle(combined_values)
#
#         # Store the generated values in the dictionary
#         new_data[column] = combined_values
#
#     # For "Setpoint" columns (already handled previously)
#     elif "Setpoint" in column:  # If the column contains "Setpoint"
#         # Special handling for Stage2.Output.Measurement<y>.U.Setpoint format
#         if "Stage2.Output.Measurement" in column and "Setpoint" in column:
#             # For this column, all values will be equal to max_value
#             new_data[column] = np.full(len(time_stamps), max_val)  # Set all values to max_value
#         else:
#             num_zero = int(0.00397473206 * len(time_stamps))  # 0.397473206% of the total data points
#             num_max = len(time_stamps) - num_zero  # Remaining values should be equal to max_val
#
#             # Generate values:
#             setpoint_values = np.concatenate([np.zeros(num_zero), np.full(num_max, max_val)])
#
#             # Shuffle the combined values to distribute the 0s and max values randomly
#             np.random.shuffle(setpoint_values)
#
#             # Store the generated values in the dictionary
#             new_data[column] = setpoint_values
#
#     # For columns with min_val < 0
#     elif min_val < 0:  # Only apply this logic if the min value is less than 0
#         num_negative = int(0.0005 * len(time_stamps))  # 0.05% of the total data points
#         num_positive = len(time_stamps) - num_negative  # Remaining values should be >= 0
#
#         # Generate values:
#         negative_values = np.random.uniform(min_val, 0, num_negative)
#         positive_values = np.random.uniform(0, max_val, num_positive)
#
#         # Combine both negative and positive values
#         combined_values = np.concatenate([negative_values, positive_values])
#
#         # Shuffle the combined values to distribute the negative values randomly
#         np.random.shuffle(combined_values)
#
#         # Store the generated values in the dictionary
#         new_data[column] = combined_values
#
#     # For columns where the min value is >= 0, generate uniformly
#     else:
#         new_data[column] = np.random.uniform(min_val, max_val, len(time_stamps))
#
# # At this point, `new_data` will have the adjusted synthetic data for all columns.


# Step 4: Create a DataFrame with the new data and time_stamps


# Step 3: Create synthetic data for each column with adjusted distribution
new_data = {}

# Define the raw material properties data
machine_raw_materials = {
    "Machine1": {
        "Property1": [11.54, 12.9, 12.59, 12.22],
        "Property2": [200, 215, 236, 201],
        "Property3": [1027.43, 980.53, 601.11, 1048.06],
        "Property4": [247, 251, 257, 252]
    },
    "Machine2": {
        "Property1": [12.59, 12.85],
        "Property2": [236, 241],
        "Property3": [601.11, 556.7],
        "Property4": [257, 256]
    },
    "Machine3": {
        "Property1": [9.02, 9.86, 8.83],
        "Property2": [186, 192, 221],
        "Property3": [421.16, 408.97, 433.18],
        "Property4": [200, 202, 205]
    }
}

for i, (column, (min_val, max_val)) in enumerate(min_max_values.items()):
    # Check if the column follows the Stage<x>.Output.Measurement<y>.U.Actual pattern
    if re.match(r"^Stage1\.Output\.Measurement\d+\.U\.Actual$", column):
        # Find the "Setpoint" column that follows the current column
        if i + 1 < len(min_max_values):
            next_column = list(min_max_values.keys())[i + 1]
            if "Setpoint" in next_column:  # Confirm that it's a Setpoint column
                setpoint_max_val = min_max_values[next_column][1]  # Get max value of the next Setpoint column
            else:
                setpoint_max_val = max_val  # Default to current column's max value if no Setpoint column is found
        else:
            setpoint_max_val = max_val  # Default to current column's max value if it's the last column

        # Define the number of values based on the length of time_stamps
        total_values = len(time_stamps)

        # 95% of values will be in the range (setpoint_max_val - 0.5, setpoint_max_val + 0.5)
        num_majority = int(0.95 * total_values)
        majority_values = np.random.uniform(setpoint_max_val - 0.5, setpoint_max_val + 0.5, num_majority)

        # 1% of values will be 0s
        num_zeros = int(0.01 * total_values)
        zeros = np.zeros(num_zeros)

        # 2% of values will be approximately equal to the max value of the current column
        num_max_value = int(0.02 * total_values)
        max_value_approx = np.full(num_max_value, max_val)

        # 2% of values will be in the range (min_val, 0)
        num_negative = int(0.02 * total_values)
        negative_values = np.random.uniform(min_val, 0, num_negative)

        # Combine all values
        combined_values = np.concatenate([majority_values, zeros, max_value_approx, negative_values])

        # Shuffle the combined values to distribute them randomly
        np.random.shuffle(combined_values)

        # Store the generated values in the dictionary
        new_data[column] = combined_values

    elif re.match(r"^Stage2\.Output\.Measurement\d+\.U\.Actual$", column):

        # Find the "Setpoint" column that follows the current column
        if i + 1 < len(min_max_values):
            next_column = list(min_max_values.keys())[i + 1]
            if "Setpoint" in next_column:  # Confirm that it's a Setpoint column
                setpoint_max_val = min_max_values[next_column][1]  # Get max value of the next Setpoint column
                upper_limit = setpoint_max_val + 5  # Add 5 to the max value of the Setpoint column
            else:
                upper_limit = max_val + 5  # Default to current column's max value if no Setpoint column is found
        else:
            upper_limit = max_val + 5  # Default to current column's max value if it's the last column

        # Define the number of values based on the length of time_stamps
        total_values = len(time_stamps)

        # Generate values in the range [0, upper_limit]
        values = np.random.uniform(0, upper_limit, total_values)

        # Store the generated values in the dictionary
        new_data[column] = values

    # For "Setpoint" columns (already handled previously)
    elif "Setpoint" in column:  # If the column contains "Setpoint"
        # Special handling for Stage2.Output.Measurement<y>.U.Setpoint format
        if "Stage2.Output.Measurement" in column and "Setpoint" in column:
            # For this column, all values will be equal to max_value
            new_data[column] = np.full(len(time_stamps), max_val)  # Set all values to max_value
        else:
            num_zero = int(0.00397473206 * len(time_stamps))  # 0.397473206% of the total data points
            num_max = len(time_stamps) - num_zero  # Remaining values should be equal to max_val

            # Generate values:
            setpoint_values = np.concatenate([np.zeros(num_zero), np.full(num_max, max_val)])

            # Shuffle the combined values to distribute the 0s and max values randomly
            np.random.shuffle(setpoint_values)

            # Store the generated values in the dictionary
            new_data[column] = setpoint_values

    # For columns with min_val < 0
    elif min_val < 0:  # Only apply this logic if the min value is less than 0
        num_negative = int(0.0005 * len(time_stamps))  # 0.05% of the total data points
        num_positive = len(time_stamps) - num_negative  # Remaining values should be >= 0

        # Generate values:
        negative_values = np.random.uniform(min_val, 0, num_negative)
        positive_values = np.random.uniform(0, max_val, num_positive)

        # Combine both negative and positive values
        combined_values = np.concatenate([negative_values, positive_values])

        # Shuffle the combined values to distribute the negative values randomly
        np.random.shuffle(combined_values)

        # Store the generated values in the dictionary
        new_data[column] = combined_values

    # For columns where the min value is >= 0, generate uniformly
    else:
        new_data[column] = np.random.uniform(min_val, max_val, len(time_stamps))

    # Handling for machine raw materials
    # Apply rules for Machine1, Machine2, and Machine3
    if re.match(r"^Machine\d+\.RawMaterial\.Property\d+$", column):
        machine_number, property_number = re.findall(r"Machine(\d+)\.RawMaterial\.Property(\d+)", column)[0]
        machine_number = int(machine_number)
        property_number = int(property_number)

        # Retrieve the corresponding raw material properties for the machine
        machine_data = machine_raw_materials[f"Machine{machine_number}"]
        property_values = machine_data[f"Property{property_number}"]

        # Randomly select values for each timestamp without repetition
        # Randomly sample from the available property values for each timestamp
        sampled_values = np.random.choice(property_values, size=len(time_stamps), replace=True)

        # Store the generated values in the dictionary
        new_data[column] = sampled_values

# At this point, `new_data` will have the adjusted synthetic data for all columns, including the machine raw material properties.

new_data_df = pd.DataFrame(new_data)
new_data_df['time_stamp'] = time_stamps


# Step 5: Save the new DataFrame to an Excel file
new_data_df.to_excel('generated_data.xlsx', index=False)
print("Data saved to 'generated_data.xlsx'.")