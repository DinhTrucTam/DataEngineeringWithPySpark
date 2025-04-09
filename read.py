from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, lit, round, to_timestamp, year, month, dayofmonth, hour, minute
from pyspark.sql.types import TimestampType
import sys, os

def main():
    # Initialize Spark session with increased memory
    spark = SparkSession.builder \
        .appName("DataProfiling") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars", "/F:/PostgreSQL/JDBC/postgresql-42.7.2.jar") \
        .getOrCreate()

    pg_url = "jdbc:postgresql://localhost:5432/DW"

    pg_properties = {
        "user": "postgres",
        "password": "tructam2992",
        "driver": "org.postgresql.Driver"
    }
    
    # Adjust Spark settings
    spark.conf.set("spark.sql.debug.maxToStringFields", "100")

    print("Data Loading...")

    # Load dataset
    file_path = "./include/source_data_manufacturing_part_*.csv"
    df_sample = spark.read.csv(file_path, header=True, inferSchema=True)
    df_sample.show(5)

    df_sample2 = spark.read.csv('./include/machine.csv', header=True, inferSchema=True)
    
    # Check if DataFrame is empty
    if df_sample.isEmpty():
        print("Dataset is empty. Exiting...")
        sys.exit()

    # Replace dots in column names with underscores
    df_sample = df_sample.toDF(*[c.replace(".", "_") for c in df_sample.columns])

    # Convert time_stamp column to TimestampType
    df_sample = df_sample.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

    # Handle missing values
    numeric_columns = [c for c, t in df_sample.dtypes if t in ('int', 'double')]

    # Replace negative values with NULL
    for column in numeric_columns:
        df_sample = df_sample.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

    # Handling Outliers using Z-score method
    for column in numeric_columns:
        stats = df_sample.select(mean(col(column)).alias("mean"), stddev(col(column)).alias("stddev")).collect()
        mean_val = stats[0]["mean"]
        stddev_val = stats[0]["stddev"]

        if stddev_val is not None and stddev_val > 0.01:  # Adjust threshold
            df_sample = df_sample.withColumn(column, when((col(column) - lit(mean_val)) / lit(stddev_val) > 3, lit(None)).otherwise(col(column)))


    # Compute mean values for numeric columns
    mean_values = {col_name: df_sample.select(mean(col(col_name))).collect()[0][0] for col_name in numeric_columns}
    mean_values = {k: v for k, v in mean_values.items() if v is not None}

    # Apply fillna only if there are valid mean values
    if mean_values:
        df_sample = df_sample.fillna(mean_values)

    # Round real number values to 2 decimal places
    for column in numeric_columns:
        df_sample = df_sample.withColumn(column, round(col(column), 2))

    # Force execution to avoid lazy evaluation issues
    df_sample.cache()
    df_sample.count()

    df_sample.show(10)
    # # Save the cleaned data into 5 separate files under the ./include directory
    # output_path = "./include/output"
    # df_sample.write.csv(output_path, header=True, mode="overwrite")

    # # To ensure Spark saves as 5 separate files, we will rename the part files
    # # Spark will create part files like part-0000, part-0001, ...; we can rename them accordingly
    # output_dir = os.path.join(output_path, "part-0000")
    # num_parts = 5  # Adjust the number of parts if needed
    # for i in range(num_parts):
    #     # Rename part files from part-0000, part-0001... to source_data_part_1.csv, source_data_part_2.csv, etc.
    #     os.rename(
    #         os.path.join(output_dir, f"part-{i:04d}"),
    #         os.path.join(output_dir, f"source_data_part_{i + 1}.csv")
    #     )

    # print("Process Completed Successfully.")
    
    df_machine = df_sample2.select(
    col("Machine_Id").alias("machine_id"),
    col("Machine_Name").alias("machine_name"),
    col("Machine_Type").alias("machine_type"),
    col("Stage").alias("stage"),
    to_timestamp(col("Last_Maintenance_Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("last_maintenance_date")
    )
    df_machine.write.jdbc(pg_url, "Dim_Machine", mode="append", properties=pg_properties)

    df_time = df_sample.select(
    to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("time_stamp"),
    year(to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("year"),
    month(to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("month"),
    dayofmonth(to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("day"),
    hour(to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("hour"),
    minute(to_timestamp(col("time_stamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("minute")
    )
    df_time.write.jdbc(pg_url, "Dim_Time", mode="append", properties=pg_properties)

    # Loading Ambient Conditions
    df_ambient = df_sample.select(
    col("AmbientConditions_AmbientHumidity_U_Actual").alias("ambient_humidity"),
    col("AmbientConditions_AmbientTemperature_U_Actual").alias("ambient_temperature")
    )
    df_ambient.write.jdbc(pg_url, "Dim_Ambient_Conditions", mode="append", properties=pg_properties)

    # Loading Machine 1 - Motor
    df_machine1_motor = df_sample.select(
        col("Machine1_MotorAmperage_U_Actual").alias("motor_amperage"),
        col("Machine1_MotorRPM_C_Actual").alias("motor_rpm")
    ).withColumn("machine_id", lit(1))
    df_machine1_motor.write.jdbc(pg_url, "Dim_Machine1_Motor", mode="append", properties=pg_properties)

    # Loading Machine 2 - Motor
    df_machine2_motor = df_sample.select(
        col("Machine2_MotorAmperage_U_Actual").alias("motor_amperage"),
        col("Machine2_MotorRPM_C_Actual").alias("motor_rpm")
    ).withColumn("machine_id", lit(2))
    df_machine2_motor.write.jdbc(pg_url, "Dim_Machine2_Motor", mode="append", properties=pg_properties)

    # Loading Machine 3 - Motor
    df_machine3_motor = df_sample.select(
        col("Machine3_MotorAmperage_U_Actual").alias("motor_amperage"),
        col("Machine3_MotorRPM_C_Actual").alias("motor_rpm")
    ).withColumn("machine_id", lit(3))
    df_machine3_motor.write.jdbc(pg_url, "Dim_Machine3_Motor", mode="append", properties=pg_properties)

    # Loading Machine 1 - Zone Temperature
    df_machine1_zone_temp = df_sample.select(
        col("Machine1_Zone1Temperature_C_Actual").alias("zone1_temperature"),
        col("Machine1_Zone2Temperature_C_Actual").alias("zone2_temperature")
    ).withColumn("machine_id", lit(1))
    df_machine1_zone_temp.write.jdbc(pg_url, "Dim_Machine1_Zone_Temperature", mode="append", properties=pg_properties)

    # Loading Machine 2 - Zone Temperature
    df_machine2_zone_temp = df_sample.select(
        col("Machine2_Zone1Temperature_C_Actual").alias("zone1_temperature"),
        col("Machine2_Zone2Temperature_C_Actual").alias("zone2_temperature")
    ).withColumn("machine_id", lit(2))
    df_machine2_zone_temp.write.jdbc(pg_url, "Dim_Machine2_Zone_Temperature", mode="append", properties=pg_properties)

    # Loading Machine 3 - Zone Temperature
    df_machine3_zone_temp = df_sample.select(
        col("Machine3_Zone1Temperature_C_Actual").alias("zone1_temperature"),
        col("Machine3_Zone2Temperature_C_Actual").alias("zone2_temperature")
    ).withColumn("machine_id", lit(3))
    df_machine3_zone_temp.write.jdbc(pg_url, "Dim_Machine3_Zone_Temperature", mode="append", properties=pg_properties)

    # Loading Machine 1 - Material Properties
    df_machine1_material_properties = df_sample.select(
        col("Machine1_RawMaterial_Property1").alias("raw_material_property1"),
        col("Machine1_RawMaterial_Property2").alias("raw_material_property2"),
        col("Machine1_RawMaterial_Property3").alias("raw_material_property3"),
        col("Machine1_RawMaterial_Property4").alias("raw_material_property4")
    ).withColumn("machine_id", lit(1))
    df_machine1_material_properties.write.jdbc(pg_url, "Dim_Machine1_Material_Properties", mode="append", properties=pg_properties)

    # Loading Machine 2 - Material Properties
    df_machine2_material_properties = df_sample.select(
        col("Machine2_RawMaterial_Property1").alias("raw_material_property1"),
        col("Machine2_RawMaterial_Property2").alias("raw_material_property2"),
        col("Machine2_RawMaterial_Property3").alias("raw_material_property3"),
        col("Machine2_RawMaterial_Property4").alias("raw_material_property4")
    ).withColumn("machine_id", lit(2))
    df_machine2_material_properties.write.jdbc(pg_url, "Dim_Machine2_Material_Properties", mode="append", properties=pg_properties)

    # Loading Machine 3 - Material Properties
    df_machine3_material_properties = df_sample.select(
        col("Machine3_RawMaterial_Property1").alias("raw_material_property1"),
        col("Machine3_RawMaterial_Property2").alias("raw_material_property2"),
        col("Machine3_RawMaterial_Property3").alias("raw_material_property3"),
        col("Machine3_RawMaterial_Property4").alias("raw_material_property4")
    ).withColumn("machine_id", lit(3))
    df_machine3_material_properties.write.jdbc(pg_url, "Dim_Machine3_Material_Properties", mode="append", properties=pg_properties)

    # Loading Combiner - Temperature
    df_combiner_temperature = df_sample.select(
        col("FirstStage_CombinerOperation_Temperature1_U_Actual").alias("temperature1"),
        col("FirstStage_CombinerOperation_Temperature2_U_Actual").alias("temperature2"),
        col("FirstStage_CombinerOperation_Temperature3_C_Actual").alias("temperature3")
    )
    df_combiner_temperature.write.jdbc(pg_url, "Dim_Combiner_Temperature", mode="append", properties=pg_properties)

    # Loading First Stage - Measurement
    df_first_stage_measurement = df_sample.select(
        col("Stage1_Output_Measurement0_U_Actual").alias("measurement0_actual"),
        col("Stage1_Output_Measurement1_U_Actual").alias("measurement1_actual"),
        col("Stage1_Output_Measurement2_U_Actual").alias("measurement2_actual"),
        col("Stage1_Output_Measurement3_U_Actual").alias("measurement3_actual"),
        col("Stage1_Output_Measurement4_U_Actual").alias("measurement4_actual"),
        col("Stage1_Output_Measurement5_U_Actual").alias("measurement5_actual"),
        col("Stage1_Output_Measurement6_U_Actual").alias("measurement6_actual"),
        col("Stage1_Output_Measurement7_U_Actual").alias("measurement7_actual"),
        col("Stage1_Output_Measurement8_U_Actual").alias("measurement8_actual"),
        col("Stage1_Output_Measurement9_U_Actual").alias("measurement9_actual"),
        col("Stage1_Output_Measurement10_U_Actual").alias("measurement10_actual"),
        col("Stage1_Output_Measurement11_U_Actual").alias("measurement11_actual"),
        col("Stage1_Output_Measurement12_U_Actual").alias("measurement12_actual"),
        col("Stage1_Output_Measurement13_U_Actual").alias("measurement13_actual"),
        col("Stage1_Output_Measurement14_U_Actual").alias("measurement14_actual")
    )
    df_first_stage_measurement.write.jdbc(pg_url, "Dim_First_Stage_Actual", mode="append", properties=pg_properties)

    # Loading First Stage - Setpoint
    df_first_stage_setpoint = df_sample.select(
        col("Stage1_Output_Measurement0_U_Setpoint").alias("measurement0_setpoint"),
        col("Stage1_Output_Measurement1_U_Setpoint").alias("measurement1_setpoint"),
        col("Stage1_Output_Measurement2_U_Setpoint").alias("measurement2_setpoint"),
        col("Stage1_Output_Measurement3_U_Setpoint").alias("measurement3_setpoint"),
        col("Stage1_Output_Measurement4_U_Setpoint").alias("measurement4_setpoint"),
        col("Stage1_Output_Measurement5_U_Setpoint").alias("measurement5_setpoint"),
        col("Stage1_Output_Measurement6_U_Setpoint").alias("measurement6_setpoint"),
        col("Stage1_Output_Measurement7_U_Setpoint").alias("measurement7_setpoint"),
        col("Stage1_Output_Measurement8_U_Setpoint").alias("measurement8_setpoint"),
        col("Stage1_Output_Measurement9_U_Setpoint").alias("measurement9_setpoint"),
        col("Stage1_Output_Measurement10_U_Setpoint").alias("measurement10_setpoint"),
        col("Stage1_Output_Measurement11_U_Setpoint").alias("measurement11_setpoint"),
        col("Stage1_Output_Measurement12_U_Setpoint").alias("measurement12_setpoint"),
        col("Stage1_Output_Measurement13_U_Setpoint").alias("measurement13_setpoint"),
        col("Stage1_Output_Measurement14_U_Setpoint").alias("measurement14_setpoint")
    )
    df_first_stage_setpoint.write.jdbc(pg_url, "Dim_First_Stage_Setpoint", mode="append", properties=pg_properties)

    # Loading Machine 4 - Temperature & Pressure
    df_machine4_temperature_pressure = df_sample.select(
        col("Machine4_Temperature1_C_Actual").alias("temperature1"),
        col("Machine4_Temperature2_C_Actual").alias("temperature2"),
        col("Machine4_Pressure_C_Actual").alias("pressure"),
        col("Machine4_Temperature3_C_Actual").alias("temperature3"),
        col("Machine4_Temperature4_C_Actual").alias("temperature4"),
        col("Machine4_Temperature5_C_Actual").alias("temperature5")
    ).withColumn("machine_id", lit(4))
    df_machine4_temperature_pressure.write.jdbc(pg_url, "Dim_Machine4_Temperature_Pressure", mode="append", properties=pg_properties)

    # Loading Machine 5 - Temperature
    df_machine5_temperature = df_sample.select(
        col("Machine5_Temperature1_C_Actual").alias("temperature1"),
        col("Machine5_Temperature2_C_Actual").alias("temperature2"),
        col("Machine5_Temperature3_C_Actual").alias("temperature3"),
        col("Machine5_Temperature4_C_Actual").alias("temperature4"),
        col("Machine5_Temperature5_C_Actual").alias("temperature5"),
        col("Machine5_Temperature6_C_Actual").alias("temperature6")
    ).withColumn("machine_id", lit(5))
    df_machine5_temperature.write.jdbc(pg_url, "Dim_Machine5_Temperature", mode="append", properties=pg_properties)

    # Loading Exit Temperature
    df_exit_temperature = df_sample.select(
        col("Machine4_ExitTemperature_U_Actual").alias("machine4_exit_temperature"),
        col("Machine5_ExitTemperature_U_Actual").alias("machine5_exit_temperature")
    )
    df_exit_temperature.write.jdbc(pg_url, "Dim_Exit_Temperature", mode="append", properties=pg_properties)

    # Loading Second Stage - Measurement
    df_second_stage_measurement = df_sample.select(
        col("Stage2_Output_Measurement0_U_Actual").alias("measurement0_actual"),
        col("Stage2_Output_Measurement1_U_Actual").alias("measurement1_actual"),
        col("Stage2_Output_Measurement2_U_Actual").alias("measurement2_actual"),
        col("Stage2_Output_Measurement3_U_Actual").alias("measurement3_actual"),
        col("Stage2_Output_Measurement4_U_Actual").alias("measurement4_actual"),
        col("Stage2_Output_Measurement5_U_Actual").alias("measurement5_actual"),
        col("Stage2_Output_Measurement6_U_Actual").alias("measurement6_actual"),
        col("Stage2_Output_Measurement7_U_Actual").alias("measurement7_actual"),
        col("Stage2_Output_Measurement8_U_Actual").alias("measurement8_actual"),
        col("Stage2_Output_Measurement9_U_Actual").alias("measurement9_actual"),
        col("Stage2_Output_Measurement10_U_Actual").alias("measurement10_actual"),
        col("Stage2_Output_Measurement11_U_Actual").alias("measurement11_actual"),
        col("Stage2_Output_Measurement12_U_Actual").alias("measurement12_actual"),
        col("Stage2_Output_Measurement13_U_Actual").alias("measurement13_actual"),
        col("Stage2_Output_Measurement14_U_Actual").alias("measurement14_actual")
    )
    df_second_stage_measurement.write.jdbc(pg_url, "Dim_Second_Stage_Actual", mode="append", properties=pg_properties)

    # Loading Second Stage - Setpoint
    df_second_stage_setpoint = df_sample.select(
        col("Stage2_Output_Measurement0_U_Setpoint").alias("measurement0_setpoint"),
        col("Stage2_Output_Measurement1_U_Setpoint").alias("measurement1_setpoint"),
        col("Stage2_Output_Measurement2_U_Setpoint").alias("measurement2_setpoint"),
        col("Stage2_Output_Measurement3_U_Setpoint").alias("measurement3_setpoint"),
        col("Stage2_Output_Measurement4_U_Setpoint").alias("measurement4_setpoint"),
        col("Stage2_Output_Measurement5_U_Setpoint").alias("measurement5_setpoint"),
        col("Stage2_Output_Measurement6_U_Setpoint").alias("measurement6_setpoint"),
        col("Stage2_Output_Measurement7_U_Setpoint").alias("measurement7_setpoint"),
        col("Stage2_Output_Measurement8_U_Setpoint").alias("measurement8_setpoint"),
        col("Stage2_Output_Measurement9_U_Setpoint").alias("measurement9_setpoint"),
        col("Stage2_Output_Measurement10_U_Setpoint").alias("measurement10_setpoint"),
        col("Stage2_Output_Measurement11_U_Setpoint").alias("measurement11_setpoint"),
        col("Stage2_Output_Measurement12_U_Setpoint").alias("measurement12_setpoint"),
        col("Stage2_Output_Measurement13_U_Setpoint").alias("measurement13_setpoint"),
        col("Stage2_Output_Measurement14_U_Setpoint").alias("measurement14_setpoint")
    )
    df_second_stage_setpoint.write.jdbc(pg_url, "Dim_Second_Stage_Setpoint", mode="append", properties=pg_properties)


    ## Loading fact tables
    # Loading Fact - Stage 1 - Operation
    df_first_stage_operation = df_sample.select(
        col("No").alias("time_id"),  
        
        col("No").alias("ambient_id"), 
        
        col("No").alias("machine1_motor_id"),  
        col("No").alias("machine2_motor_id"), 
        col("No").alias("machine3_motor_id"),  
        
        col("No").alias("machine1_zone_temp_id"), 
        col("No").alias("machine2_zone_temp_id"),  
        col("No").alias("machine3_zone_temp_id"), 
        
        col("No").alias("machine1_material_id"),  
        col("No").alias("machine2_material_id"), 
        col("No").alias("machine3_material_id"),
        
        col("No").alias("combiner_temp_id"),
        
        ((col("Machine1_MotorAmperage_U_Actual") + 
            col("Machine2_MotorAmperage_U_Actual") + 
            col("Machine3_MotorAmperage_U_Actual")) / 3).alias("avg_motor_amperage"),

        ((col("Machine1_MotorRPM_C_Actual") + 
            col("Machine2_MotorRPM_C_Actual") + 
            col("Machine3_MotorRPM_C_Actual")) / 3).alias("avg_motor_rpm"),
        
        ((col("Machine1_MaterialPressure_U_Actual") +
            col("Machine2_MaterialPressure_U_Actual") +
            col("Machine3_MaterialPressure_U_Actual")) / 3).alias("avg_material_pressure"),
        
        ((col("Machine1_MaterialTemperature_U_Actual") +
            col("Machine2_MaterialTemperature_U_Actual") +
            col("Machine3_MaterialTemperature_U_Actual")) / 3).alias("avg_material_temperature"),
        
        ((col("Machine1_ExitZoneTemperature_C_Actual") +
            col("Machine2_ExitZoneTemperature_C_Actual") +
            col("Machine3_ExitZoneTemperature_C_Actual")) / 3).alias("avg_exit_zone_temp")
    )
    df_first_stage_operation.write.jdbc(pg_url, "Fact_Stage1_Operation", mode="append", properties=pg_properties)

    # Loading Fact - Stage 1 - Output
    df_first_stage_output = df_sample.select(
        col("No").alias("time_id"),  
        
        col("No").alias("ambient_id"), 
        
        col("No").alias("first_stage_actual_id"),
        
        col("No").alias("first_stage_setpoint_id"),
        
        ((col("Stage1_Output_Measurement0_U_Actual") +
            col("Stage1_Output_Measurement1_U_Actual") +
            col("Stage1_Output_Measurement2_U_Actual") +
            col("Stage1_Output_Measurement3_U_Actual") +
            col("Stage1_Output_Measurement4_U_Actual") +
            col("Stage1_Output_Measurement5_U_Actual") +
            col("Stage1_Output_Measurement6_U_Actual") +
            col("Stage1_Output_Measurement7_U_Actual") +
            col("Stage1_Output_Measurement8_U_Actual") +
            col("Stage1_Output_Measurement9_U_Actual") +
            col("Stage1_Output_Measurement10_U_Actual") +
            col("Stage1_Output_Measurement11_U_Actual") +
            col("Stage1_Output_Measurement12_U_Actual") +
            col("Stage1_Output_Measurement13_U_Actual") +
            col("Stage1_Output_Measurement14_U_Actual")) / 15).alias("avg_measurement_actual"),
        
        # add a column avg_setpoint to the fact table fact_stage1_output
        ((col("Stage1_Output_Measurement0_U_Setpoint") +
            col("Stage1_Output_Measurement1_U_Setpoint") +
            col("Stage1_Output_Measurement2_U_Setpoint") +
            col("Stage1_Output_Measurement3_U_Setpoint") +
            col("Stage1_Output_Measurement4_U_Setpoint") +
            col("Stage1_Output_Measurement5_U_Setpoint") +
            col("Stage1_Output_Measurement6_U_Setpoint") +
            col("Stage1_Output_Measurement7_U_Setpoint") +
            col("Stage1_Output_Measurement8_U_Setpoint") +
            col("Stage1_Output_Measurement9_U_Setpoint") +
            col("Stage1_Output_Measurement10_U_Setpoint") +
            col("Stage1_Output_Measurement11_U_Setpoint") +
            col("Stage1_Output_Measurement12_U_Setpoint") +
            col("Stage1_Output_Measurement13_U_Setpoint") +
            col("Stage1_Output_Measurement14_U_Setpoint")) / 15).alias("avg_setpoint"),
    )
    df_first_stage_output.write.jdbc(pg_url, "Fact_Stage1_Output", mode="append", properties=pg_properties)

    # Loading Fact - Stage 2 - Operation
    df_second_stage_operation = df_sample.select(
        col("No").alias("time_id"),  
        
        col("No").alias("ambient_id"),
        
        col("No").alias("machine4_temp_pressure_id"),
        
        col("No").alias("machine5_temp_id"),
        
        col("No").alias("exit_temp_id"),
        
        # add a column avg_machine4_temperature and avg_machine5_temperature to the fact table fact_stage2_operation
        ((col("Machine4_Temperature1_C_Actual") +
            col("Machine4_Temperature2_C_Actual") +
            col("Machine4_Temperature3_C_Actual") +
            col("Machine4_Temperature4_C_Actual") +
            col("Machine4_Temperature5_C_Actual")) / 5).alias("avg_machine4_temperature"),
        
        ((col("Machine5_Temperature1_C_Actual") +
            col("Machine5_Temperature2_C_Actual") +
            col("Machine5_Temperature3_C_Actual") +
            col("Machine5_Temperature4_C_Actual") +
            col("Machine5_Temperature5_C_Actual") +
            col("Machine5_Temperature6_C_Actual")) / 6).alias("avg_machine5_temperature"),
        
        ((col("Machine4_ExitTemperature_U_Actual") +
            col("Machine5_ExitTemperature_U_Actual")) / 2).alias("avg_exit_temperature")
    )
    df_second_stage_operation.write.jdbc(pg_url, "Fact_Stage2_Operation", mode="append", properties=pg_properties)

    # Loading Fact - Stage 2 - Output
    df_second_stage_output = df_sample.select(
        col("No").alias("time_id"),  
        
        col("No").alias("ambient_id"), 
        
        col("No").alias("second_stage_actual_id"),
        
        col("No").alias("second_stage_setpoint_id"),
        
        ((col("Stage2_Output_Measurement0_U_Actual") +
            col("Stage2_Output_Measurement1_U_Actual") +
            col("Stage2_Output_Measurement2_U_Actual") +
            col("Stage2_Output_Measurement3_U_Actual") +
            col("Stage2_Output_Measurement4_U_Actual") +
            col("Stage2_Output_Measurement5_U_Actual") +
            col("Stage2_Output_Measurement6_U_Actual") +
            col("Stage2_Output_Measurement7_U_Actual") +
            col("Stage2_Output_Measurement8_U_Actual") +
            col("Stage2_Output_Measurement9_U_Actual") +
            col("Stage2_Output_Measurement10_U_Actual") +
            col("Stage2_Output_Measurement11_U_Actual") +
            col("Stage2_Output_Measurement12_U_Actual") +
            col("Stage2_Output_Measurement13_U_Actual") +
            col("Stage2_Output_Measurement14_U_Actual")) / 15).alias("avg_measurement_actual"),
        
        # add a column avg_setpoint to the fact table fact_stage2_output
        ((col("Stage2_Output_Measurement0_U_Setpoint") +
            col("Stage2_Output_Measurement1_U_Setpoint") +
            col("Stage2_Output_Measurement2_U_Setpoint") +
            col("Stage2_Output_Measurement3_U_Setpoint") +
            col("Stage2_Output_Measurement4_U_Setpoint") +
            col("Stage2_Output_Measurement5_U_Setpoint") +
            col("Stage2_Output_Measurement6_U_Setpoint") +
            col("Stage2_Output_Measurement7_U_Setpoint") +
            col("Stage2_Output_Measurement8_U_Setpoint") +
            col("Stage2_Output_Measurement9_U_Setpoint") +
            col("Stage2_Output_Measurement10_U_Setpoint") +
            col("Stage2_Output_Measurement11_U_Setpoint") +
            col("Stage2_Output_Measurement12_U_Setpoint") +
            col("Stage2_Output_Measurement13_U_Setpoint") +
            col("Stage2_Output_Measurement14_U_Setpoint")) / 15).alias("avg_setpoint"),
    )
    df_second_stage_output.write.jdbc(pg_url, "Fact_Stage2_Output", mode="append", properties=pg_properties)

if __name__ == "__main__":
    main()