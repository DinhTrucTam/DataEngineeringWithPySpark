from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_timestamp, year, month, dayofmonth, hour, minute, monotonically_increasing_id
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
    monotonically_increasing_id().alias("ambient_id"),  # Generate unique ID
    col("AmbientConditions_AmbientHumidity_U_Actual").alias("ambient_humidity"),
    col("AmbientConditions_AmbientTemperature_U_Actual").alias("ambient_temperature")
)
df_ambient.write.jdbc(pg_url, "Dim_Ambient_Conditions", mode="append", properties=pg_properties)

# Loading Machine 1 - Motor
df_machine1_motor = df_sample.select(
    col("Machine1_MotorAmperage_U_Actual").alias("motor_amperage"),
    col("Machine1_MotorRPM_C_Actual").alias("ambient_temperature")
)
df_machine1_motor.write.jdbc(pg_url, "Dim_Machine1_Motor", mode="append", properties=pg_properties)

# Loading Machine 2 - Motor
df_machine2_motor = df_sample.select(
    col("Machine2_MotorAmperage_U_Actual").alias("motor_amperage"),
    col("Machine2_MotorRPM_C_Actual").alias("ambient_temperature")
)
df_machine2_motor.write.jdbc(pg_url, "Dim_Machine2_Motor", mode="append", properties=pg_properties)

# Loading Machine 3 - Motor
df_machine3_motor = df_sample.select(
    col("Machine3_MotorAmperage_U_Actual").alias("motor_amperage"),
    col("Machine3_MotorRPM_C_Actual").alias("ambient_temperature")
)
df_machine3_motor.write.jdbc(pg_url, "Dim_Machine3_Motor", mode="append", properties=pg_properties)

# Loading Machine 1 - Zone Temperature
df_machine1_zone_temp = df_sample.select(
    col("Machine1_Zone1Temperature_C_Actual").alias("zone1_temperature"),
    col("Machine1_Zone2Temperature_C_Actual").alias("zone2_temperature")
)
df_machine1_zone_temp.write.jdbc(pg_url, "Dim_Machine1_Zone_Temperature", mode="append", properties=pg_properties)

# Loading Machine 2 - Zone Temperature
df_machine2_zone_temp = df_sample.select(
    col("Machine2_Zone1Temperature_C_Actual").alias("zone1_temperature"),
    col("Machine2_Zone2Temperature_C_Actual").alias("zone2_temperature")
)
df_machine2_zone_temp.write.jdbc(pg_url, "Dim_Machine2_Zone_Temperature", mode="append", properties=pg_properties)

# Loading Machine 3 - Zone Temperature
df_machine3_zone_temp = df_sample.select(
    col("Machine3_Zone1Temperature_C_Actual").alias("zone1_temperature"),
    col("Machine3_Zone2Temperature_C_Actual").alias("zone2_temperature")
)
df_machine3_zone_temp.write.jdbc(pg_url, "Dim_Machine3_Zone_Temperature", mode="append", properties=pg_properties)

# Loading Machine 1 - Material Properties
df_machine1_material_properties = df_sample.select(
    col("Machine1_RawMaterial_Property1").alias("raw_material_property1"),
    col("Machine1_RawMaterial_Property2").alias("raw_material_property2"),
    col("Machine1_RawMaterial_Property3").alias("raw_material_property3"),
    col("Machine1_RawMaterial_Property4").alias("raw_material_property4")
)
df_machine1_material_properties.write.jdbc(pg_url, "Dim_Machine1_Material_Properties", mode="append", properties=pg_properties)

# Loading Machine 2 - Material Properties
df_machine2_material_properties = df_sample.select(
    col("Machine2_RawMaterial_Property1").alias("raw_material_property1"),
    col("Machine2_RawMaterial_Property2").alias("raw_material_property2"),
    col("Machine2_RawMaterial_Property3").alias("raw_material_property3"),
    col("Machine2_RawMaterial_Property4").alias("raw_material_property4")
)
df_machine2_material_properties.write.jdbc(pg_url, "Dim_Machine2_Material_Properties", mode="append", properties=pg_properties)

# Loading Machine 3 - Material Properties
df_machine3_material_properties = df_sample.select(
    col("Machine3_RawMaterial_Property1").alias("raw_material_property1"),
    col("Machine3_RawMaterial_Property2").alias("raw_material_property2"),
    col("Machine3_RawMaterial_Property3").alias("raw_material_property3"),
    col("Machine3_RawMaterial_Property4").alias("raw_material_property4")
)
df_machine3_material_properties.write.jdbc(pg_url, "Dim_Machine3_Material_Properties", mode="append", properties=pg_properties)

# Loading Combiner - Temperature
df_combiner_temperature = df_sample.select(
    col("FirstStage_CombinerOperation_Temperature1_U_Actual").alias("temperature1"),
    col("FirstStage_CombinerOperation_Temperature2_U_Actual").alias("temperature2"),
    col("FirstStage_CombinerOperation_Temperature3_U_Actual").alias("temperature3")
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
    col("Stage1_Output_Setpoint0_U_Actual").alias("measurement0_setpoint"),
    col("Stage1_Output_Setpoint1_U_Actual").alias("measurement1_setpoint"),
    col("Stage1_Output_Setpoint2_U_Actual").alias("measurement2_setpoint"),
    col("Stage1_Output_Setpoint3_U_Actual").alias("measurement3_setpoint"),
    col("Stage1_Output_Setpoint4_U_Actual").alias("measurement4_setpoint"),
    col("Stage1_Output_Setpoint5_U_Actual").alias("measurement5_setpoint"),
    col("Stage1_Output_Setpoint6_U_Actual").alias("measurement6_setpoint"),
    col("Stage1_Output_Setpoint7_U_Actual").alias("measurement7_setpoint"),
    col("Stage1_Output_Setpoint8_U_Actual").alias("measurement8_setpoint"),
    col("Stage1_Output_Setpoint9_U_Actual").alias("measurement9_setpoint"),
    col("Stage1_Output_Setpoint10_U_Actual").alias("measurement10_setpoint"),
    col("Stage1_Output_Setpoint11_U_Actual").alias("measurement11_setpoint"),
    col("Stage1_Output_Setpoint12_U_Actual").alias("measurement12_setpoint"),
    col("Stage1_Output_Setpoint13_U_Actual").alias("measurement13_setpoint"),
    col("Stage1_Output_Setpoint14_U_Actual").alias("measurement14_setpoint")
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
)
df_machine4_temperature_pressure.write.jdbc(pg_url, "Dim_Machine4_Temperature_Pressure", mode="append", properties=pg_properties)

# Loading Machine 5 - Temperature
df_machine5_temperature = df_sample.select(
    col("Machine5_Temperature1_C_Actual").alias("temperature1"),
    col("Machine5_Temperature2_C_Actual").alias("temperature2"),
    col("Machine5_Temperature3_C_Actual").alias("temperature3"),
    col("Machine5_Temperature4_C_Actual").alias("temperature4"),
    col("Machine5_Temperature5_C_Actual").alias("temperature5"),
    col("Machine5_Temperature6_C_Actual").alias("temperature6")
)
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
    col("Stage2_Output_Setpoint0_U_Actual").alias("measurement0_setpoint"),
    col("Stage2_Output_Setpoint1_U_Actual").alias("measurement1_setpoint"),
    col("Stage2_Output_Setpoint2_U_Actual").alias("measurement2_setpoint"),
    col("Stage2_Output_Setpoint3_U_Actual").alias("measurement3_setpoint"),
    col("Stage2_Output_Setpoint4_U_Actual").alias("measurement4_setpoint"),
    col("Stage2_Output_Setpoint5_U_Actual").alias("measurement5_setpoint"),
    col("Stage2_Output_Setpoint6_U_Actual").alias("measurement6_setpoint"),
    col("Stage2_Output_Setpoint7_U_Actual").alias("measurement7_setpoint"),
    col("Stage2_Output_Setpoint8_U_Actual").alias("measurement8_setpoint"),
    col("Stage2_Output_Setpoint9_U_Actual").alias("measurement9_setpoint"),
    col("Stage2_Output_Setpoint10_U_Actual").alias("measurement10_setpoint"),
    col("Stage2_Output_Setpoint11_U_Actual").alias("measurement11_setpoint"),
    col("Stage2_Output_Setpoint12_U_Actual").alias("measurement12_setpoint"),
    col("Stage2_Output_Setpoint13_U_Actual").alias("measurement13_setpoint"),
    col("Stage2_Output_Setpoint14_U_Actual").alias("measurement14_setpoint")
)
df_second_stage_setpoint.write.jdbc(pg_url, "Dim_Second_Stage_Setpoint", mode="append", properties=pg_properties)

## Loading fact tables
# Loading Fact - Stage 1 - Operation
df_first_stage_operation = df_sample.select(
    monotonically_increasing_id().alias("time_id"),  
    
    monotonically_increasing_id().alias("ambient_id"), 
    
    monotonically_increasing_id().alias("machine1_motor_id"),  
    monotonically_increasing_id().alias("machine2_motor_id"), 
    monotonically_increasing_id().alias("machine3_motor_id"),  
    
    monotonically_increasing_id().alias("machine1_zone_temp_id"), 
    monotonically_increasing_id().alias("machine2_zone_temp_id"),  
    monotonically_increasing_id().alias("machine3_zone_temp_id"), 
    
    monotonically_increasing_id().alias("machine1_material_id"),  
    monotonically_increasing_id().alias("machine2_material_id"), 
    monotonically_increasing_id().alias("machine3_material_id"),
    
    monotonically_increasing_id().alias("combiner_temp_id"),
    
    avg(col("Machine1_MotorAmperage_U_Actual") + 
        col("Machine2_MotorAmperage_U_Actual") + 
        col("Machine3_MotorAmperage_U_Actual")).alias("avg_motor_amperage"),

    avg(col("Machine1_MotorRPM_U_Actual") + 
        col("Machine2_MotorRPM_U_Actual") + 
        col("Machine3_MotorRPM_U_Actual")).alias("avg_moto r_rpm"),
    
    avg(col("Machine1_Material_Pressure_Actual") +
        col("Machine2_Material_Pressure_Actual") +
        col("Machine3_Material_Pressure_Actual")).alias("avg_material_pressure"),
    
    avg(col("Machine1_Material_Temperature_Actual") +
        col("Machine2_Material_Temperature_Actual") +
        col("Machine3_Material_Temperature_Actual")).alias("avg_material_temperature"),
    
    avg(col("Machine1_ExitZone_Temperature_Actual") +
        col("Machine2_ExitZone_Temperature_Actual") +
        col("Machine3_ExitZone_Temperature_Actual")).alias("avg_exit_zone_temp")
)

# Loading Fact - Stage 1 - Output
df_first_stage_output = df_sample.select(
    monotonically_increasing_id().alias("time_id"),  
    
    monotonically_increasing_id().alias("ambient_id"), 
    
    monotonically_increasing_id().alias("first_stage_actual_id"),
    
    monotonically_increasing_id().alias("first_stage_setpoint_id"),
    
    avg(col("Stage1_Output_Measurement0_U_Actual") +
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
        col("Stage1_Output_Measurement14_U_Actual")).alias("avg_measurement_actual"),
    
    # add a column avg_setpoint to the fact table fact_stage1_output
    avg(col("Stage1_Output_Setpoint0_U_Actual") +
        col("Stage1_Output_Setpoint1_U_Actual") +
        col("Stage1_Output_Setpoint2_U_Actual") +
        col("Stage1_Output_Setpoint3_U_Actual") +
        col("Stage1_Output_Setpoint4_U_Actual") +
        col("Stage1_Output_Setpoint5_U_Actual") +
        col("Stage1_Output_Setpoint6_U_Actual") +
        col("Stage1_Output_Setpoint7_U_Actual") +
        col("Stage1_Output_Setpoint8_U_Actual") +
        col("Stage1_Output_Setpoint9_U_Actual") +
        col("Stage1_Output_Setpoint10_U_Actual") +
        col("Stage1_Output_Setpoint11_U_Actual") +
        col("Stage1_Output_Setpoint12_U_Actual") +
        col("Stage1_Output_Setpoint13_U_Actual") +
        col("Stage1_Output_Setpoint14_U_Actual")).alias("avg_setpoint"),
)

# Loading Fact - Stage 2 - Operation
df_second_stage_operation = df_sample.select(
    monotonically_increasing_id().alias("time_id"),  
    
    monotonically_increasing_id().alias("ambient_id"),
    
    monotonically_increasing_id().alias("machine4_temp_pressure_id"),
    
    monotonically_increasing_id().alias("machine5_temp_id"),
    
    monotonically_increasing_id().alias("exit_temp_id"),
    
    # add a column avg_machine4_temperature and avg_machine5_temperature to the fact table fact_stage2_operation
    avg(col("Machine4_Temperature1_C_Actual") +
        col("Machine4_Temperature2_C_Actual") +
        col("Machine4_Temperature3_C_Actual") +
        col("Machine4_Temperature4_C_Actual") +
        col("Machine4_Temperature5_C_Actual")).alias("avg_machine4_temperature"),
    
    avg(col("Machine5_Temperature1_C_Actual") +
        col("Machine5_Temperature2_C_Actual") +
        col("Machine5_Temperature3_C_Actual") +
        col("Machine5_Temperature4_C_Actual") +
        col("Machine5_Temperature5_C_Actual") +
        col("Machine5_Temperature5_C_Actual")).alias("avg_machine5_temperature"),
    
    avg(col("Machine4_ExitTemperature_U_Actual") +
        col("Machine5_ExitTemperature_U_Actual")).alias("avg_exit_temperature")
)

# Loading Fact - Stage 1 - Output
df_first_stage_output = df_sample.select(
    monotonically_increasing_id().alias("time_id"),  
    
    monotonically_increasing_id().alias("ambient_id"), 
    
    monotonically_increasing_id().alias("second_stage_actual_id"),
    
    monotonically_increasing_id().alias("second_stage_setpoint_id"),
    
    avg(col("Stage2_Output_Measurement0_U_Actual") +
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
        col("Stage2_Output_Measurement14_U_Actual")).alias("avg_measurement_actual"),
    
    # add a column avg_setpoint to the fact table fact_stage2_output
    avg(col("Stage2_Output_Setpoint0_U_Actual") +
        col("Stage2_Output_Setpoint1_U_Actual") +
        col("Stage2_Output_Setpoint2_U_Actual") +
        col("Stage2_Output_Setpoint3_U_Actual") +
        col("Stage2_Output_Setpoint4_U_Actual") +
        col("Stage2_Output_Setpoint5_U_Actual") +
        col("Stage2_Output_Setpoint6_U_Actual") +
        col("Stage2_Output_Setpoint7_U_Actual") +
        col("Stage2_Output_Setpoint8_U_Actual") +
        col("Stage2_Output_Setpoint9_U_Actual") +
        col("Stage2_Output_Setpoint10_U_Actual") +
        col("Stage2_Output_Setpoint11_U_Actual") +
        col("Stage2_Output_Setpoint12_U_Actual") +
        col("Stage2_Output_Setpoint13_U_Actual") +
        col("Stage2_Output_Setpoint14_U_Actual")).alias("avg_setpoint"),
)

print("Single record inserted into PostgreSQL for verification.")