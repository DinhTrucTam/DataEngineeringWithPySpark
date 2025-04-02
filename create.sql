CREATE TABLE "dim_time" (
  "time_id" SERIAL PRIMARY KEY,
  "time_stamp" timestamp,
  "year" int,
  "month" int,
  "day" int,
  "hour" int,
  "minute" int
);

CREATE TABLE "dim_ambient_conditions" (
  "ambient_id" SERIAL PRIMARY KEY,
  "ambient_humidity" DOUBLE PRECISION,
  "ambient_temperature" DOUBLE PRECISION
);

CREATE TABLE "dim_machine1_motor" (
  "machine1_motor_id" SERIAL PRIMARY KEY,
  "motor_amperage" DOUBLE PRECISION,
  "motor_rpm" DOUBLE PRECISION
);

CREATE TABLE "dim_machine2_motor" (
  "machine2_motor_id" SERIAL PRIMARY KEY,
  "motor_amperage" DOUBLE PRECISION,
  "motor_rpm" DOUBLE PRECISION
);

CREATE TABLE "dim_machine3_motor" (
  "machine3_motor_id" SERIAL PRIMARY KEY,
  "motor_amperage" DOUBLE PRECISION,
  "motor_rpm" DOUBLE PRECISION
);

CREATE TABLE "dim_machine1_zone_temperature" (
  "machine1_zone_temp_id" SERIAL PRIMARY KEY,
  "zone1_temperature" DOUBLE PRECISION,
  "zone2_temperature" DOUBLE PRECISION
);

CREATE TABLE "dim_machine2_zone_temperature" (
  "machine2_zone_temp_id" SERIAL PRIMARY KEY,
  "zone1_temperature" DOUBLE PRECISION,
  "zone2_temperature" DOUBLE PRECISION
);

CREATE TABLE "dim_machine3_zone_temperature" (
  "machine3_zone_temp_id" SERIAL PRIMARY KEY,
  "zone1_temperature" DOUBLE PRECISION,
  "zone2_temperature" DOUBLE PRECISION
);

CREATE TABLE "dim_machine1_material_properties" (
  "machine1_material_id" SERIAL PRIMARY KEY,
  "raw_material_property1" DOUBLE PRECISION,
  "raw_material_property2" int,
  "raw_material_property3" DOUBLE PRECISION,
  "raw_material_property4" int
);

CREATE TABLE "dim_machine2_material_properties" (
  "machine2_material_id" SERIAL PRIMARY KEY,
  "raw_material_property1" DOUBLE PRECISION,
  "raw_material_property2" int,
  "raw_material_property3" DOUBLE PRECISION,
  "raw_material_property4" int
);

CREATE TABLE "dim_machine3_material_properties" (
  "machine3_material_id" SERIAL PRIMARY KEY,
  "raw_material_property1" DOUBLE PRECISION,
  "raw_material_property2" int,
  "raw_material_property3" DOUBLE PRECISION,
  "raw_material_property4" int
);

CREATE TABLE "dim_combiner_temperature" (
  "combiner_temp_id" SERIAL PRIMARY KEY,
  "temperature1" DOUBLE PRECISION,
  "temperature2" DOUBLE PRECISION,
  "temperature3" DOUBLE PRECISION
);

CREATE TABLE "dim_first_stage_actual" (
  "first_stage_actual_id" SERIAL PRIMARY KEY,
  "measurement0_actual" DOUBLE PRECISION,
  "measurement1_actual" DOUBLE PRECISION,
  "measurement2_actual" DOUBLE PRECISION,
  "measurement3_actual" DOUBLE PRECISION,
  "measurement4_actual" DOUBLE PRECISION,
  "measurement5_actual" DOUBLE PRECISION,
  "measurement6_actual" DOUBLE PRECISION,
  "measurement7_actual" DOUBLE PRECISION,
  "measurement8_actual" DOUBLE PRECISION,
  "measurement9_actual" DOUBLE PRECISION,
  "measurement10_actual" DOUBLE PRECISION,
  "measurement11_actual" DOUBLE PRECISION,
  "measurement12_actual" DOUBLE PRECISION,
  "measurement13_actual" DOUBLE PRECISION,
  "measurement14_actual" DOUBLE PRECISION
);

CREATE TABLE "dim_first_stage_setpoint" (
  "first_stage_setpoint_id" SERIAL PRIMARY KEY,
  "measurement0_setpoint" DOUBLE PRECISION,
  "measurement1_setpoint" DOUBLE PRECISION,
  "measurement2_setpoint" DOUBLE PRECISION,
  "measurement3_setpoint" DOUBLE PRECISION,
  "measurement4_setpoint" DOUBLE PRECISION,
  "measurement5_setpoint" DOUBLE PRECISION,
  "measurement6_setpoint" DOUBLE PRECISION,
  "measurement7_setpoint" DOUBLE PRECISION,
  "measurement8_setpoint" DOUBLE PRECISION,
  "measurement9_setpoint" DOUBLE PRECISION,
  "measurement10_setpoint" DOUBLE PRECISION,
  "measurement11_setpoint" DOUBLE PRECISION,
  "measurement12_setpoint" DOUBLE PRECISION,
  "measurement13_setpoint" DOUBLE PRECISION,
  "measurement14_setpoint" DOUBLE PRECISION
);

CREATE TABLE "dim_machine4_temperature_pressure" (
  "machine4_temp_pressure_id" SERIAL PRIMARY KEY,
  "temperature1" DOUBLE PRECISION,
  "temperature2" DOUBLE PRECISION,
  "pressure" DOUBLE PRECISION,
  "temperature3" DOUBLE PRECISION,
  "temperature4" DOUBLE PRECISION,
  "temperature5" DOUBLE PRECISION
);

CREATE TABLE "dim_machine5_temperature" (
  "machine5_temp_id" SERIAL PRIMARY KEY,
  "temperature1" DOUBLE PRECISION,
  "temperature2" DOUBLE PRECISION,
  "temperature3" DOUBLE PRECISION,
  "temperature4" DOUBLE PRECISION,
  "temperature5" DOUBLE PRECISION,
  "temperature6" DOUBLE PRECISION
);

CREATE TABLE "dim_exit_temperature" (
  "exit_temp_id" SERIAL PRIMARY KEY,
  "machine4_exit_temperature" DOUBLE PRECISION,
  "machine5_exit_temperature" DOUBLE PRECISION
);

CREATE TABLE "dim_second_stage_actual" (
  "second_stage_actual_id" SERIAL PRIMARY KEY,
  "measurement0_actual" DOUBLE PRECISION,
  "measurement1_actual" DOUBLE PRECISION,
  "measurement2_actual" DOUBLE PRECISION,
  "measurement3_actual" DOUBLE PRECISION,
  "measurement4_actual" DOUBLE PRECISION,
  "measurement5_actual" DOUBLE PRECISION,
  "measurement6_actual" DOUBLE PRECISION,
  "measurement7_actual" DOUBLE PRECISION,
  "measurement8_actual" DOUBLE PRECISION,
  "measurement9_actual" DOUBLE PRECISION,
  "measurement10_actual" DOUBLE PRECISION,
  "measurement11_actual" DOUBLE PRECISION,
  "measurement12_actual" DOUBLE PRECISION,
  "measurement13_actual" DOUBLE PRECISION,
  "measurement14_actual" DOUBLE PRECISION
);

CREATE TABLE "dim_second_stage_setpoint" (
  "second_stage_setpoint_id" SERIAL PRIMARY KEY,
  "measurement0_setpoint" DOUBLE PRECISION,
  "measurement1_setpoint" DOUBLE PRECISION,
  "measurement2_setpoint" DOUBLE PRECISION,
  "measurement3_setpoint" DOUBLE PRECISION,
  "measurement4_setpoint" DOUBLE PRECISION,
  "measurement5_setpoint" DOUBLE PRECISION,
  "measurement6_setpoint" DOUBLE PRECISION,
  "measurement7_setpoint" DOUBLE PRECISION,
  "measurement8_setpoint" DOUBLE PRECISION,
  "measurement9_setpoint" DOUBLE PRECISION,
  "measurement10_setpoint" DOUBLE PRECISION,
  "measurement11_setpoint" DOUBLE PRECISION,
  "measurement12_setpoint" DOUBLE PRECISION,
  "measurement13_setpoint" DOUBLE PRECISION,
  "measurement14_setpoint" DOUBLE PRECISION
);

CREATE TABLE "fact_stage1_operation" (
  "time_id" int,
  "ambient_id" int,
  "machine1_motor_id" int,
  "machine2_motor_id" int,
  "machine3_motor_id" int,
  "machine1_zone_temp_id" int,
  "machine2_zone_temp_id" int,
  "machine3_zone_temp_id" int,
  "machine1_material_id" int,
  "machine2_material_id" int,
  "machine3_material_id" int,
  "combiner_temp_id" int,
  "avg_motor_amperage" DOUBLE PRECISION,
  "avg_motor_rpm" DOUBLE PRECISION,
  "avg_material_pressure" DOUBLE PRECISION,
  "avg_material_temperature" DOUBLE PRECISION,
  "avg_exit_zone_temp" DOUBLE PRECISION
);

CREATE TABLE "fact_stage1_output" (
  "time_id" int,
  "ambient_id" int,
  "first_stage_actual_id" int,
  "first_stage_setpoint_id" int,
  "avg_measurement_actual" DOUBLE PRECISION,
  "avg_setpoint" DOUBLE PRECISION
);

CREATE TABLE "fact_stage2_operation" (
  "time_id" int,
  "ambient_id" int,
  "machine4_temp_pressure_id" int,
  "machine5_temp_id" int,
  "exit_temp_id" int,
  "avg_machine4_temperature" DOUBLE PRECISION,
  "avg_machine5_temperature" DOUBLE PRECISION,
  "avg_exit_temperature" DOUBLE PRECISION
);

CREATE TABLE "fact_stage2_output" (
  "time_id" int,
  "ambient_id" int,
  "second_stage_actual_id" int,
  "second_stage_setpoint_id" int,
  "avg_measurement_actual" DOUBLE PRECISION,
  "avg_setpoint" DOUBLE PRECISION
);

ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

ALTER TABLE "fact_stage1_operation" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

ALTER TABLE "fact_stage1_output" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

ALTER TABLE "fact_stage2_operation" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");

ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("time_id");

ALTER TABLE "fact_stage2_output" ADD FOREIGN KEY ("ambient_id") REFERENCES "dim_ambient_conditions" ("ambient_id");
