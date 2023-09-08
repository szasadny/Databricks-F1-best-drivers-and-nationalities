# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
import requests
import datetime

# Create a Spark session
spark = SparkSession.builder.appName("F1DriverData").getOrCreate()

# Define the schema
schema = StructType([
    StructField("DriverId", StringType(), True),
    StructField("GivenName", StringType(), True),
    StructField("FamilyName", StringType(), True),
    StructField("DateOfBirth", DateType(), True),
    StructField("Nationality", StringType(), True),
    StructField("TotalPoints", DoubleType(), True)
])

# Load existing data from Databricks Delta Lake (if it exists)
try:
    all_data = spark.read.format("delta").load("/mnt/delta/F1DriverData")
except:
    all_data = spark.createDataFrame([], schema=schema)

# Get the current year
current_year = datetime.datetime.now().year

# Determine the range of years to fetch data
if all_data.count() == 0:
    # If F1DriverData is empty, fetch data from 1950 to the current year
    years_to_fetch = range(1950, current_year + 1)
else:
    # If F1DriverData is not empty, fetch data for the current year only
    years_to_fetch = [current_year]

# Loop through each year in years_to_fetch
for year in years_to_fetch:
    url = f"https://ergast.com/api/f1/{year}/driverStandings.json"
    response = requests.get(url)
    data = response.json()

    # Extract drivers data as a DataFrame
    drivers = data["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
    year_data = [(driver["Driver"]["driverId"], driver["Driver"]["givenName"],
                  driver["Driver"]["familyName"], driver["Driver"]["dateOfBirth"],
                  driver["Driver"]["nationality"], float(driver["points"])) for driver in drivers]

    # Convert year_data to a DataFrame
    year_df = spark.createDataFrame(year_data, schema=["DriverId", "GivenName", "FamilyName", "DateOfBirth", "Nationality", "Points"])

    # Join the year_df with all_data and update existing drivers
    all_data = all_data.alias("all_data").join(year_df.alias("year_df"), "DriverId", "outer")\
        .select(
            coalesce(col("all_data.DriverId"), col("year_df.DriverId")).alias("DriverId"),
            coalesce(col("all_data.GivenName"), col("year_df.GivenName")).alias("GivenName"),
            coalesce(col("all_data.FamilyName"), col("year_df.FamilyName")).alias("FamilyName"),
            coalesce(col("all_data.DateOfBirth"), col("year_df.DateOfBirth")).alias("DateOfBirth"),
            coalesce(col("all_data.Nationality"), col("year_df.Nationality")).alias("Nationality"),
            (coalesce(col("all_data.TotalPoints"), lit(0)) + coalesce(col("year_df.Points"), lit(0))).alias("TotalPoints")
        )

# Write the updated data to Databricks Delta Lake Table
all_data.write.format("delta").mode("append").save("/mnt/delta/F1DriverData")


# COMMAND ----------


