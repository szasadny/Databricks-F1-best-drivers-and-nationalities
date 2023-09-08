# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC
# MAGIC This is a Databricks project I made to test my understanding before undergoing the Databricks Data Engineer Associate exam. This notebook ingests data from the ergast.com API of all the Formula 1 driver standings by year, then transforms it into one big table with the sum of the points of each driver. Finally it shows the top 10 drivers of all time(by points) and the top 10 nationalities of all time(by points).
# MAGIC
# MAGIC #### Disclaimer
# MAGIC I know that there were way less points earned in a season during the olden days. This application is just a test of knowledge.

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL
# MAGIC Extract the data from the API and transform it into one dataframe.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

schema = StructType([
    StructField("DriverId", StringType(), True),
    StructField("GivenName", StringType(), True),
    StructField("FamilyName", StringType(), True),
    StructField("DateOfBirth", DateType(), True),
    StructField("Nationality", StringType(), True),
    StructField("TotalPoints", DoubleType(), True)
])

# Initialize an empty DataFrame with the defined schema
all_data = spark.createDataFrame([], schema=schema)

# Get range of all the years
years = range(1950, (datetime.datetime.now().year + 1))

# Loop through each year
for year in years:
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

# COMMAND ----------

# MAGIC %md
# MAGIC Make two new dataframes. 
# MAGIC - nationalities_df aggregates all_data to sort by the most points scored by a nationality.
# MAGIC - drivers_df selects the relevant data from all_data to show the best drivers by points.

# COMMAND ----------

nationality_df = all_data.groupBy("Nationality") \
    .agg(sum("TotalPoints").alias("TotalPoints")) \
    .orderBy(col("TotalPoints").desc())

driver_df = all_data.select("GivenName", "FamilyName", "TotalPoints") \
    .orderBy(col("TotalPoints").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC # Results
# MAGIC The best nationalities by points:

# COMMAND ----------

nationality_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC The best drivers by points:

# COMMAND ----------

driver_df.show(10)

# COMMAND ----------


