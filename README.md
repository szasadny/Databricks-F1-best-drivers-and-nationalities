# Introduction

This is a Databricks project I made to test my understanding before undergoing the Databricks Data Engineer Associate exam. This notebook ingests data from the ergast.com API of all the Formula 1 driver standings by year, then transforms it into one big table with the sum of the points of each driver. Finally it shows the top 10 drivers of all time(by points) and the top 10 nationalities of all time(by points).

### Formula 1 best drivers and nationalities by points.dbc
Is an interative notebook of the pipeline where the code can be run and the data can be displayed.

### F1 ETL as DLT pipeline.dbc
Is an alteration of the above notebook. This notebook is meant to be run externally through a Delta Live Tables(DLT) workflow pipeline. It creates an initial DLT named F1DriverData of all the current data and it updates its content annually by appending the data of the latest year.

## Disclaimer
I know that there were way less points earned in a season during the olden days. This application is just made as a test of knowledge.
