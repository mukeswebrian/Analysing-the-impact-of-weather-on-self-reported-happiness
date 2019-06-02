'''
Author Exam Number: B145418
Date: June 1, 2019
Institution: University of Edinburgh 

Description: This script a joins a specified sample of weather data
             with surveyed happiness data based on area codes. 
             The script reports correlations between each weather attribute
             and happiness metric, and writes the joined dataset to a specified 
             file format i.e (either csv, xls, xlsx or standard output).
             The user can specify the following parameters:
             output: the format for reporting the joined dataset
             metrics: the metrics to use for correlation. options include the following
                     ("avg_rating", "happy", "unhappy", "low_0_4", "medium_5_6", "high_7_8", "vhigh_9_10"
             features: The weather attributes to be included when reporting correlations.

'''
# import relevant libraries
from pymongo import MongoClient
import pandas as pd
# utilty functions
from utility_scripts import classification_utils as part2_utils 
from utility_scripts import clustering_utils as part1_utils
from utility_scripts import happiness_data_utils as utils

## SETUP: user can specify input parameters here ##
output = 'xlsx' # choose from {'csv', 'xls', 'xlsx', 'standard'}
metrics = ["avg_rating", "happy", "unhappy"] # metrics to use for reporting corelations
features = ["af_days", "rain_mm", "sun_hours" , "tmax_degC","tmin_degC"] # features to report


## Get station location data ##
# configure the database where the station data is stored
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"labeled_station_coords_3"} # use the same database as part2

client = MongoClient(host=server["host"], port=server["port"])
db = client[server["dbName"]]
database = db[server["collection"]]

# retrieve station coordinates
station_coords = part1_utils.query_to_dataframe({}, server["collection"])
print("\nRetrieved station coordinates\n")
print(station_coords.head(10))



## Get happiness data from database##
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"happiness"}

happiness = part1_utils.query_to_dataframe({}, server["collection"])
print("\nRetrieved shappiness data\n")
print(happiness.head(10))



## Sample weather data
# Get weather data for a specified sample year or set of sample years
sample_years = [2018]
season = [1,2,3,4,5,6,7,8,9,10,11,12]
weather = part1_utils.get_weather_data(sample_years=sample_years) 
weather = part1_utils.fill_missing_values(weather)
weather = part1_utils.get_season_average(season, weather)
print("\nRetrieved samplle weather data\n")


## Join area codes with weather data and store new collection ##
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"weather_area"}

database = db[server["collection"]]

# Join sampled weather data with area codes and store in a database collection
weather_area = utils.join_weather_to_area_codes(weather, station_coords, happiness)
utils.store_data(weather_area, database)


## Aggregate weather data and happiness data using weather_area collection
# specify aggregation details
query = [{
          '$lookup': {
            'from': 'happiness', 
            'localField': 'area', 
            'foreignField': 'area_codes', 
            'as': 'happiness'
          }
        }
       ]

# Join weather and happiness data
joined = utils.aggregation_to_dataframe(query, database, station_coords)

# Combine happiness metrics data into two categories happy and unhappy
happy = joined.high_7_8 + joined.vhigh_9_10
happy.name = "happy"
unhappy = joined.low_0_4 + joined.medium_5_6
unhappy.name = "unhappy"
joined = pd.concat([joined,happy,unhappy], axis=1)           


# Report correlations
utils.report_corelations(joined, features, metrics)
           
# Report joined dataset
if output == "csv":
    joined.to_csv("joined_data_set.csv")
elif output == "standard":
    print(joined)
else:
    joined.to_excel("joined_data_set."+output)
 