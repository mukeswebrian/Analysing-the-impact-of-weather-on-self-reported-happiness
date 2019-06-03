'''
Author: Brian Mukeswe
Institution: The University of Edinburgh
Department: School of Informatics
Contact: b.mukeswe@sms.ed.ac.uk
Date: May 31, 2019

Description: This script performs the following actions:
    1. Downloading weather data for a set of stations specified 
       as a list contained in a text file "stations.txt". 
    2. Cleans the weather data for all the stations by removing 
       non-numerical characters. All the downloaded weather data 
       is loaded into a single data frame.
    3. Storing all the cleaned weather data in a specifed Mongodb
       database. 

'''

# import relevant libraries
from utility_scripts import processing_utils as utils # utilty functions for processing weather data
from pymongo import MongoClient
letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    
# read station names from a text file (optional: specify a few stations in a list)
station_names = utils.get_station_names()
#station_names = ["ballypatrick","cwmystwyth"]
    
# specify the source website for the weather data
url_root = "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/"
    
# save raw weather data into text files
utils.download_weather_data(url_root, station_names)
print("downloading completed \n")

# clean weather data and store it in a data frame
to_remove = letters + "*#$" # specify characters to be removed
data = utils.clean_weather_data(station_names, to_remove)
print("cleaning completed \n")
    
# configure the database where the data is to be stored
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"weather_data"}

client = MongoClient(host=server["host"], port=server["port"])
db = client[server["dbName"]]
database = db[server["collection"]]
    
utils.store_weather_data(data, database)
print("storing weather data completed")

