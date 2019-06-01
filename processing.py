'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 

Description:

'''

# import relevant libraries
from utility_scripts import processing_utils as utils # utilty functions for processing weather data
letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    
# read station names from a text file or specify a list of stations
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
    
# specify the database where the data is to be stored
database = {"host":"midgard09",
            "port":27017,
            "dbName":"pitdis_assessment3",
            "collection":"weather_data"}
    
utils.store_weather_data(data, database)
print("storing completed")

