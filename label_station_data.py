'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 

Description: This script assigns labels to all the weather stations and
             stores the labeled data set in a mongodb database. The labels
             are created depending on the number of regions specifed by the
             user. e.g for three regions, the labels {0, 1, 2} will be used, 
             and for five regions, the labels {0, 1, 2, 3, 4} will be used.
             

'''
# import relevant libraries
from pymongo import MongoClient
from utility_scripts import classification_utils as utils # utilty functions

##SETUP : user can change parameters here#
num_regions = 3


## Store station labels ##
# configure the database where the labeled data is to be stored

server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"labeled_station_coords_"+str(num_regions)}

client = MongoClient(host=server["host"], port=server["port"])
db = client[server["dbName"]]
database = db[server["collection"]]

# store labeled data
utils.store_station_labels(database, num_regions)