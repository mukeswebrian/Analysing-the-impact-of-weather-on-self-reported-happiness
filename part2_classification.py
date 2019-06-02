'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 

Description: This script reports the classification accuracy score using 
             a sample dataset that can be specified by the user. The user 
             can specify the following parameters when running the script:
             1. test_size: The number of data points to be set aside for evaluating
                           classification accuracy expressed as a proportion of the
                           total number of datapoints in the dataset.
             2. sample_years: list of years which should be included in the sample data.
             3. season: list of months that should be included when calculating a reduced
                         weather dataset.
             4. features: list of weather attributes to be used for classification.
             5. num_regions: the number of parts (classes) into which to divide the UK for
                             classification purposes.

'''
# import relevant libraries
from pymongo import MongoClient
import pandas as pd
from utility_scripts import classification_utils as utils # utilty functions
from utility_scripts import clustering_utils as part1_utils


## SETUP : user can change parameters here ##
test_size = 0.15 # training and test size
features = ["tmax_degC", "tmin_degC", "af_days", "rain_mm"] # features to consider
sample_years = [2018]
season = [1,2,3,4,5,6,7,8,9,10,11,12]
num_regions = 3 # ensure that an appropriate database has been created using the script (label_station_data.py)

## LOAD DATA ##
# Get weather data for a specified sample year or set of sample years
data = part1_utils.get_weather_data(sample_years=sample_years) 
data = part1_utils.fill_missing_values(data)
data = part1_utils.get_season_average(season, data)
print("data")
print(data.head(10))  


# configure the database where the labeled data is stored
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"labeled_station_coords_"+str(num_regions)}

client = MongoClient(host=server["host"], port=server["port"])
db = client[server["dbName"]]
database = db[server["collection"]]

labels = part1_utils.query_to_dataframe({}, server["collection"])
labels = labels.set_index("index")

# ensure that indecies match before adding labels
# Drop stations that do not have 2018 data
to_drop = []
for station in labels.index:
    if not station in data.index:
        to_drop.append(station)
        print("dropped: ",station)
        
print("\nlabels")
print(labels)       
labels.drop(index=to_drop, inplace=True)

# Join labels and weather data into a single dataframe
labeled_data = pd.concat([data, labels.label], axis=1)
labeled_data.drop(columns=['INDEX','year','month'], inplace=True)
print("\nlabled data")
print(labeled_data)

## Classification ##
# return accuracy scores
utils.get_accuracy_scores(labeled_data, test_size, features)