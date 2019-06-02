'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 

Description: This script performs clustering of weather data using a set of
             parameters that are specified by the user. The user is required 
             to specify the following parameters:
             
             1. sample_years: list of years which should be included in the sample data.
             2. season: list of months that should be included when calculating a reduced
                        weather dataset.
             3. num_clusters: the number of clusters to be discovered
             4. features: list of weather attributes to be used for clustering.
             

'''
# import relevant libraries
from utility_scripts import clustering_utils as utils # utilty functions

## SETUP : User can change inputs here ##
sample_years = [2018]
season = [1,2,3,4,5,6,7,8,9,10,11,12]
num_clusters = 3 # number of clusters
features = ["tmax_degC", "tmin_degC", "af_days", "rain_mm"] # features to consider


## LOAD DATA ##
# Get weather data for a specified sample year or set of sample years
data = utils.get_weather_data(sample_years=sample_years)
print("\n Data sample loaded successfully \n")
print(data.describe())

# fill in missing values if any 
data = utils.fill_missing_values(data)
print(data.describe())


## GENERATE OUTPUTS ##
# save a pair plot showing the discovered clusters
clustred = utils.get_clusters(data, season, num_clusters, features)
utils.plot_clusters(clustred)

# print the stations in each cluster
utils.print_clusters(clustred)