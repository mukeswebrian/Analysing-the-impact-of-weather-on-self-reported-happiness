'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 

Description:

'''
# import relevant libraries
from utility_scripts import clustering_utils as utils # utilty functions for processing weather data


## Get weather data for a specified sample year or set of sample years
data = utils.get_weather_data(sample_years=[2018])
print("\n Data sample loaded successfully \n")
print(data.describe())

# fill in missing values if any 
data = utils.fill_missing_values(data)
print(data.describe())


## specify clustering inputs ##

# specify months to considered when calculating averages reduced data
season = [1,2,3,4,5,6,7,8,9,10,11,12]

# number of clusters
num_clusters = 3

# features to consider
features = ["tmax_degC", "tmin_degC", "af_days", "rain_mm"]


# save a pair plot showing the discovered clusters
clustred = utils.get_clusters(data, season, num_clusters, features, algorithm)
utils.plot_clusters(clustred)


# print the stations in each cluster
utils.print_clusters(clustred)