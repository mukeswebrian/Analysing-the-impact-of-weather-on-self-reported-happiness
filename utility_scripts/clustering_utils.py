'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 
'''

# import relevant libraries
import pandas as pd
from pymongo import MongoClient
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

def query_to_dataframe(query, collection):
    '''
    custom function to create a dataframe of query results
    from the weather_data
    '''
    
    client = MongoClient(host="localhost", port=27017)
    weather_data = client.pitds_weather_data[collection]
    
    entries = []
    for entry in weather_data.find(query):
        entries.append(entry)
      
    if len(entries)==0: # no hits for the query
        return None
    else:
        return pd.DataFrame(entries).drop(columns=["_id"])

def get_weather_data(sample_years=[]):
    '''
    load weather data from the specified sample years into a
    single data frame
    '''
    
    data = []
    
    for year in sample_years:
        data.append(query_to_dataframe({"year":year}, "weather_data"))
        
    return pd.concat(data, axis=0)


def get_fill_values(station, fill_years, feature):
    '''
    Function to calculate the average number of sun hours across the
    specified fill years for each month
    '''
    count = 0 # index through the fill years
    
    # cummulatively sum up values from fill years
    for year in fill_years:
        
        df = query_to_dataframe({"station_name":station, "year":year}, "weather_data")
        
        if count == 0:   
            values = pd.concat([df[feature], df.month], axis=1).set_index("month")
            
        else:
            values = values + pd.concat([df[feature], df.month], axis=1).set_index("month")
            
    return values/len(fill_years) # divide by number of fill years to get average


def get_fill_years(station, num_years_to_check=60):
    '''
    Get the most recently available years of data
    '''
    
    years = []
    
    # check past years for recorded feature data (i.e count down years)
    for year in range(-2018, num_years_to_check-2018):
        
        
        df = query_to_dataframe({"station_name":station, "year":-year}, "weather_data")
        
        # check for any entries
        if df is None:
            pass
        else:
            if len(df[df.sun_hours<1000])==12:
                years.append(-year)
            
        if len(years)>=3: # number of fill years to consider
            break
                    
    return years

def fill_feature(feature, data): 
    '''
    fill in missing values for a specific feature
    '''  
    
    # identify stations that are missing data entries
    missing_stations = data[data[feature]==1000].station_name.unique()
    
    # get fill years
    fill_years = {}
    for station in missing_stations:
        fill_years[station] = get_fill_years(station)
        
    # fill in missing values
    for station in missing_stations:
        df_slice = data[data.station_name==station].sun_hours.index
        values = get_fill_values(station, fill_years[station], feature)
        #values.index = values.index.astype(int)
        
        for i in range(0, len(values)):
            val = values.iloc[i]
            data.set_value(col=feature, index=df_slice[i], value=val)
   
    
def fill_missing_values(data):
    
    # identify features that have missing values
    features = []
    for feature in data.columns:
        if len(data[data[feature] == 1000]) > 0:
            features.append(feature)
            
    for feature in features:
        fill_feature(feature, data)
        print("\n Filled in missing values for ", feature)
        
    return data


def get_season_average(months, data):
    
    season = data[data.month == months[0]].set_index("station_name")
    
    for month in range(1, len(months)):
        season = season + data[data.month == months[month]].set_index("station_name")
        
    return season/len(months)


def get_clusters(data, season, num_clusters, features):
    
   
    # initialize classifier
    clf = KMeans(n_clusters=num_clusters, random_state=1000) 

    season_avg = get_season_average(season, data)
    
    # identify features to drop
    to_drop = []
    for feature in season_avg.columns:
        if not feature in features:
            to_drop.append(feature)
            
    season_avg.drop(columns=to_drop, inplace=True)
    
    # normalize data
    scaler = MinMaxScaler()
    scaler.fit(season_avg)
    season_avg_scaled = scaler.transform(season_avg)
    
    # Fit clustering algorithm
    clf.fit(season_avg_scaled)
    df = pd.DataFrame(season_avg_scaled, columns=season_avg.columns, index=season_avg.index)
    
    clustred = pd.concat([df, pd.Series(clf.labels_, name="labels",index=season_avg.index)], axis=1)
    
    return clustred
    
def plot_clusters(clustred):
    
    # create a figure of a pair plot
    to_plot = clustred.drop(columns=["labels"]).columns
    fig = sns.pairplot(data=clustred, vars=to_plot, hue="labels", height=2.5)
    fig.savefig("cluster_plot.png")
    

def print_cluster(group, label):
    '''print the stations in a specified cluster'''
    
    print("\n", "Cluster", label, "\n")
    for i in group:
        print(i)

def print_clusters(clustred):
    
    groups = clustred.groupby("labels").groups
    
    # print the stations assigned to each cluster
    for i in range(0, len(groups)):
        print_cluster(groups[i], i)
