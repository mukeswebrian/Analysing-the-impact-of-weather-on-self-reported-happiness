'''
Author: Brian Mukeswe
Institution: The University of Edinburgh
Department: School of Informatics
Contact: b.mukeswe@sms.ed.ac.uk
Date: June 1, 2019

'''
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from sklearn.preprocessing import MinMaxScaler

def clean_happiness_data(data):
    
    to_keep = ["Area Codes", "Area names", "Unnamed: 4", "Unnamed: 5",
               "Unnamed: 6","Unnamed: 7", "Unnamed: 8", "Unnamed: 24"]

    # Keep only columns of interest
    for column in data.columns:
        if not column in to_keep:
            data.drop(columns=[column], inplace=True)
        
    rename = {"Area Codes" : "area_codes",
          "Area names" : "area_names",
          "Unnamed: 4" : "low_0_4",
          "Unnamed: 5" : "medium_5_6",
          "Unnamed: 6" : "high_7_8",
          "Unnamed: 7" : "vhigh_9_10",
          "Unnamed: 8" : "avg_rating",
          "Unnamed: 24": "sample_size"}

    # Rename data columns
    data.columns = rename.values()

    # Drop trailing rows
    for i in range(441, len(data)):
        data.drop(index=[i], inplace=True)
        
    # Get Area codes of interest
    columns = ["area_code", "area_name", "area_latitude", "area_longitude"]
    regions = pd.read_csv("regions.txt", names=columns)

    for i in range(0, len(data)):
        # Keep only data that corresponds to the area codes of interest
        if not data.area_codes.loc[i] in regions.area_code.values:
            data.drop(index=[i], inplace=True)
    
    # Merge coordinates with happiness data
    data.reset_index(inplace=True)
    data.drop(columns=["index"], inplace=True)
    data = pd.concat([data, regions], axis=1)
    data.drop(columns=["area_name", "area_code"], inplace=True)
    return data

def make_lineObj(line, data):
    ''' create a dict object of a specified row
        of a dataframe
    '''
   
    obj = {}
    
    for column in data.columns:
        obj[column] = data[column].iloc[line]
        
    return obj


def storeObj(obj, database):
    '''
    store a dict object
    '''
    database.insert_one(obj)
      
def store_data(data, database):
    for row in range(0, len(data)):
        obj = make_lineObj(row, data)
        storeObj(obj, database)

def square(x):
    return x*x

def get_shortest_distance(a, b):
    x = b.area_longitude - a.longitude
    y = b.area_latitude - a.latitude
    
    xx = x.apply(func=square)
    yy = y.apply(func=square)
    
    return np.sqrt(xx+yy).sort_values().index[0]
     
    
def get_area_index(station, stations, areas):
    '''
    custom function to identify the area to which a given station belongs.
    The fucntion chooses the area that has the least straight line distance from
    the station. The straight line distance betwen a stationa an area is determined 
    using the coordiates of the station and the coordinate sof the area.
    
    param: station - the name of the station on interest
    param: stations - the names and locations of all stations
    param: areas - the names and locations of all stations
    
    return: area - the name of the closest area
    '''
    from_location = stations.loc[station]
    index = get_shortest_distance(a=from_location, b=areas)
    
    return areas.index[index]

def get_nearest_area(station, stations, happiness):
    
    h = happiness.copy(deep=True)
    areas = pd.concat([h.area_codes, h.area_latitude, h.area_longitude], axis=1)
    
    return h.area_codes.loc[get_area_index(station, stations, areas)]
     
def get_station_area(station_coords, happiness):
    
    areas = []
    for station in station_coords.index:
        area = get_nearest_area(station, station_coords.drop(columns=["label"]), happiness)
        areas.append(area)
        
    
    station_area = pd.concat([pd.Series(station_coords.index), pd.Series(areas, name="area")], axis=1)

    return station_area

def join_weather_to_area_codes(weather, station_coords, happiness):
    ''' join sampled weather data with area codes and store in a database collection
    '''
    station_coords = station_coords.set_index("index")
    station_area = get_station_area(station_coords, happiness)
    
    
    weather_area = pd.concat([weather, station_area.set_index("index")], axis=1, sort=True)
    
    return weather_area

def flatten(aggregate_obj, to_extract):
    '''
    Custom function to flatten a nested dictionary that
    results from a mongodb aggregation.
    '''
    # Extracct nested entires
    extract = aggregate_obj[to_extract][0]
    
    # discard redundant dict items
    aggregate_obj.pop(to_extract)
    aggregate_obj.pop("_id")
    extract.pop("_id")
    
    # return flattened dictionary
    return {**aggregate_obj, **extract}
    
    
def aggregation_to_dataframe(query, database, station_coords):
    '''
    Custom function that returns the results of a mongodb 
    aggregation in a dataframe
    '''
    
    entries = []
    
    # collect entries
    for entry in database.aggregate(query):
        
        flat_entry = flatten(aggregate_obj=entry, to_extract="happiness")
        entries.append(flat_entry)
        
    #create dataframe
    joined = pd.DataFrame(entries)
    
    # Include station coordinates
    station_coords = station_coords.drop(columns=["label"])
    station_coords = station_coords.set_index("index")
    station_coords.columns = ["station_lat", "station_long"]
    
    joined = pd.concat([joined, station_coords.reset_index().drop(columns=["index"])], axis=1)
    joined.drop(columns=["INDEX","area","month","year"], inplace=True)
    
    return joined  
    
def get_correlation(data, feature1, feature2):
    
    scaler = MinMaxScaler()
    h = data.drop(columns=["area_names", "area_codes"]).dropna()

    x = np.array(h[feature1]).reshape(-1,1)
    scaler.fit(x)
    x = scaler.transform(x)

    y = np.array(h[feature2]).reshape(-1,1)
    scaler.fit(y)
    y = scaler.transform(y)

    return pearsonr(x,y)[0][0]

def report_corelations(joined, features, metrics):
    
    for metric in metrics:
        for attribute in features:
            coef = get_correlation(data=joined, feature1=attribute, feature2=metric)
            print("pearson correlation:", metric,"and", attribute, "=", round(coef, 4))
    
