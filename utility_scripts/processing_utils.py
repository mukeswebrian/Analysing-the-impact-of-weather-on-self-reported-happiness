'''
Author Exam Number: B145418
Date: May 31, 2019
Institution: University of Edinburgh 
'''

# import relevant libraries
import requests
from pymongo import MongoClient
import pandas as pd
import numpy as np

def get_station_names():
    '''read station names from a text file'''
    
    names = open("stations.txt", 'r').readlines()
    
    # clean names (remove new line marker)
    clean_names = []
    for name in names:
        clean_names.append(name.replace("\n", ""))
        
    return clean_names
    
    
def download_weather_data(url_root, station_names):
    
    suffix = "data.txt"
    
    for name in station_names:
        # download data
        url_content = requests.get(url_root + name + suffix).content
        
        # report successful or failed downloads
        if open("data\\"+name+".txt", "wb").write(url_content):
            print("successfully downloaded: "+ name)
        else:
            print("download failed: "+ name)
    

def add_station(station, data, index, to_remove):
    '''Adds the data of a specified station to a dataframe'''
    
    lines = open("data\\"+station+".txt").readlines()
    
    for line in range(8, len(lines)): # start from line 8 to skip header lines
        data, index = insert_line(lines[line], data, station, index, to_remove)
        
    return data, index

def append_row(data_frame, row_items):
    '''Add a new row to an existing data frame'''

    item_array = np.array(row_items).reshape(1,len(data_frame.columns))
    
    item_data = pd.DataFrame(item_array,columns=data_frame.columns) 
    
    return pd.concat([data_frame, item_data], axis=0)



def insert_line(line, data, station, index, to_remove):
    ''' clean the data entries on a given line in the text file
        return: A list containing the cleaned entries on the line
    '''
    cols = line.strip().split(" ")
    items = [station] # store the station name as the first line item
    
    for item in cols:
        if item!='':
            
            # Clean data entries
            clean_item = item
            for character in to_remove:
                clean_item = clean_item.replace(character, "")
            
            # use 1000 as a numeric indicator for a missing value
            clean_item = clean_item.replace("---","1000") 
            
            items.append(clean_item)
            
            # stop if 8  valid items have been cleaned
            if len(items)>=8:
                items.append(index)
                index += 1
                break
    
    # incase fewer than expected items were cleaned
    if len(items)<8:
        # append a missing value indicator if only 7 valid items are cleaned
        if len(items)==7:
            items.append("1000")
            items.append(index)
            index += 1
        
        # print out invalid line entries otherwise    
        else:
            print("invalid line skipped in:", station, items)
        
    # make sure the row item matches the dataframe size
    if len(items)==len(data.columns):
        data = append_row(data, items)   
        
    return data, index


def clean_weather_data(station_names, to_remove):
    '''
    cleans the weather data from each station and returns a single dataframe
    containing all the weather data
    '''
    
    # Create a dataframe for storing cleaned weather data
    column_names = ["station_name", "year", "month", "tmax_degC", "tmin_degC", "af_days", "rain_mm", "sun_hours", "INDEX"]
    data = pd.DataFrame(columns=column_names)
    index = 0 # used to track the number of items added to the dataframe
    
    for station in station_names:
        data, index = add_station(station, data, index, to_remove)
    
    # set numeric datatypes
    numeric_features = ["year", "month", "tmax_degC", "tmin_degC", "af_days", "rain_mm", "sun_hours", "INDEX"]
    for feature in numeric_features:
        data[feature] = data[feature].astype(float)
    
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
    configure specified database and store a dict object
    '''
    
    client = MongoClient(host=database["host"], port=database["port"])
    
    db = client[database["dbName"]]
    weather_data = db[database["collection"]]
    
    weather_data.insert_one(obj)  
      

def store_weather_data(data, database):
    for row in range(0, len(data)):
        storeObj(make_lineObj(row, data), database)
        
        # report every 200 entries
        if row%2000 == 0:
            print("stored entries:", row)

