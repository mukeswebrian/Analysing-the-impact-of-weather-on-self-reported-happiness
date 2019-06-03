'''
Author: Brian Mukeswe
Institution: The University of Edinburgh
Department: School of Informatics
Date: May 17, 2019

'''
from multiprocessing import Process
from pymongo import MongoClient
import pandas as pd

data = pd.read_excel("weather_data.xlsx")


def make_lineObj(line, data):
    
    # create mongo document object
    # ints are cast as floats to solve mongo encoding issue
    return {
        "index" :  float(data.index[line]),
        "station_name" : data.station_name.iloc[line],
        "year" : float(data.year.iloc[line]),
        "month" : float(data.month.iloc[line]),
        "tmax_degC" : data.tmax_degC.iloc[line],
        "tmin_degC" : data.tmin_degC.iloc[line],
        "af_days" : float(data.af_days.iloc[line]),
        "rain_mm" : data.rain_mm.iloc[line],
        "sun_hours" : data.sun_hours.iloc[line] 
    }


def storeObj(obj):
    collection = "weather_data"
    
    client = MongoClient(host="localhost", port=27017)
    db = client.pitds_weather_data
    weather_data = db[collection]
    
    weather_data.insert_one(obj)


def store_lines(batch):
    
    batch_dict = {"b1" : [0, 9200],
                  "b2" : [9200, 18400],
                  "b3" : [18400, 27600],
                  "b4" : [24600, 37259]
                 }
    
    start = batch_dict[batch][0]
    end = batch_dict[batch][1]

    worked = []
    for row in range(start, end):
        storeObj(make_lineObj(row, data))
        worked.append(row)
        
    
        # report every 2000 entries
        if row%2000 == 0:
            print("stored entries:", row)
            
    return worked
