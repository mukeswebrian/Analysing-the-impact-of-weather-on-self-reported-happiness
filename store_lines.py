from multiprocessing import Process
from pymongo import MongoClient
import pandas as pd

data = pd.read_excel("weather_data.xlsx")


def make_lineObj(line, data):
    
    return {
        "station_name" : data.station_name.iloc[line],
        "year" : data.year.iloc[line],
        "month" : data.month.iloc[line],
        "tmax_degC" : data.tmax_degC.iloc[line],
        "tmin_degC" : data.tmin_degC.iloc[line],
        "af_days" : data.af_days.iloc[line],
        "rain_mm" : data.rain_mm.iloc[line],
        "sun_hours" : data.sun_hours.iloc[line] 
    }


def storeObj(obj):
    collection = "weather_data"
    
    client = MongoClient(host="MIDGARD09", port=27017)
    db = client.pitds_weather_data
    weather_data = db[collection]
    
    weather_data.insert_one(obj)



def store_lines(batch):
    
    batch_dict = {"b1" : [0, 5],
                  "b2" : [10, 15],
                  "b3" : [20, 25],
                  "b4" : [30, 35]
                 }
    
    start = batch_dict[batch][0]
    end = batch_dict[batch][1]

    worked = []
    for row in range(start, end):
        storeObj(make_lineObj(row, data))
        worked.append(row)
        
        
    
        # report every 100 entries
        if row%2000 == 0:
            print("stored entries:", row)
            
    return worked
