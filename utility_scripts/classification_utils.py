'''
Author Exam Number: B145418
Date: June 1, 2019
Institution: University of Edinburgh 
'''

# import relevant libraries
import pandas as pd
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split
import numpy as np

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

def get_station_names():
    
    names = open("stations.txt", 'r').readlines()
    clean_names = []
    for name in names:
        clean_names.append(name.replace("\n", ""))
        
    return clean_names

def get_lat_long_text(station):
    '''
    Get the lines containing the latitude and longitude information
    of a specified station
    '''
    lines = open("data\\"+station+".txt", "r").readlines()
    text = ""
    
    for line in lines:
        if "Lat" in line:
            text = line
            break
            
    return text

def parse_line(line):
    '''
    Extract latitude and longitude data from a line entry
    '''
    tokens = line.split(" ")
    lat_long = []
    
    for i in range(0, len(tokens)):
        
        if "Lat" in tokens[i]:
            lat_long.append(tokens[i+1].strip().replace(",",""))
        elif "Lon" in tokens[i]:
            lat_long.append(tokens[i+1].strip().replace(",",""))
        else:
            pass
        
        if len(lat_long) >= 2:
            break
            
    return lat_long

def get_lat_long():
    "Extract latitude longitude data and store it in a data frame"
    locations = {}
    for station in get_station_names():
        lat_long = parse_line(get_lat_long_text(station))
        locations[station] = lat_long
    
    return pd.DataFrame(locations, index=["latitude", "longitude"]).transpose()

def get_regions(num_regions, strt=49.9, end=60.9):
    '''
    obtain the region boundaries for the specified number of regions
    '''
    bounds = []
    count = strt
    step = (end - strt)/num_regions
    
    while count < end:
        bounds.append(round(count, 2))
        count += step
              
        if len(bounds) == num_regions:
            bounds.append(end)
            break
            
    regions = {}
    label = 0
    for i in range(0, len(bounds)-1):
        regions[str(label)] = bounds[i], bounds[i+1]
        label +=1
         
    return regions

def get_label(latitude, regions):
    '''
    obtain the appropriate label for a specifed latitude
    '''
    label = -1
    for key in regions.keys():
        if latitude >= regions[key][0] and latitude <= regions[key][1]:
            label = key
            break
            
    return int(label)

def store_station_labels(database, num_regions):
    
    # read station coordinates
    coords = get_lat_long()
    
    # set data types
    coords.latitude = coords.latitude.astype(float)
    coords.longitude = coords.longitude.astype(float)
    
    # create a labels column
    coords = pd.concat([coords, pd.Series(name="label", index=coords.index)], axis=1)
    
    # get region boundsries
    regions = get_regions(num_regions)
    
    # label all stations
    for index in range(0, len(coords)):
        coords.iat[index, 2] = get_label(coords.latitude.iloc[index], regions)
       
    # store labeled station data
    coords = coords.reset_index()
    for row in range(0, len(coords)):
        storeObj(make_lineObj(row, coords), database)
    

def get_accuracy_scores(labeled_data, test_size, features):
    
    X = np.array(labeled_data[features])
    y = np.array(labeled_data.label)

    # Randomly select five stations to be part of the test set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=1000)

    # Fit and evaluate a gaussioan naive bayes classifier
    clf = GaussianNB()
    clf.fit(X_train, y_train)
    
    # check accuracy score on training set
    print("\nTraining set accuracy: ", round(clf.score(X_train, y_train),4))

    # check accuracy score in test set
    print("Testing set accuracy: ", round(clf.score(X_test, y_test),4))
    
    