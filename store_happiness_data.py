'''
Author Exam Number: B145418
Date: June 1, 2019
Institution: University of Edinburgh 

Description: This script reads happiness data from the a source spreadsheet downloaded 
             from the Office for National Statistics website. 
             source url: http://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/datasets/personalwellbeingestimatesgeographicalbreakdown
             The script parses the spreadsheet and only selects data that corresponds
             to a specified set of area codes. The data is then stored in a mongodb
             database.
'''
import pandas as pd
import utility_scripts.happiness_data_utils as utils
from pymongo import MongoClient


# Load happiness data
happiness = pd.read_excel("geographicbreakdownreferencetable_tcm77-417203.xls",
                         sheet_name="Happiness", header=5)

# Clean happiness data
happiness = utils.clean_happiness_data(happiness)


# Store happiness data
server = {"host":"localhost",
          "port":27017,
          "dbName":"pitds_assessment3",
          "collection":"happiness"}

client = MongoClient(host=server["host"], port=server["port"])
db = client[server["dbName"]]
database = db[server["collection"]]

utils.store_data(happiness, database)




