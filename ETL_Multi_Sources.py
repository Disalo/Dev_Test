import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import numpy as np

# Define a a function to extract .csv files
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process, index_col=0)
    return dataframe

# Define a a function to extract .json files
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

# Define a a function to extract .xml files
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = str(car.find("fuel").text)
        dataframe = dataframe.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture,
         "price":price, "fuel":fuel}, ignore_index=True)
    return dataframe

# Define a a function to extract entire files
def extract():
    # create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    
    #process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data

# Define a function to transform fuel price from USD to GBP and drop USD price column
def transform(data):
    # Transform price from USD to GBP
    data['price in GBP'] = np.round(np.float64(data['price'] * 0.732398), 3)
    
    # Drop 'price' column (in USD) 
    data = data.drop(['price'], axis=1)
    
    return data

# Define a function to load the transformed data to a csv file
def load(data_to_load):
    data_to_load.to_csv('transformed_data.csv', index=False)

# Define a function to log the ETL process
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

# Run ETL
if __name__ == '__main__':
    log("ETL Job Started")
    log("Extract phase Started")
    extracted_data = extract()
    log("Extract phase Ended")
    log("Transform phase Started")
    transformed_data = transform(extracted_data) 
    log("Transform phase Ended")
    log("Load phase Started")
    load(transformed_data)
    log("Load phase Ended")
    log("ETL Job Ended")
