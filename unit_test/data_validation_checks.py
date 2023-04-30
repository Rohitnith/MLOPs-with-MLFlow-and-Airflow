"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
from collections import Counter
import os
import sqlite3
from sqlite3 import Error

###############################################################################
# Define function to validate raw data's schema
############################################################################### 
             
def load_data(file_path_list):
    data = []
    for eachfile in file_path_list:
        data.append(pd.read_csv(eachfile, index_col=0))
    return data

def raw_data_schema_check(DATA_DIRECTORY,raw_data_schema):
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    raw_data=load_data([f"{DATA_DIRECTORY}leadscoring.csv",])[0]
    columns=raw_data.columns.to_list()
    if (Counter(columns)== Counter(raw_data_schema)):
        return 'Raw datas schema is in line with the schema present in schema.py'
    else:
        return 'Raw datas schema is NOT in line with the schema present in schema.py'
    

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check(DB_PATH,DB_FILE_NAME,model_input_schema):
    
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df = pd.read_sql('select * from interactions_mapped', cnx)
    df.drop(columns=['index'], inplace=True, axis=1, errors='ignore')
    source_columns = df.columns.to_list()
    result =  all(elem in source_columns for elem in model_input_schema)
    if result:
        print('Models input schema is in line with the schema present in schema.py')
    else:
        print('Models input schema is NOT in line with the schema present in schema.py')    
                    
                    
    
                    
    

    
    
