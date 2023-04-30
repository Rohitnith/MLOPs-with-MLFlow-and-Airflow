'''
filename: utils.py
functions: encode_features, load_model
creator: RC
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import collections

#Function to read data
def load_data(file_path_list):
    data = []
    for eachfile in file_path_list:
        data.append(pd.read_csv(eachfile, index_col=0))
    return data

#Function to read input data from db
def __read_input_data(DB_PATH,DB_FILE_NAME, table_name):
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from '+ table_name, cnx)
    df.drop(columns=['level_0', 'index'], axis = 1, inplace=True, errors='ignore')
    cnx.close()
    print("Data has been extracted successfully from lead_scoring_model_experimentation.")
    return df

#Function to save data  into db
def __save_data_to_db(DB_PATH,DB_FILE_NAME, input_data, table):
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    input_data.to_sql(name=table, con=cnx, if_exists='replace')
    print('input_data has been saved successfully to table ' + table);
    cnx.close()

###############################################################################
# Define the function to encode featuresof the the model
# ##############################################################################


def encode_features(DB_PATH,DB_FILE_NAME,FEATURES_TO_ENCODE,ONE_HOT_ENCODED_FEATURES):
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''
    input_data = __read_input_data(DB_PATH,DB_FILE_NAME,"interactions_mapped")
    df = input_data[FEATURES_TO_ENCODE]
    encoded_df = pd.DataFrame(columns= FEATURES_TO_ENCODE)
    placeholder_df = pd.DataFrame()
    # One-Hot Encoding using get_dummies for the specified categorical features
    for f in FEATURES_TO_ENCODE:
        if(f in df.columns):
            encoded = pd.get_dummies(input_data[f])
            encoded = encoded.add_prefix(f + '_')
            placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
        else:
            print('Feature not found')
    # Implement these steps to prevent any mismatch in dimensions during inference
    for feature in ONE_HOT_ENCODED_FEATURES:
        if feature in input_data.columns:
            encoded_df[feature] = input_data[feature]
        if feature in placeholder_df.columns:
            encoded_df[feature] = placeholder_df[feature]
    # fill all null values
    encoded_df.fillna(0, inplace=True)
    #Splitting X,y 
    X = encoded_df.drop('app_complete_flag',axis=1)
    y = encoded_df[['app_complete_flag']]
    
    encoded_df.drop('app_complete_flag', axis=1, inplace=True, errors='ignore')
    __save_data_to_db(DB_PATH,DB_FILE_NAME, encoded_df, 'features')


###############################################################################
# Define the function to get model predicions
# ##############################################################################

def get_models_prediction(DB_PATH,DB_FILE_NAME, MODEL_NAME, STAGE, TRACKING_URI):
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    mlflow.set_tracking_uri(TRACKING_URI)
    X = __read_input_data(DB_PATH, DB_FILE_NAME, 'features')
    model_uri = f"models:/{MODEL_NAME}/{STAGE}".format(MODEL_NAME=MODEL_NAME, STAGE=STAGE)
    #model_uri='runs:/5bd33c4640ec48e983c69dc4f2a6cca3/models'
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    
    predictions = loaded_model.predict(pd.DataFrame(X))
    print(predictions)
    predicted_output = pd.DataFrame(predictions, columns=['predicted_output']) 
    __save_data_to_db(DB_PATH, DB_FILE_NAME, predicted_output, 'predicted_output')
    return "Predictions are done and save in Final_Predictions Table"

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check(DB_PATH, DB_FILE_NAME):
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    input_data = __read_input_data(DB_PATH, DB_FILE_NAME, 'predicted_output')
    outputfile_name = 'prediction_distribution_'+ datetime.now().strftime("%Y%m%d%H%M%S") +'.txt'
    #input_data.to_csv(outputfile_name, header=None, index=None, sep='\t')
    
    output = input_data.groupby(['predicted_output']).size().reset_index(name='counts')
    count_0 = output[output['predicted_output'] == 0]
    count_0 = count_0['counts'][0]
    count_1 = output[output['predicted_output'] == 1]
    count_1 = count_1['counts'][1]

    result_1 = round((count_1/len(input_data.index))*100, 2)
    result_0 = round((count_0/len(input_data.index))*100, 2)
    data = {'is_churn':['0', '1'], 'percentage(%)':[result_0, result_1]}  
    result_df = pd.DataFrame(data)
    result_df.set_index(['is_churn'])
    result_df.to_csv('/home/codepro/03_inference_pipeline'+ outputfile_name, header=None, index=None, sep='\t')

    print('Output file has been generated successfully ' + outputfile_name)

###############################################################################
# Define the function to check the columns of input features
# ##############################################################################
   

def input_features_check(DB_PATH,DB_FILE_NAME,ONE_HOT_ENCODED_FEATURES):
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    input_data = __read_input_data(DB_PATH, DB_FILE_NAME, 'features')
    source_cols = input_data.columns.to_list()
    
    if collections.Counter(source_cols) == collections.Counter(ONE_HOT_ENCODED_FEATURES):
        print('All the models input are present')
    else:
        print('Some of the models inputs are missing')
   