'''
filename: utils.py
functions: encode_features, get_train_model
creator: RC
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np
from sklearn import preprocessing
import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score, recall_score
from sklearn.metrics import precision_recall_fscore_support

#create a sqlite db fo storing all the model artifacts etc
import sqlite3
from sqlite3 import Error

#Function to read input data from db
def __read_input_data(DB_PATH,DB_FILE_NAME):
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    df = pd.read_sql('select * from interactions_mapped', cnx)
    df.drop(columns=['level_0','index'], axis = 1, inplace=True, errors='ignore')
    cnx.close()
    print("Data has been extracted successfully from lead_scoring_model_experimentation.")
    return df

#Function to save input data to db
def __save_data_to_db(DB_PATH,DB_FILE_NAME, input_data, table):
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    input_data.to_sql(name=table, con=cnx, if_exists='replace')
    print('input_data has been saved successfully to table ' + table);
    cnx.close()

###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features(DB_PATH,DB_FILE_NAME,FEATURES_TO_ENCODE,ONE_HOT_ENCODED_FEATURES):
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        DB_FILE_NAME : Name of the database file 
        DB_PATH : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    '''
    input_data = __read_input_data(DB_PATH,DB_FILE_NAME)
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
    
    __save_data_to_db(DB_PATH,DB_FILE_NAME, X, 'feature')
    __save_data_to_db(DB_PATH,DB_FILE_NAME, y, 'target')
    
    print('input data has been written successfully to tables features & target')


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model(DB_PATH,DB_FILE_NAME,model_config, EXPERIMENT, TRACKING_URI):
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    X = pd.read_sql('select * from feature', cnx)
    y = pd.read_sql('select * from target', cnx)
    
    X.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    y.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.3, random_state= 0)
    print('Train test data loaded')
    mlflow.set_tracking_uri(TRACKING_URI)
    #Model Training
    with mlflow.start_run(run_name=EXPERIMENT) as mlrun:
        #Model Training
        clf = lgb.LGBMClassifier()
        clf.set_params(**model_config) # add ** airflow throws an error
        clf.fit(X_train, y_train)

        mlflow.sklearn.log_model(sk_model=clf, artifact_path="models",  registered_model_name='LightGBM')
        mlflow.log_params(model_config)    

        # predict the results on training dataset
        y_pred=clf.predict(X_test)

        # view accuracy
        acc=accuracy_score(y_pred, y_test)
        conf_mat = confusion_matrix(y_pred, y_test)
       
        precision = precision_score(y_pred, y_test,average= 'macro')
        recall = recall_score(y_pred, y_test, average= 'macro')
        cm = confusion_matrix(y_test, y_pred)
        tn = cm[0][0]
        fn = cm[1][0]
        tp = cm[1][1]
        fp = cm[0][1]
        class_zero = precision_recall_fscore_support(y_test, y_pred, average='binary',pos_label=0)
        class_one = precision_recall_fscore_support(y_test, y_pred, average='binary',pos_label=1)
        
        roc_auc = roc_auc_score(y_test, y_pred)
        
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric('test_accuracy', acc)
        mlflow.log_metric("Precision", precision)
        mlflow.log_metric("Recall", recall)
        mlflow.log_metric("Precision_0", class_zero[0])
        mlflow.log_metric("Precision_1", class_one[0])
        mlflow.log_metric("Recall_0", class_zero[1])
        mlflow.log_metric("Recall_1", class_one[1])
        mlflow.log_metric("f1_0", class_zero[2])
        mlflow.log_metric("f1_1", class_one[2])
        mlflow.log_metric("False Negative", fn)
        mlflow.log_metric("True Negative", tn)
        
        print('get_trained_model has been executed successfully.')

   
