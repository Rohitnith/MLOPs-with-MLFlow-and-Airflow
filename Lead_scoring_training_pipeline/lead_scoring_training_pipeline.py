##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import importlib.util

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

utils = module_from_file("utils", "/home/codepro/02_training_pipeline/scripts/utils.py")
constants= module_from_file("constants","/home/codepro/02_training_pipeline/scripts/constants.py")

#import constants and variables

db_path= constants.DB_PATH
db_file_name=constants.DB_FILE_NAME
tracking_uri=constants.TRACKING_URI
experiement=constants.EXPERIMENT
model_config=constants.model_config
features_to_encode=constants.FEATURES_TO_ENCODE
one_hot_encoded_features=constants.ONE_HOT_ENCODED_FEATURES

###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################


encode_features= PythonOperator(task_id='encode_cat_features',
                                python_callable=utils.encode_features,
                                op_kwargs={"DB_PATH":db_path,
                                           "DB_FILE_NAME":db_file_name,
                                        "ONE_HOT_ENCODED_FEATURES":one_hot_encoded_features,
                                           "FEATURES_TO_ENCODE":features_to_encode},
                                dag=ML_training_dag)

###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################

training_model= PythonOperator(task_id='training_model',
                                python_callable=utils.get_trained_model,
                                op_kwargs={"DB_PATH":db_path,
                                           "DB_FILE_NAME":db_file_name,
                                           "model_config":model_config,
                                           "EXPERIMENT":experiement,
                                           "TRACKING_URI":tracking_uri},
                                dag=ML_training_dag)

###############################################################################
# Define relations between tasks
# ##############################################################################

encode_features>>training_model