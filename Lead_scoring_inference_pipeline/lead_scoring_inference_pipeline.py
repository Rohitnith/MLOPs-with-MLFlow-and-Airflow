##############################################################################
# Import necessary modules
# #############################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import importlib.util

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

#importing all the modules
utils=module_from_file("utils","/home/codepro/03_inference_pipeline/scripts/utils.py")
constants=module_from_file("constants","/home/codepro/03_inference_pipeline/scripts/constants.py")                    
#import all the constants
                           
db_path= constants.DB_PATH
db_file_name= constants.DB_FILE_NAME
db_file_mlflow= constants.DB_FILE_MLFLOW 
file_path= constants.FILE_PATH
tracking_uri= constants.TRACKING_URI
model_name= constants.MODEL_NAME
stage= constants.STAGE                           
experiment= constants.EXPERIMENT
one_hot_encoded_features= constants.ONE_HOT_ENCODED_FEATURES                           
features_to_encode= constants.FEATURES_TO_ENCODE     
                           
###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,4,23),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@hourly',
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################

encoding_categorical_variables= PythonOperator(task_id='encoding_cat_var',
                                 python_callable=utils.encode_features,
                                 op_kwargs={'DB_PATH':db_path,
                                            'DB_FILE_NAME':db_file_name,
                                            'FEATURES_TO_ENCODE':features_to_encode,
                                            'ONE_HOT_ENCODED_FEATURES':one_hot_encoded_features},
                                 dag=Lead_scoring_inference_dag)
                                            

###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
generating_models_prediction= PythonOperator(task_id='get_models_prediction',
                                 python_callable=utils.get_models_prediction,
                                 op_kwargs={'DB_PATH':db_path,
                                            'DB_FILE_NAME':db_file_name,
                                            'MODEL_NAME':model_name,
                                            'STAGE':stage,
                                            'TRACKING_URI':tracking_uri},
                                 dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
checking_model_prediction_ratio= PythonOperator(task_id='prediction_ratio_check',
                                 python_callable=utils.prediction_ratio_check,
                                 op_kwargs={'DB_PATH':db_path,
                                            'DB_FILE_NAME':db_file_name},
                                 dag=Lead_scoring_inference_dag)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
checking_input_features= PythonOperator(task_id='checking_input_features',
                                 python_callable=utils.input_features_check,
                                 op_kwargs={'DB_PATH':db_path,
                                            'DB_FILE_NAME':db_file_name,
                                            'ONE_HOT_ENCODED_FEATURES':one_hot_encoded_features},
                                 dag=Lead_scoring_inference_dag)
###############################################################################
# Define relation between tasks
# ##############################################################################

encoding_categorical_variables>>generating_models_prediction>>checking_model_prediction_ratio>>checking_input_features
