ROOT_FOLDER= "/home/codepro/"
DB_PATH = ROOT_FOLDER +"database/"
DB_FILE_NAME = "data_cleaning.db"

DB_FILE_MLFLOW = ROOT_FOLDER+ 'database/Lead_scoring_mlflow_production.db'

FILE_PATH =ROOT_FOLDER+ '03_inference_pipeline/data/leadscoring_inference.csv'

TRACKING_URI = "http://0.0.0.0:6006"

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = 'LightGBM'
STAGE = 'production'
EXPERIMENT = 'Lead_scoring_mlflow_production'

ONE_HOT_ENCODED_FEATURES = ['total_leads_droppped','city_tier','first_platform_c_Level8', 'first_platform_c_others','first_platform_c_Level2','first_utm_medium_c_others','first_utm_medium_c_Level13', 'first_utm_medium_c_Level0','first_platform_c_Level7', 'first_platform_c_Level0', 'app_complete_flag']

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['first_platform_c', 'first_utm_medium_c','first_utm_source_c'] 
