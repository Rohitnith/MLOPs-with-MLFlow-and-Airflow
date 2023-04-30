# You can create more variables according to your project. The following are the basic variables that have been provided to you
ROOT_FOLDER="/home/codepro/"
DB_PATH = ROOT_FOLDER +"database/"
DB_FILE_NAME = "data_cleaning.db"
#UNIT_TEST_DB_FILE_NAME = "utils_output.db"
DATA_DIRECTORY = ROOT_FOLDER+"01_data_pipeline/notebooks/data/"
INTERACTION_MAPPING = ROOT_FOLDER+ "01_data_pipeline/scripts/"
INDEX_COLUMNS_TRAINING = ['created_date', 'city_tier', 'first_platform_c','first_utm_medium_c', 'first_utm_source_c','total_leads_droppped','referred_lead','app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c','first_utm_medium_c', 'first_utm_source_c','total_leads_droppped','referred_lead']
LEAD_SCORING_FILE="leadscoring.csv"
#NOT_FEATURES = []




