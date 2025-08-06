import os
from airflow.models import Variable

class Config:
    FILE_PATH = Variable.get("income_data_path", 
                           default_var="/opt/airflow/dags/income_data")
    API_TIMEOUT = int(os.getenv('API_TIMEOUT', '10'))
    CSV_OUTPUT_PATH = os.getenv('CSV_OUTPUT_PATH', 
                               '/opt/airflow/dags/income_data/')
    
    @classmethod
    def update_file_path(cls, new_path):
        cls.FILE_PATH = new_path
