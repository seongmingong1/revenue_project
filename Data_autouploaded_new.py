
"""
Main DAG file for automating income data processing and revenue calculation
수업 데이터 처리 및 수익 계산 자동화를 위한 메인 DAG 파일
"""

import logging
import os
from config import Config
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from airflow.operators.email import EmailOperator
import pandas as pd 
import numpy as np 
from dotenv import load_dotenv
from forex_python.converter import CurrencyRates
import requests
import pymysql

# Initial setup and configurations
# 초기 설정 및 구성
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('income_etl.log'),
        logging.StreamHandler()
    ]
)
pymysql.install_as_MySQLdb()
load_dotenv()

file_path = Variable.get("income_data_path", default_var="/opt/airflow/dags/income_data")
Config.update_file_path(file_path)


"""def connect_to_db():
    try: 
        password = os.getenv("MYSQL_PASSWORD")
        connection = mysql.connector.connect(
            host = "localhost",
            user= "root",
            password = password,
            database="job_project"

        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None
"""

# Database connection function 

def connect_to_db():
    """
    Establishes connection to MySQL database using Airflow's MySqlHook
    Airflow의 MySqlHook을 사용하여 MySQL 데이터베이스 연결을 설정
    
    Returns:
        connection: MySQLConnection object
    """
    #connection = None
    try:
        hook = MySqlHook(mysql_conn_id='airflow_db')
        connection = hook.get_conn()
        return connection
    
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise
    

def get_latest_file(folder_path):
    """
    Finds the most recently modified Excel file in the specified directory
    지정된 디렉토리에서 가장 최근에 수정된 Excel 파일을 찾음
    
    Args:
        folder_path (str): Path to directory containing Excel files
    
    Returns:
        str: Full path to the latest Excel file
        None: If no Excel files found
    """
    try:
        files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
        if not files:
            logging.error("No excel files found in directory")
            return None
        latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        return os.path.join(folder_path, latest_file)
    except Exception as e:
        logging.error(f"No excel files found in directory: {e}")
        raise
        


def upload_mysql(file_path):
    """
    Reads income data from Excel file and uploads to MySQL database
    Excel 파일에서 수업 데이터를 읽어서 MySQL 데이터베이스에 업로드
    
    Args:
        file_path (str): Path to directory containing Excel files
    
    Process:
    1. Connects to database
    2. Gets latest Excel file
    3. Validates data format
    4. Inserts data into income table
    """
    connection = connect_to_db()
    if connection is None: 
        logging.error("Database connection failed")
        return
    
    cursor = None
    try: 
        excel_file = get_latest_file(file_path)
        if excel_file is None:
            raise ValueError("No excel file found in directory")
        
        df = pd.read_excel(excel_file, engine='openpyxl')
        validate_income_data(df)

        cursor = connection.cursor()

        for _, row in df.iterrows():
            income_date = pd.to_datetime(row['income_date']).strftime('%Y-%m-%d')
            insert_query = """
            INSERT IGNORE INTO income (client_id, income_date, income_type, client_name)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['client_id'],
                income_date,
                row['income_type'],
                row['client_name']
                ))
    
        connection.commit()
        logging.info(f"Data from {file_path} successfully inserted into MySQL")
    except Exception as e:
        logging.error(f"Error in upload_mysql: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def validate_income_data(df): 
    """
    Validates the required columns and data format in data
    데이터의 필수 컬럼과 데이터 형식 검증
    
    Args:
        df (pandas.DataFrame): DataFrame containing income data
        
    Raises:
        ValueError: If required columns missing or data format invalid
    """
    required_columns = ['client_id', 'income_date', 'income_type','client_name' ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns: 
        raise ValueError(f"requeired columns have been missed: {missing_columns}")
    
    if not pd.to_datetime(df['income_date'], errors='coerce').notna().all():
        raise ValueError("invaild data format in 'income_date'")
    
    if df['client_id'].isnull().any():
        raise ValueError("Null Value in client_id")

def get_exchange_rates(): 
    """
    Fetches current exchange rates from fixer.io API
    fixer.io API에서 현재 환율 정보를 가져옴
    
    Returns:
        dict: Exchange rates for USD, EUR, RUB, KRW conversions
    """
    try: 
        api_key = Variable.get("api_key")
        url = f"http://data.fixer.io/api/latest?access_key={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()  

        if not data.get('success', False):
            raise ValueError(f"API failed: {data.get('error','unknown')}")
        
        # currency calculation 
        eur_to_usd = data['rates']['USD']  # 1 EUR -> USD 환율
        eur_to_rub = data['rates']['RUB']  # 1 EUR -> RUB
        eur_to_krw = data['rates']['KRW'] # 1EUR -> KRW
        EUR = data['rates']['EUR']

        usd_to_eur = 1 / eur_to_usd
        rub_to_eur = 1 / eur_to_rub
        usd_to_krw = usd_to_eur * eur_to_krw
        rub_to_krw = rub_to_eur * eur_to_krw

        return {
            'usd_to_eur': usd_to_eur,
            'rub_to_eur': rub_to_eur,
            'eur_to_krw': eur_to_krw,
            'usd_to_krw': usd_to_krw,
            'rub_to_krw': rub_to_krw
        }
    except requests.Timeout:
        logging.error("currency rate API request Time out")
        raise

    except requests.RequestException as e :
        logging.error(f"currency API request failed: {e}")
        raise

def calculate_daily_revenue(ti):
    """
    Calculates daily revenue from income data with currency conversion
    수업 데이터로부터 일일 수익을 계산하고 환율을 적용
    
    Process:
    1. Fetches client and income data
    2. Merges datasets
    3. Applies exchange rates
    4. Calculates total revenue in EUR and KRW
    """
    try:
        logging.info("Daily revenue calculation started")

        hook = MySqlHook(mysql_conn_id = 'airflow_db')
        conn = hook.get_conn()


        clients_df = pd.read_sql("SELECT * FROM clients", conn)
        incomes_df = pd.read_sql("SELECT * FROM income", conn)
        conn.close()

        merged_df = pd.merge(incomes_df, clients_df, on ='client_id')
        merged_df['income_date'] = pd.to_datetime(merged_df['income_date'], errors='coerce')

        rates = get_exchange_rates()


        #daily revenue
        daily_revenue = merged_df.groupby(['income_date','currency']).apply(lambda x: (x['fee']).sum()).unstack()
        daily_revenue['total_eur'] = (
            daily_revenue['USD'].fillna(0) * rates['usd_to_eur'] +
            daily_revenue['RUB'].fillna(0) * rates['rub_to_eur'] + 
            daily_revenue['EUR'].fillna(0)
            ).round(2)
    
        daily_revenue['total_krw'] = (
            daily_revenue['KRW'].fillna(0) + 
            daily_revenue['USD'].fillna(0) * rates['usd_to_krw'] + 
            daily_revenue['RUB'].fillna(0) * rates['rub_to_krw'] + 
            daily_revenue['EUR'].fillna(0)* rates['eur_to_krw']
            ).round(2)
    
        daily_revenue.to_csv(f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv", index = True)
        ti.xcom_push(key='daily_revenue_path', value=f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv")

    except Exception as e :
        logging.error(f"Error in daily revenue calculation: {e}")
        raise




def calculate_weekly_revenue(ti):
    """
    Aggregates daily revenue into weekly summaries
    일일 수익을 주간 단위로 집계
    
    Process:
    1. Reads daily revenue data
    2. Groups by week
    3. Saves weekly summary to CSV
    """

    try: 
        logging.info("Starting weekly revenue calculation")
   
        daily_revenue_path = ti.xcom_pull(key='daily_revenue_path', task_ids='daily_task') 
        daily_revenue = pd.read_csv(daily_revenue_path, index_col=0)
        logging.info(f"Columns in daily_revenue: {daily_revenue.columns.tolist()}")
        #daily_revenue['income_date'] = pd.to_datetime(daily_revenue['income_date'])
        daily_revenue.index = pd.to_datetime(daily_revenue.index)

        weekly_revenue = daily_revenue.groupby(pd.Grouper(freq='W-SUN')).sum()


        weekly_revenue_path = f"{Config.CSV_OUTPUT_PATH}weekly_revenue.csv"
        weekly_revenue.to_csv(weekly_revenue_path, index=True)
        ti.xcom_push(key='weekly_revenue_path', value=weekly_revenue_path)

        logging.info("weekly revenue calculation completed")

    except Exception as e:
        logging.error(f"Error in weekly revenue calculation: {e}")
        raise



def calculate_monthly_revenue(ti):
    """
    Aggregates daily revenue into monthly summaries
    일일 수익을 월간 단위로 집계
    
    Process:
    1. Reads daily revenue data
    2. Groups by month
    3. Saves monthly summary to CSV
    """
    try:
        logging.info("Starting monthly revenue calculation")
    
        # 여기서 변수명을 daily_revenue_path로 수정
        daily_revenue_path = ti.xcom_pull(key='daily_revenue_path', task_ids='daily_task')
        daily_revenue = pd.read_csv(daily_revenue_path, index_col=0)
        daily_revenue.index = pd.to_datetime(daily_revenue.index)

        # 월별 수입 계산 (이번 달도 포함)
        current_date = pd.to_datetime('today')

        monthly_revenue = daily_revenue[daily_revenue.index <= current_date].groupby(pd.Grouper(freq='M')).sum()

        monthly_revenue_path = f"{Config.CSV_OUTPUT_PATH}monthly_revenue.csv"
        monthly_revenue.to_csv(monthly_revenue_path, index=True)
        ti.xcom_push(key='monthly_revenue_path', value=monthly_revenue_path)

        logging.info("monthly revenue calculation completed")

    except Exception as e:
        logging.error(f"Error in monthly revenue calculation: {e}")
        raise
        
def save_to_csv(ti):
    """
    Verifies all revenue CSV files have been created
    모든 수익 CSV 파일이 생성되었는지 확인
    
    Process:
    1. Checks for daily, weekly, monthly CSV files
    2. Logs file locations
    3. Raises error if any file missing
    """

    try:
        logging.info("Starting to save CSV files")
    
   
        daily_revenue_path = ti.xcom_pull(key='daily_revenue_path', task_ids='daily_task')
        weekly_revenue_path = ti.xcom_pull(key='weekly_revenue_path', task_ids='weekly_task')
        monthly_revenue_path = ti.xcom_pull(key='monthly_revenue_path', task_ids='monthly_task')

  
        if all([daily_revenue_path, weekly_revenue_path, monthly_revenue_path]):
            logging.info("All CSV files have been saved successfully.")
            logging.info(f"Daily revenue at: {daily_revenue_path}")
            logging.info(f"Weekly revenue at: {weekly_revenue_path}")
            logging.info(f"Monthly revenue at: {monthly_revenue_path}")
        else:
            raise ValueError("One or more revenue paths are missing")

    except Exception as e: 
        logging.error(f"Error checking CSV files: {e}")
        raise

# DAG definition 
# DAG 정의 
"""
    Task sequence:
    1. Upload income data from Excel
    2. Calculate daily revenue
    3. Calculate weekly summary
    4. Calculate monthly summary
    5. Verify CSV files
    6. Send email with reports
    
    작업 순서:
    1. Excel에서 수업 데이터 업로드
    2. 일일 수익 계산
    3. 주간 요약 계산
    4. 월간 요약 계산
    5. CSV 파일 확인
    6. 보고서 이메일 발송
"""
with DAG(
    dag_id = "Data_autouploaded",
    schedule = "0 0 * * *", # Run daily at midnight
    start_date=pendulum.datetime(2024,10,1, tz="Europe/Madrid"),
    catchup= False
) as dag: 
    upload_task = PythonOperator(
        task_id = 'upload_task',
        python_callable=upload_mysql,
        op_args=[Config.FILE_PATH]
        #op_args=[os.path.join(os.path.dirname(__file__), 'income_data')]

    )

    daily_task =PythonOperator(
        task_id = 'daily_task',
        python_callable=calculate_daily_revenue
    )

    weekly_task = PythonOperator(
        task_id = 'weekly_task',
        python_callable=calculate_weekly_revenue
    )

    monthly_task = PythonOperator(
        task_id = 'monthly_task',
        python_callable=calculate_monthly_revenue
    )

    csv_task = PythonOperator(
        task_id ='csv_task',
        python_callable=save_to_csv
    )

    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to='seongmingong14@gmail.com',
        subject = 'Revenue result',
        html_content = """"
        <h3>Auto mail sending system</h3>
        <p>Daily, weekly, monthly revenue with csv files</p>
        """,
        files=[
            f"{Config.CSV_OUTPUT_PATH}daily_revenue.csv",
            f"{Config.CSV_OUTPUT_PATH}weekly_revenue.csv",
            f"{Config.CSV_OUTPUT_PATH}monthly_revenue.csv"
        ]
    )

upload_task >> daily_task >> weekly_task >> monthly_task >> csv_task >> send_email_task
