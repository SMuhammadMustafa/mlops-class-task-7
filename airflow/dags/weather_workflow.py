from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import csv

# Function to collect weather data
def collect_weather_data():
    API_KEY = 'e6545216d6704fdb90875513242611'
    BASE_URL = 'http://api.weatherapi.com/v1/history.json'
    location = 'Islamabad'
    days = 5
    weather_data = []
    today = datetime.now()
    for i in range(days):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        params = {'key': API_KEY, 'q': location, 'dt': date}
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        for hour in data['forecast']['forecastday'][0]['hour']:
            weather_data.append([
                hour['time'],
                hour['temp_c'],
                hour['humidity'],
                hour['wind_kph'],
                hour['condition']['text']
            ])
    with open('../../data/weather_data.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Temperature', 'Humidity', 'Wind Speed', 'Weather Condition'])
        writer.writerows(weather_data)

# Function to preprocess data
def preprocess_data():
    df = pd.read_csv('../../data/weather_data.csv')
    df['Temperature'] = df['Temperature'].fillna(df['Temperature'].mean())
    df['Humidity'] = df['Humidity'].fillna(df['Humidity'].mean())
    df['Wind Speed'] = df['Wind Speed'].fillna(df['Wind Speed'].mean())
    df['Weather Condition'] = df['Weather Condition'].fillna(df['Weather Condition'].mode()[0])
    df['Temperature'] = (df['Temperature'] - df['Temperature'].min()) / (df['Temperature'].max() - df['Temperature'].min())
    df['Wind Speed'] = (df['Wind Speed'] - df['Wind Speed'].min()) / (df['Wind Speed'].max() - df['Wind Speed'].min())
    df.to_csv('../../data/processed_data.csv', index=False)

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to collect and preprocess weather data daily',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the tasks
collect_task = PythonOperator(
    task_id='collect_weather_data',
    python_callable=collect_weather_data,
    dag=dag,
)

preprocess_task = PythonOperator(
    task_id='preprocess_weather_data',
    python_callable=preprocess_data,
    dag=dag,
)

# Set task dependencies
collect_task >> preprocess_task
