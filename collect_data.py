import requests
import csv
from datetime import datetime, timedelta

API_KEY = 'e6545216d6704fdb90875513242611'
BASE_URL = 'http://api.weatherapi.com/v1/history.json'

# Function to fetch weather data for a specific date
def fetch_weather_data(location, date):
    params = {
        'key': API_KEY,
        'q': location,
        'dt': date
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

# Function to save data to a CSV file
def save_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Time', 'Temperature', 'Humidity', 'Wind Speed', 'Weather Condition'])
        for entry in data:
            writer.writerow(entry)

# Main function to collect data for multiple days
def collect_weather_data(location, days):
    weather_data = []
    today = datetime.now()
    for i in range(days):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        data = fetch_weather_data(location, date)
        for hour in data['forecast']['forecastday'][0]['hour']:
            weather_data.append([
                hour['time'],
                hour['temp_c'],
                hour['humidity'],
                hour['wind_kph'],
                hour['condition']['text']
            ])
    save_to_csv(weather_data, './data/weather_data.csv')

# Collect weather data for the past 5 days for a specific location
collect_weather_data('New York', 5)
