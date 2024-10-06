from confluent_kafka import Producer
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='/Users/piotrtrybus/Documents/python stuff/weather_app_with_database/.env')

def weather_call():
    api_key = os.getenv('api_key')
    city = input('Pick a city: ')
    url = f'https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&aqi=no'
    query_data = {}
    
    response = requests.get(url)
    
    if not api_key:
        print("Error: API key not found. Check your .env file.")
        return

    elif response.status_code == 200: 
        weather_data = response.json()
        for i in weather_data:
            query_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'city_name': weather_data['location']['name'],
            'country_name': weather_data['location']['country'],
            'curr_temp': weather_data['current']['temp_c'],
            'curr_desc': weather_data['current']['condition']['text'],
            'curr_wind': weather_data['current']['wind_kph'],
            'curr_time': weather_data['current']['last_updated']
            }

        city_name = query_data['city_name']
        curr_temp = query_data['curr_temp']
        curr_desc = query_data['curr_desc']
        curr_wind = query_data['curr_wind']
        country_name = query_data['country_name']
        curr_time = query_data['curr_time']

        print(f'''The current weather in {city_name}, {country_name} as of {curr_time}:
                - {curr_desc}
                - Temperature: {curr_temp} C
                - Wind: {curr_wind} KM/H''')
        
        return query_data
                
    else:
        print(f'Error: {response.status_code} - {response.text}')

def send_to_kafka(query_data):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    topic_name = 'weather_query_data'
    data = query_data
    producer.produce(topic_name, key=None, value=json.dumps(data).encode('utf-8'))
    producer.flush() 
    print(f"Data sent to topic {topic_name}: {data}")

def main():
    query_data = weather_call()

    if query_data:
        send_to_kafka(query_data)
    else:
        print(f'No data found in query_data variable')

main()