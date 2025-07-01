import requests
import pandas as pd
import time
from datetime import datetime, timedelta

class Weather_data_ingestion:
    def __init__(self, api_key, station_id, start_date, end_date):
         """
        Initializes the WeatherCollector with API key, station ID, and date range.

        Args:
            api_key (str): Your Weather Channel API key.
            station_id (str): The ID of the weather station.
            start_date (datetime): The start date for data collection.
            end_date (datetime): The end date for data collection.
        """
        self.api_key = api_key
        self.station_id = station_id
        self.start_date = start_date
        self.end_date = end_date
        self.all_data = []
        
    def fetch_data_for_date(self, date_str):
         """
        Fetches hourly weather data for a specific date.

        Args:
            date_str (str): The date in YYYYMMDD format.

        Returns:
            list: A list of observation dictionaries, or an empty list if no data or an error occurs.
        """
        url = (
            f"https://api.weather.com/v2/pws/history/hourly?"
            f"stationId={self.station_id}&format=json&units=m&date={date_str}&apiKey={self.api_key}"
        )
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

            data = response.json()
            if "observations" in data:
                print(f"Pobrano dane dla {date_str}")
                return data["observations"]
            else:
                print(f"Brak danych dla {date_str}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Błąd pobierania {date_str}: {e}")
            return []
