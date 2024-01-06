from apis.helpers import convert_to_datetime
from ..weather_helpers import kelvin_to_temp
from apis.helpers import flatten_list_of_dicts

import pandas as pd
class WeatherLocation:

    def __init__(self, data, unit:str="F"):
        self.lat = data.get('lat')
        self.lon = data.get('lon')
        self.timezone = data.get('timezone')
        self.timezone_offset =  data.get('timezone_offset')


        current = data.get('current')

        self.current_time = convert_to_datetime(current.get('dt'))
        self.sunrise = convert_to_datetime(current.get('sunrise'))
        self.sunset = convert_to_datetime(current.get('sunset'))
        self.temp = kelvin_to_temp(current.get('temp'),unit=unit)
        self.feels_like = kelvin_to_temp(current.get('feels_like'), unit=unit)
        self.pressure = current.get('pressure')
        self.humidity = current.get('humidity')
        self.dew_point = current.get('dew_point')
        self.uvi = current.get('uvi')
        self.clouds = current.get('clouds')
        self.visibility = current.get('visibility')
        self.wind_speed = current.get('wind_speed')
        self.wind_degree = current.get('wind_deg')
        
        weather = current.get('weather')
        self.weather_summary = [i.get('main') for i in weather]
        self.weather_desc = [i.get('description') for i in weather]

        self.data_dict = {
            'lat': self.lat,
            'lon': self.lon,
            'timezone': self.timezone,
            'timezone_offset': self.timezone_offset,
            'current_time': self.current_time,
            'sunrise': self.sunrise,
            'sunset': self.sunset,
            'temp': self.temp,
            'feels_like': self.feels_like,
            'pressure': self.pressure,
            'humidity': self.humidity,
            'dew_point': self.dew_point,
            'uvi': self.uvi,
            'clouds': self.clouds,
            'visibility': self.visibility,
            'wind_speed': self.wind_speed,
            'wind_degree': self.wind_degree,
            'weather_summary': self.weather_summary,
            'weather_desc': self.weather_desc
        }


        self.as_dataframe = pd.DataFrame(self.data_dict)
