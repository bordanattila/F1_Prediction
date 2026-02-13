import pandas as pd
from ..utils.utils import CYAN, RESET

def aggregate_weather_data(weather_df: pd.DataFrame):
    """
    Aggregates weather data by calculating mean, min, max, and variability for key weather parameters.
    """
    # Aggregate weather data 
    print(f'{CYAN}INFO: Aggregating weather data{RESET}')
    print(f'{CYAN}************ Weather DataFrame Head ***********{RESET}')
    print(weather_df.head())

    weather_df = weather_df.drop(columns=['unnamed:_0'], errors='ignore')

    weather_aggregated = weather_df.copy().groupby('sessionkey', as_index=False).agg(
        air_temp_mean=('airtemp', 'mean'),
        air_temp_min=('airtemp', 'min'),
        air_temp_max=('airtemp', 'max'),
        track_temp_mean=('tracktemp', 'mean'),
        track_temp_min=('tracktemp', 'min'),
        track_temp_max=('tracktemp', 'max'),
        humidity_mean=('humidity', 'mean'),
        humidity_min=('humidity', 'min'),
        humidity_max=('humidity', 'max'),
        wind_speed_mean=('windspeed', 'mean'),
        wind_speed_min=('windspeed', 'min'),
        wind_speed_max=('windspeed', 'max'), 
        rain_any=('rainfall', 'max'),                 # True if any True exists
        rain_samples_ratio=('rainfall', 'mean'),      # Ratio of True samples
        rain_samples=('rainfall', 'size'),
    )

    # Calculate weather variability (standard deviation)
    weather_variability = (
        weather_df.copy().groupby('sessionkey', as_index=False)
        .agg(
            air_temp_std=('airtemp', 'std'),
            track_temp_std=('tracktemp', 'std'),
            humidity_std=('humidity', 'std'),
            wind_speed_std=('windspeed', 'std'),
            )
    )

    # Merge aggregated weather data with variability
    weather_aggregated = weather_aggregated.merge(weather_variability, on='sessionkey', how='left')

    return weather_aggregated