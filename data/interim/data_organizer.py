import pandas as pd
import os
from .utils.utils import standardize_cols, drop_columns, CYAN, RESET, extract_features_from_df
from .aggregators.weather_aggregate import aggregate_weather_data
from .aggregators.track_status_aggregate import aggregate_track_status_data
from .aggregators.laps_aggregate import aggregate_laps_data


class DataOrganizer:
    def __init__(self, raw_data_dir: str, organized_data_dir: str):
        self.raw_data_dir = raw_data_dir
        self.organized_data_dir = organized_data_dir
        os.makedirs(self.organized_data_dir, exist_ok=True)

    # # Normalize common column names
    # def standardize_cols(self, df):
    #     """
    #     Standardizes column names by stripping whitespace, replacing spaces with underscores, and converting to lowercase.
    #     """
    #     df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
    #     return df

    # def aggregate_weather_data(self, weather_df: pd.DataFrame):
    #     """
    #     Aggregates weather data by calculating mean, min, max, and variability for key weather parameters.
    #     """
    #     # Aggregate weather data 
    #     print(f'{CYAN}INFO: Aggregating weather data{RESET}')
    #     print(f'{CYAN}************ Weather DataFrame Head ***********{RESET}')
    #     print(weather_df.head())

    #     weather_df = weather_df.drop(columns=['unnamed:_0'], errors='ignore')

    #     weather_aggregated = weather_df.copy().groupby('sessionkey', as_index=False).agg(
    #         air_temp_mean=('airtemp', 'mean'),
    #         air_temp_min=('airtemp', 'min'),
    #         air_temp_max=('airtemp', 'max'),
    #         track_temp_mean=('tracktemp', 'mean'),
    #         track_temp_min=('tracktemp', 'min'),
    #         track_temp_max=('tracktemp', 'max'),
    #         humidity_mean=('humidity', 'mean'),
    #         humidity_min=('humidity', 'min'),
    #         humidity_max=('humidity', 'max'),
    #         wind_speed_mean=('windspeed', 'mean'),
    #         wind_speed_min=('windspeed', 'min'),
    #         wind_speed_max=('windspeed', 'max'), 
    #         rain_any=('rainfall', 'max'),                 # True if any True exists
    #         rain_samples_ratio=('rainfall', 'mean'),      # Ratio of True samples
    #         rain_samples=('rainfall', 'size'),
    #     )

    #     # Calculate weather variability (standard deviation)
    #     weather_variability = (
    #         weather_df.copy().groupby('sessionkey', as_index=False)
    #         .agg(
    #             air_temp_std=('airtemp', 'std'),
    #             track_temp_std=('tracktemp', 'std'),
    #             humidity_std=('humidity', 'std'),
    #             wind_speed_std=('windspeed', 'std'),
    #             )
    #     )

    #     # Merge aggregated weather data with variability
    #     weather_aggregated = weather_aggregated.merge(weather_variability, on='sessionkey', how='left')

    #     return weather_aggregated
    
    # def aggregate_track_status_data(self, track_status_df: pd.DataFrame):
    #     """
    #     Aggregates track status data by calculating counts and durations of different track statuses.
    #     """
    #     # Convert Time column of track_status_df
    #     print(f'{CYAN}INFO: Aggregating track status data{RESET}')
    #     print(f'{CYAN}************ Track Status DataFrame Head ***********{RESET}')
    #     print(track_status_df.head())

    #     ts = track_status_df.copy()
    #     ts = ts.drop(columns=['unnamed:_0'], errors='ignore')
    #     ts['time'] = pd.to_timedelta(ts['time'], errors='raise')

    #     # Sort and compute duration each status lasted
    #     ts = ts.sort_values(by=['sessionkey', 'time'])
    #     ts['next_time'] = ts.groupby('sessionkey')['time'].shift(-1)
    #     ts['duration'] = (ts['next_time'] - ts['time'])
    #     # Replace the last row with 0 seconds.
    #     ts['duration'] = ts['duration'].fillna(pd.Timedelta(seconds=0))

    #     # Aggregate track status durations
    #     ts['yellow'] = ts['status'].astype(str).eq('2')
    #     ts['red'] = ts['status'].astype(str).eq('5')
    #     ts['vsc_deployed'] = ts['status'].astype(str).eq('6')
    #     ts['vsc_ending'] = ts['status'].astype(str).eq('7')
    #     ts['sc_deployed'] = ts['status'].astype(str).eq('4')
    #     ts['not_green'] = ts['status'].astype(str).ne('1')


    #     track_status_aggregated = (ts.groupby('sessionkey', as_index=False)
    #         .agg(
    #             yellow_count=('yellow', 'sum'),
    #             red_count=('red', 'sum'),
    #             vsc_deployed_count=('vsc_deployed', 'sum'),
    #             sc_deployed_count=('sc_deployed', 'sum'),
    #             vsc_duration=('duration', lambda x: x[ts.loc[x.index,'vsc_deployed']].sum()),
    #             vsc_ending_duration=('duration', lambda x: x[ts.loc[x.index,'vsc_ending']].sum()),
    #             sc_duration=('duration', lambda x: x[ts.loc[x.index,'sc_deployed']].sum()),
    #             not_green_duration=('duration', lambda x: x[ts.loc[x.index,'not_green']].sum()),
    #             total_duration=('duration', 'sum'),
    #         )
    #     )

    #     # Convert timedeltas into seconds
    #     track_status_aggregated['vsc_duration_in_seconds'] = track_status_aggregated['vsc_duration'].dt.total_seconds()
    #     track_status_aggregated['vsc_ending_duration_in_seconds'] = track_status_aggregated['vsc_ending_duration'].dt.total_seconds()
    #     track_status_aggregated['sc_duration_in_seconds'] = track_status_aggregated['sc_duration'].dt.total_seconds()
    #     track_status_aggregated['not_green_duration_in_seconds'] = track_status_aggregated['not_green_duration'].dt.total_seconds()
    #     track_status_aggregated['total_duration_in_seconds'] = track_status_aggregated['total_duration'].dt.total_seconds()

    #     track_status_aggregated['disruption_ratio'] = track_status_aggregated['not_green_duration_in_seconds'] / track_status_aggregated['total_duration_in_seconds'].replace(0, 1)  # Avoid division by zero

    #     return track_status_aggregated
    
    # def aggregate_laps_data(self, laps_df: pd.DataFrame):
    #     """
    #     Aggregates laps data by calculating statistics for lap times and other performance metrics.
    #     """
    #     # Aggragate laps data
    #     print(f'{CYAN}INFO: Aggregating laps data{RESET}')
    #     print(f'{CYAN}************ Laps DataFrame ***********{RESET}')
    #     print(laps_df.head())

    #     # Drop columns that are not needed for aggregation
    #     laps_df = laps_df.drop(columns=['unnamed:_0','deletedreason','lapstartdate','fastf1generated'], errors='ignore')

    #     # Convert time object columns to seconds
    #     laps_aggregated = laps_df.copy()
    #     TIME_COLS = ['time', 'laptime', 'pitouttime', 'pitintime', 'sector1time', 'sector2time', 'sector3time',
    #         'sector1sessiontime', 'sector2sessiontime', 'sector3sessiontime', 'lapstarttime',
    #     ]
        
    #     for col in TIME_COLS:
    #         laps_aggregated[col] = pd.to_timedelta(laps_aggregated[col], errors='coerce')
    #         laps_aggregated[col + '_seconds'] = laps_aggregated[col].dt.total_seconds()
       
    #     laps_aggregated = (laps_aggregated.groupby('sessionkey', as_index=False)
    #         .agg(
    #             lap_count=('lapnumber', 'max'),
    #             lap_mean=('laptime_seconds', 'mean'),
    #             lap_std=('laptime_seconds', 'std'),
    #             lap_best=('laptime_seconds', 'min'),
    #             lap_median=('laptime_seconds', 'median'),
    #         )
    #     )

    #     return laps_aggregated

    def organize_session_data(self, year: int, grand_prix: str, session_type: str):
        """
        Organizes raw session data into a structured format.

        Args:
            year (int): The year of the season.
            grand_prix (str): The name of the grand prix.
            session_type (str): The type of session ('FP1', 'FP2', 'FP3', 'Q', 'S', 'SS', 'SQ', 'R').
        """
        grand_prix = grand_prix.replace(' ', '_')

        # Define file paths
        laps_file = os.path.join(self.raw_data_dir, f'{year}_{grand_prix}_{session_type}_laps.csv')
        weather_file = os.path.join(self.raw_data_dir, f'{year}_{grand_prix}_{session_type}_weather.csv')
        results_file = os.path.join(self.raw_data_dir, f'{year}_{grand_prix}_{session_type}_results.csv')
        track_status_file = os.path.join(self.raw_data_dir, f'{year}_{grand_prix}_{session_type}_track_status.csv')
        session_info_file = os.path.join(self.raw_data_dir, f'{year}_{grand_prix}_{session_type}_session_info.csv')           

        # Load raw data
        laps_df = pd.read_csv(laps_file)
        weather_df = pd.read_csv(weather_file)
        results_df = pd.read_csv(results_file)
        track_status_df = pd.read_csv(track_status_file)
        session_info_df = pd.read_csv(session_info_file)

        # Standardize column names
        laps_df = standardize_cols(laps_df)
        weather_df = standardize_cols(weather_df)
        results_df = standardize_cols(results_df)
        track_status_df = standardize_cols(track_status_df)
        session_info_df = standardize_cols(session_info_df)        

        # Merge data from cv files under a shared key into one DataFrame
        print(f'{CYAN}INFO: Confirm sessionkey to dataframes{RESET}')
        print(session_info_df.head())
        print(f'{CYAN}Session Key:', session_info_df.copy()['sessionkey'].iloc[0], f'{RESET}')
        session_info_df = session_info_df.drop(columns=['unnamed:_0'], errors='ignore')

        print(f'{CYAN}INFO: Merging dataframes by key{RESET}')
        for merged_df in [laps_df, weather_df, results_df, track_status_df]:
            merged_df['sessionkey'] = session_info_df['sessionkey'].iloc[0]

        print(f'{CYAN}INFO: Results DataFrame{RESET}')
        print(f'{CYAN}************ Results DataFrame Head ***********{RESET}')
        print(results_df.head())
        # results_df = results_df.drop(columns=['unnamed:_0', 'countrycode'], errors='ignore')
        results_df = drop_columns(results_df)

        weather_aggregated = aggregate_weather_data(weather_df)
        track_status_aggregated = aggregate_track_status_data(track_status_df)
        laps_aggregated = aggregate_laps_data(laps_df)

        # Merge all organized data into a single DataFrame
        print(f'{CYAN}INFO: Merging all organized data into a single DataFrame{RESET}')
        merged_df = laps_aggregated.merge(weather_aggregated, on='sessionkey', how='left') \
            .merge(results_df, on='sessionkey', how='left') \
            .merge(track_status_aggregated, on='sessionkey', how='left') \
            .merge(session_info_df, on='sessionkey', how='left')
        print(f'{CYAN}INFO: Merged DataFrame info:{RESET}')
        print(merged_df.info())
        print(f'{CYAN}INFO: Merged DataFrame head:{RESET}')
        print(merged_df.head())

        # Save new DataFrame to a csv file in organized_data_dir for processing
        output_file = os.path.join(self.organized_data_dir, f'{year}_{grand_prix}_{session_type}_organized.csv')
        merged_df.to_csv(output_file, index=False)
        print(f'{CYAN}INFO: Organized data saved to {output_file}{RESET}')

        # Save list of processed session for further processing
        list_of_files = os.path.join(self.organized_data_dir, 'list_of_files.csv')
        if os.path.exists(list_of_files):
            existing_files_df = pd.read_csv(list_of_files)
            new_file_entry = pd.DataFrame(f'{year}_{grand_prix}_{session_type}_organized.csv', columns=['filename'])
            updated_files_df = pd.concat([existing_files_df, new_file_entry], ignore_index=True)
            updated_files_df.to_csv(list_of_files, index=False)
        else:
            new_file_entry = pd.DataFrame(f'{year}_{grand_prix}_{session_type}_organized.csv', columns=['filename'])
            new_file_entry.to_csv(list_of_files, index=False)
        

        return merged_df
        

# org = DataOrganizer(raw_data_dir='../raw/csv_files', organized_data_dir='./organized_data/csv_files')
# result = org.organize_session_data(2018, 'Monaco', 'R')
