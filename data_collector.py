"""
This is a script that iterates through the list of races and utilises the RawDataCollector class to pull data from FastF1.
"""
from data.raw.raw_data_collector import RawDataCollector
import time
import fastf1 as f1

session_year = 2018
sessions = ['Australia', 'Bahrain', 'China', 'Azerbaijan', 'Spain', 'Monaco', 'Canada', 'France', 'Austria', 'Great Britain', 'Germany',
         'Hungary', 'Belgium', 'Italy', 'Singapore', 'Russia', 'Japan', 'United States', 'Mexico', 'Brazil', 'Abu Dhabi']
session_type = ['FP1', 'FP2', 'FP3', 'SQ', 'Q', 'S', 'SS', 'R']

data_collector = RawDataCollector(cache_dir='./')

# Iterate through the list of races and session types
for session_name in sessions:
    session_name = session_name.replace(' ', '_')
    print(f'********** Processing session: {session_name} **********')
    for ses_type in session_type:
        try:
            print(f'********** Fetching data for session type: {ses_type} **********')
            session_data = data_collector.fetch_session_data(session_year, session_name, ses_type)
            
            if session_data is None:
                raise ValueError("No data found for this session type.")
            
        except f1.req.RateLimitExceededError as e:
            print(f"RATE LIMIT hit for {session_year} {session_name} {ses_type}: {e}")
            time.sleep(30)  # backoff
            continue
            
        except ValueError as ve:
            print(f"WARNING: {ve} for {session_year} {session_name} {ses_type} session.")
            continue

        except Exception as e:
            print(f"WARNING: Could not fetch data for {session_year} {session_name} {ses_type} session. Error: {e}")
            continue
        
        # Save the fetched data to CSV files
        session_data['laps'].to_csv(f'./data/raw/csv_files/{session_year}_{session_name}_{ses_type}_laps.csv')
        session_data['weather_data'].to_csv(f'./data/raw/csv_files/{session_year}_{session_name}_{ses_type}_weather.csv')
        session_data['results'].to_csv(f'./data/raw/csv_files/{session_year}_{session_name}_{ses_type}_results.csv')
        session_data['track_status'].to_csv(f'./data/raw/csv_files/{session_year}_{session_name}_{ses_type}_track_status.csv')
        session_data['session_info'].to_csv(f'./data/raw/csv_files/{session_year}_{session_name}_{ses_type}_session_info.csv')
        print(f"INFO: Saved data for {session_year} {session_name} {ses_type} session to CSV files.")