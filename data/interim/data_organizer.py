import pandas as pd
import os

class DataOrganizer:
    def __init__(self, raw_data_dir: str, organized_data_dir: str):
        self.raw_data_dir = raw_data_dir
        self.organized_data_dir = organized_data_dir
        os.makedirs(self.organized_data_dir, exist_ok=True)

    # Normalize common column names
    def standardize_cols(df):
        """

        Args:
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
        return df

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
        laps_file = os.path.join(self.raw_data_dir, f"{year}_{grand_prix}_{session_type}_laps.csv")
        weather_file = os.path.join(self.raw_data_dir, f"{year}_{grand_prix}_{session_type}_weather.csv")
        results_file = os.path.join(self.raw_data_dir, f"{year}_{grand_prix}_{session_type}_results.csv")
        track_status_file = os.path.join(self.raw_data_dir, f"{year}_{grand_prix}_{session_type}_track_status.csv")
        session_info_file = os.path.join(self.raw_data_dir, f"{year}_{grand_prix}_{session_type}_session_info.csv")           

        # Load raw data
        laps_df = pd.read_csv(laps_file)
        weather_df = pd.read_csv(weather_file)
        results_df = pd.read_csv(results_file)
        track_status_df = pd.read_csv(track_status_file)
        session_info_df = pd.read_csv(session_info_file)

        laps_df = self.standardize_cols(laps_df)
        weather_df = self.standardize_cols(weather_df)
        results_df = self.standardize_cols(results_df)
        track_status_df = self.standardize_cols(track_status_df)
        session_info_df = self.standardize_cols(session_info_df)

        # Merge data from cv files under a shared key into one DataFrame
        for merged_df in [laps_df, weather_df, results_df, track_status_df]:
            merged_df['sessionkey'] = session_info_df['sessionkey'].iloc[0]

        
        # Merge laps and weather data on DriverNumber
        merged_df = pd.merge(laps_df, weather_df, on='DriverNumber', how='outer')

        # Merge with results data
        merged_df = pd.merge(merged_df, results_df, on='DriverNumber', how='outer')

        # Merge with track status data
        merged_df = pd.merge(merged_df, track_status_df, on='DriverNumber', how='outer')

        # Merge merged_df under a shared keys (GrandPrix,Date,SessionType) into one DataFrame
        session_info = {
            'GrandPrix': grand_prix,
            'Date': merged_df['Date'].iloc[0] if 'Date' in merged_df.columns else None,
            'SessionType': session_type
        }
        session_info_df = pd.DataFrame([session_info])
        merged_df = pd.merge(merged_df, session_info_df, how='cross')

        # Save new DataFrame to a csv file in organized_data_dir for processing
        output_file = os.path.join(self.organized_data_dir, f"{year}_{grand_prix}_{session_type}_organized.csv")
        merged_df.to_csv(output_file, index=False)
        print(f"INFO: Organized data saved to {output_file}")

        return merged_df
        

org = DataOrganizer(raw_data_dir='../raw', organized_data_dir='./')
result = org.organize_session_data(2023, 'São Paulo Grand Prix', 'FP1')

print(result.head())  
