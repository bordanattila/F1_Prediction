"""
Concatenates individual CSV files into a single DataFrame and adding new features 'year', 'event_id' and 'row_id' using the list_of_files.csv.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from f1_prediction_ml.cleaning.utils import get_list_of_sessions

data_path = project_root / 'data' / 'interim' / 'organized_csv_files' / '{input}.csv'

list_of_sessions = get_list_of_sessions()

def concatenate_csv_files(list_of_sessions):
    dataframes = []
    for session in list_of_sessions:
        file_path = data_path.format(input=session)
        df = pd.read_csv(file_path)
        dataframes.append(df)

    concatenated_df = pd.concat(dataframes, ignore_index=True)

    return concatenated_df
    