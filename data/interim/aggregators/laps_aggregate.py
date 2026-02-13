import pandas as pd
from ..utils.utils import CYAN, RESET

def aggregate_laps_data(laps_df: pd.DataFrame):
        """
        Aggregates laps data by calculating statistics for lap times and other performance metrics.
        """
        # Aggragate laps data
        print(f'{CYAN}INFO: Aggregating laps data{RESET}')
        print(f'{CYAN}************ Laps DataFrame ***********{RESET}')
        print(laps_df.head())

        # Drop columns that are not needed for aggregation
        laps_df = laps_df.drop(columns=['unnamed:_0','deletedreason','lapstartdate','fastf1generated'], errors='ignore')

        # Convert time object columns to seconds
        laps_aggregated = laps_df.copy()
        TIME_COLS = ['time', 'laptime', 'pitouttime', 'pitintime', 'sector1time', 'sector2time', 'sector3time',
            'sector1sessiontime', 'sector2sessiontime', 'sector3sessiontime', 'lapstarttime',
        ]
        
        for col in TIME_COLS:
            laps_aggregated[col] = pd.to_timedelta(laps_aggregated[col], errors='coerce')
            laps_aggregated[col + '_seconds'] = laps_aggregated[col].dt.total_seconds()
       
        laps_aggregated = (laps_aggregated.groupby('sessionkey', as_index=False)
            .agg(
                lap_count=('lapnumber', 'max'),
                lap_mean=('laptime_seconds', 'mean'),
                lap_std=('laptime_seconds', 'std'),
                lap_best=('laptime_seconds', 'min'),
                lap_median=('laptime_seconds', 'median'),
            )
        )

        return laps_aggregated