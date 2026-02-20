# Color codes
CYAN = "\033[36m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
RESET = "\033[0m"

# Normalize common column names
def standardize_cols(df):
    """
    Standardizes column names by stripping whitespace, replacing spaces with underscores, and converting to lowercase.
    """
    df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
    return df

# Drop unecessary columns
def drop_columns(df):
    """

    """
    cols = ['unnamed:_0', 'countrycode', 'headshoturl', 'firstname', 'lastname', 'broadcastname', 'fullname']
    return df.drop(columns=cols, errors='ignore')