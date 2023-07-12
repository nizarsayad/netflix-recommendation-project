import pandas as pd
import time
import argparse

def read_csv(file_path: str, chunk_size: int= 1000000, nb_chunks: int = 1) -> pd.DataFrame:
    """
    Reads a CSV file and returns a DataFrame.

    This function reads a CSV file from the given file path. It supports reading the data in chunks.

    Parameters:
    file_path (str): The path to the CSV file.
    chunk_size (int, optional): The size of chunks to read at a time. Default is 1,000,000.
    nb_chunks (int, optional): The number of chunks to read. If None, reads the full data. Default is 1.

    Returns:
    DataFrame: The loaded data.
    """
    if nb_chunks == None:
        print("Reading full data...")
        df = pd.read_csv(file_path)
    
    else:
        print("Reading data in chunks...")
        df_iter = pd.read_csv(file_path, iterator=True, chunksize=chunk_size)
        df = pd.concat([chunk for chunk in df_iter][:nb_chunks])
    return df
    
def filter_by_date(df, year_start=2005, year_end=None, month_start=6, month_end=12):
    """
    Filters a DataFrame by date and saves the filtered data to a CSV file.

    This function filters a DataFrame by year and month. It saves the filtered data to a CSV file named 
    "sample_data_{suffix}.csv", where "suffix" is based on the filter parameters.

    Parameters:
    df (DataFrame): The DataFrame to filter.
    year_start (int, optional): The start year for filtering. Default is 2005.
    year_end (int, optional): The end year for filtering. If None, filters by start year only. Default is None.
    month_start (int, optional): The start month for filtering. Default is 6.
    month_end (int, optional): The end month for filtering. If None, filters by start month only. Default is 12.

    Returns: None
    """
    if year_start is not None and year_end is not None:
        df = df[df['year'].between(year_start, year_end)]
        suffix = f"{year_start}_{year_end}"
    elif year_start is not None:
        df = df[df['year'] == year_start]
        suffix = f"{year_start}"

    if month_start is not None and month_end is not None:
        df = df[df['month'].between(month_start, month_end)]
        suffix = suffix+"_"+f"{month_start}_{month_end}"
    elif month_start is not None:
        df = df[df['month'] == month_start]
        suffix = suffix+"_"+f"{month_start}"
    # Save filtered data to CSV file
    print("Saving data...")
    df.to_csv(f"sample_data_{suffix}.csv",index= False)
    print("Done saving data!")




if __name__ == "__main__":
    # Start timer
    start_time = time.time()

    print("Loading data...")
    df = read_csv("filtered_data_sorted.csv", chunk_size=1000000, nb_chunks=None)
    print("Getting data sample...")
    year_start = 2005
    year_end = None
    month_start = 10
    month_end = 12
    df = filter_by_date(df, year_start=year_start, year_end=year_end, month_start=month_start, month_end=month_end)

    # End timer and print total time
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")