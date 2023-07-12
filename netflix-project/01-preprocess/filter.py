import pandas as pd
import time

def filter_data():
    """
    Filters the Netflix Prize dataset and saves the filtered data to a CSV file.

    The function reads the processed data from "processed_data.csv". It filters out movies and customers based on the 
    70th percentile of the count of ratings for movies and customers. The data is then renamed and sorted by date, 
    and the sorted data is saved to "filtered_data_sorted.csv".

    Parameters: None

    Returns: None
    """
    # Start filtering data
    print("Filtering data...")

    # Read data from CSV file
    print("Reading data...")
    df = pd.read_csv("processed_data.csv")
    print("Done reading data!")

    # Define aggregation functions
    agg_functions = ['count','mean']

    print("Processing data...")

    # Group data by movie_id and calculate count and mean of ratings
    df_movie_summary = df.groupby('movie_id')['rating'].agg(agg_functions)
    df_movie_summary.index = df_movie_summary.index.map(int)

    # Calculate 70th percentile of count of ratings for movies
    movie_benchmark = round(df_movie_summary['count'].quantile(0.7),0)

    # Get list of movie_ids to drop
    drop_movie_list = df_movie_summary[df_movie_summary['count'] < movie_benchmark].index

    print('Movie minimum times of review: {}'.format(movie_benchmark))

    # Group data by customer_id and calculate count and mean of ratings
    df_cust_summary = df.groupby('customer_id')['rating'].agg(agg_functions)
    df_cust_summary.index = df_cust_summary.index.map(int)

    # Calculate 70th percentile of count of ratings for customers
    cust_benchmark = round(df_cust_summary['count'].quantile(0.7),0)

    # Get list of customer_ids to drop
    drop_cust_list = df_cust_summary[df_cust_summary['count'] < cust_benchmark].index

    print('Customer minimum times of review: {}'.format(cust_benchmark))

    print('Original Shape: {}'.format(df.shape))

    # Filter data
    df = df[~df['movie_id'].isin(drop_movie_list)]
    df = df[~df['customer_id'].isin(drop_cust_list)]

    # Rename columns and convert date column to timestamp
    df.rename(columns={'customer_id': 'user_id', 'date':'timestamp', 'rating': 'user_rating'}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    print('After Trim Shape: {}'.format(df.shape))
    print('Data ready for saving!')

    # Sort data by date
    print("Sorting data...")
    df.sort_values(by='timestamp', inplace=True, ascending=False)
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month

    print("Done sorting data!")
    print("Saving sorted data...")
    df.to_csv("filtered_data_sorted.csv",index= False)

if __name__ == "__main__":
    # Start timer
    start_time = time.time()

    # Call filter_data function
    filter_data()

    # End timer and print total time
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")
