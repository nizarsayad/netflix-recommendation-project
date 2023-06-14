import pandas as pd
import time

def filter_data():
    print("Filtering data...")
    print("Reading data...")
    df = pd.read_csv("processed_data.csv")
    print("Done reading data!")
    agg_functions = ['count','mean']
    print("Processing data...")
    df_movie_summary = df.groupby('movie_id')['rating'].agg(agg_functions)
    df_movie_summary.index = df_movie_summary.index.map(int)
    movie_benchmark = round(df_movie_summary['count'].quantile(0.7),0)
    drop_movie_list = df_movie_summary[df_movie_summary['count'] < movie_benchmark].index
    print('Movie minimum times of review: {}'.format(movie_benchmark))
    df_cust_summary = df.groupby('customer_id')['rating'].agg(agg_functions)
    df_cust_summary.index = df_cust_summary.index.map(int)
    cust_benchmark = round(df_cust_summary['count'].quantile(0.7),0)
    print(cust_benchmark)
    drop_cust_list = df_cust_summary[df_cust_summary['count'] < cust_benchmark].index
    print('Customer minimum times of review: {}'.format(cust_benchmark))
    print('Original Shape: {}'.format(df.shape))
    df = df[~df['movie_id'].isin(drop_movie_list)]
    df = df[~df['customer_id'].isin(drop_cust_list)]
    print('After Trim Shape: {}'.format(df.shape))
    print('Data ready for saving!')
    df.to_csv("filtered_data.csv",index= False)
    print("Done saving data!")

if __name__ == "__main__":
    # Start timer
    start_time = time.time()
    filter_data()
    # End timer
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")