import pandas as pd
import os
import time
import boto3
from botocore.exceptions import ClientError
def process_chunk(chunk, prev_movie_id):
    # Convert the "customer_id" column to string type
    chunk.reset_index(drop=True, inplace=True)
    chunk["customer_id"] = chunk["customer_id"].astype(str)
    # Find rows with movie ids
    movie_id_rows = chunk[chunk["customer_id"].str.contains(":")].index
    # If chunk contains movie ids
    if len(movie_id_rows) > 0:
        # If chunk starts with a customer rating row, use the previous movie id
        if movie_id_rows[0] != 0:
            movie_ids = [(prev_movie_id, (0, movie_id_rows[0]))]
        else:
            movie_ids = []

        # Create a list of tuples with movie id and the corresponding index range
        for i in range(len(movie_id_rows) - 1):
            movie_id = chunk.at[movie_id_rows[i], "customer_id"].replace(":", "")
            idx_range = (movie_id_rows[i] + 1, movie_id_rows[i + 1])
            movie_ids.append((movie_id, idx_range))

        # Add last movie id and its index range
        movie_id = chunk.at[movie_id_rows[-1], "customer_id"][:-1]
        idx_range = (movie_id_rows[-1] + 1, len(chunk))
        movie_ids.append((movie_id, idx_range))

        # Store the last movie id for the next chunk
        next_movie_id = movie_id
    else:
        # If chunk does not contain movie ids, use the previous movie id
        movie_ids = [(prev_movie_id, (0, chunk.shape[0] - 1))]
        next_movie_id = prev_movie_id
    # Create a dataframe with movie ids, customer ids, ratings, and dates
    data = []
    for movie_id, (start, end) in movie_ids:
        customer_ratings = chunk.iloc[start:end].copy()
        customer_ratings["movie_id"] = int(movie_id)
        data.append(customer_ratings)

    processed_chunk = pd.concat(data, ignore_index=True)
    return processed_chunk, next_movie_id


def process_files(chunksize:int = 1000000, drop_date:bool = False):

    # Process data in chunks
    chunksize = chunksize
    data_files = ["combined_data_1.txt", "combined_data_2.txt", "combined_data_3.txt", "combined_data_4.txt"]


    
    # get the parent directory of the current script
    parent_dir = os.path.dirname(os.path.abspath("preprocess.py"))
    
    # construct the path to the src directory
    data_dir = os.path.join(parent_dir, "..", "data")
   
    # construct the path to the files
    data_files_path = [os.path.join(data_dir, file) for file in data_files]
    
    prev_movie_id = None
    for file_path,file_name in zip(data_files_path, data_files):
        # Initialize the master dataframe
        master_df = pd.DataFrame()
        i=1
        print(f"Processing {file_name}...")
        for chunk in pd.read_csv(file_path, chunksize=chunksize, header=None, names=["customer_id", "rating", "date"]):
            print(f"Processing chunk n°{i}")
            processed_chunk, prev_movie_id = process_chunk(chunk, prev_movie_id)
            processed_chunk = processed_chunk[["customer_id", "movie_id", "rating", "date"]]
            i+=1
            if drop_date==True:
                processed_chunk.drop(columns=["date"], inplace=True)
            master_df = pd.concat([master_df, processed_chunk], ignore_index=True)
            del processed_chunk
        print(f"Saving processed {file_name} to csv...")
        if file_name == "combined_data_1.txt":
            master_df.to_csv("processed_data.csv", mode="a", index=False)
        else:
            master_df.to_csv("processed_data.csv", mode="a", index=False, header=False)
        print(f"Done processing {file_name}!")
        del master_df
    
    print("Done processing all files!")
    

if __name__ == "__main__":
    # Start timer
    start_time = time.time()
    process_files()
    # End timer
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")