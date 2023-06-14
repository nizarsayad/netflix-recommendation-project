from confluent_kafka import Consumer
import json
import ccloud_lib
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests
import os

# Kafka config
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "netflix_recommendation" 
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'netflix_prediction'
consumer_conf['auto.offset.reset'] = 'latest'
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

# Get title-id mapping for all movies
df_title = pd.read_csv('https://netflix-project-bucket.s3.eu-west-3.amazonaws.com/data/movie_titles.csv',
                       encoding = "ISO-8859-1",
                        header = None, names = ['Movie_Id', 'Year', 'Name'], 
                        on_bad_lines='skip').set_index('Movie_Id')

def predict(user_id,movie_id):
    prediction = requests.get(f'http://35.180.109.35:4000/get-model?user_id={user_id}&movie_id={movie_id}').json()
    return prediction

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print(f'error: {msg.error()}')
        else:
            print("processing message...")
            record_key = msg.key()
            record_value = msg.value().decode('utf-8')
            print("loading json...")
            record_value_df = pd.read_json(record_value)
            print(f"received value = {record_value}")
            # process latest movies the user is currently watching
            user_curr_watching = record_value_df.copy() 
            # get user id
            user_id = user_curr_watching['customerID'][0]
            # get list of movies the user is currently watching
            watched_movies = user_curr_watching['Movie_Id'].tolist()
            # Predict user rating for a sample of movies
            user_pred = df_title.copy().sample(50)
            user_pred = user_pred.reset_index()
            # Remove the movies the user is currently watching from the list of movies to predict
            user_pred = user_pred[~user_pred['Movie_Id'].isin(watched_movies)]
            # Predict user rating for each movie using the predict function
            user_pred['Estimate_Score'] = user_pred['Movie_Id'].apply(lambda x: predict(user_id,x))
            # Add current time to the prediction
            user_pred["current_time"] = user_curr_watching["current_time"]
            # Rename columns
            user_pred.rename(columns={'Year':'YearRelease'}, inplace=True)
            # Add user id to the prediction dataframe
            user_pred['customerID'] = user_id
            # Reorganize columns order
            user_pred = user_pred[['customerID','Movie_Id','YearRelease','Name','current_time','Estimate_Score']]
            # Lower case column names
            user_pred = user_pred.rename(columns=str.lower).sort_values('estimate_score', ascending=False)
            # Renaming current_time because it created a conflict with the database
            user_pred.rename(columns={'current_time':'request_time'}, inplace=True)
            # Get top 5 movies
            top_movies = user_pred.head(5).pivot(index='customerid', columns='movie_id', values='name')
            top_movies.columns = [f'top{i+1}' for i in range(5)]
            top_movies['request_time'] = user_pred['request_time'].iloc[0]
            top_movies.reset_index(inplace=True)
            # Specify your PostgreSQL database, and credentials
            pg_db_uri = os.getenv('POSTGRES_DB_URI')
            engine = create_engine(pg_db_uri)

            # Use pandas to_sql() function to insert the DataFrame data into the table
            user_pred.to_sql('netflix_predictions', engine, if_exists='append', index=False)
            top_movies.to_sql('top_movies', engine, if_exists='append', index=False)

            
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
