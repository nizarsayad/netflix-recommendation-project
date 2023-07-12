from confluent_kafka import Consumer
import ccloud_lib
import time
import pandas as pd
from sqlalchemy import create_engine
import mlflow
import os

# Kafka config
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "netflix_recommendation" 
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'netflix_prediction'
consumer_conf['auto.offset.reset'] = 'latest'
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])


# Specify your PostgreSQL database, and credentials
pg_db_uri = os.getenv('POSTGRES_DB_URI')
engine = create_engine(pg_db_uri)

# Read the current movies from s3 bucket and load the model
print("loading movie data...")
movies = pd.read_csv("s3://ns-data-resources-bucket/netflix-recommendation/current_movies.csv")
print("loading model...")
model_name = "svd_surprise"
client = mlflow.MlflowClient()
model_version = client.get_registered_model(name="svd_surprise").latest_versions[0].version
ranker = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
print("model loaded")
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
            # Add current time to the DataFrame
            user_curr_watching["timestamp"] = int(time.time()) 
            # get user id
            user_id = user_curr_watching['user_id'][0]
            # get list of movies the user is currently watching
            watched_movies = user_curr_watching['movie_id'].tolist()
            # Predict user rating for a sample of movies
            user_pred = movies.sample(50)
            # Add user id to the prediction
            user_pred['user_id'] = user_id
            #user_pred = user_pred.reset_index()
            # Remove the movies the user is currently watching from the list of movies to predict
            user_pred = user_pred[~user_pred['movie_id'].isin(watched_movies)]
            # Predict user rating for each movie using the predict function
            print("Making predictions...")
            predictions = ranker.predict(user_pred.loc[:,['user_id', 'movie_id']])
            # Add the predictions to the DataFrame
            user_pred['prediction'] = predictions['prediction']
            # Sort the movies by predicted rating
            user_pred.sort_values(by='prediction', ascending=False, inplace=True)
            # Add current time to the prediction
            user_pred["timestamp"] = int(time.time())
            print("Predictions made")
            print(user_pred.head(5))
            user_pred = user_pred.head(10)


            # Use pandas to_sql() function to insert the DataFrame data into the table
            user_pred.to_sql('model_predictions', engine, if_exists='append', index=False)
            # display data is the same as model_predictions but the rows are replaced every time
            # This is to display the predictions on the dashboard
            user_pred.to_sql('display_data', engine, if_exists='replace', index=False)
            # Add the current movies the user is watching to the training data
            # We consider it as a positive rating the fact that the user is watching the movie
            user_curr_watching.to_sql('training_data', engine, if_exists='append', index=False)
            time.sleep(12)

            
except KeyboardInterrupt:
    pass
finally:
    # Ensure the connection is closed
    engine.dispose()
    consumer.close()
