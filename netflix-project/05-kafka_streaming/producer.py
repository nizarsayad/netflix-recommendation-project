from confluent_kafka import Producer
import requests
import pandas as pd
import ccloud_lib # Library not installed with pip but imported from ccloud_lib.py
import time
import datetime

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "netflix_recommendation"

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
producer = Producer(producer_conf)

# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)


delivered_records = 0
def movie_api_get():
    """This function sends a get request to the API.
    Processes the response from the API to a pandas dataframe.
    Returns the dataframe as a dictionary."""

    url = 'https://jedha-netflix-real-time-api.herokuapp.com/users-currently-watching-movie'
    response = requests.get(url).json()
    df=pd.read_json(response, orient='split').reset_index().rename(columns={'index':'Movie_Id'})
    return df

# Callback called acked (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    # Delivery report handler called on successful or failed delivery of message
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

try:
    while True:
        data = movie_api_get()
        user_id = data["customerID"][0]
        current_time = data['current_time'][0]
        print(f"Producing record - time: {current_time}\tuserID: {user_id}")
        record_value = data.to_json()
        # This will actually send data to your topic
        producer.produce(
            TOPIC,
            key=f"netflix_{user_id}",
            value=record_value,
            on_delivery=acked
        )
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls thanks to acked callback
        producer.poll(0)
        time.sleep(12)
except KeyboardInterrupt:
    pass
finally:
    producer.flush() # Finish producing the latest event before stopping the whole script