![Screenshot](jedhaXnetflix.png)
# 🍿 Netflix-recommendation-engine 🍿
This repository houses all resources related to the project presentation on the Netflix recommendation engine.
<br><br>
Below is a description of the content and purpose of each folder in this repository.<br>
<br>

# ⚙️ preprocess ⚙️ 
This folder contains a script that concatenates data from Kaggle.
The data was extracted from: <br>
https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data.

# 🧠 training 🧠 
This folder contains information related to model training. It includes the following files:
- training_cfstep.py: The Python script responsible for training an incremental collaborative filtering model.
- training_surprise_svd.py: The Python script responsible for training the famous funk SVD algorithm

# 📡 kafka_streaming 📡 
This is the folder containing the Kafka streaming process. It includes the following files:
- ccloud.lib & python.config: Configuration files for the Kafka producer and consumer, enabling communication through Confluent, the fully managed, cloud-native event streaming platform powered by Apache Kafka.
- consumer.py: A Python script that receives the Netflix API's data, produced by producer.py every 12 seconds.
- Dockerfile: A personalized Docker image that installs necessary dependencies on top of the Kafka-based image provided by Jedha. This corresponds to the build of the Docker image used during the presentation.
- producer.py: A Python script that calls the model's inference API, gets the predictions and feeds the data in a postgres database.
- requirements.txt: A text file that enables the installation of all required Python packages to run the python scripts.
- run.sh: shell command to launch the Docker container that allows to run the kafka scripts.

# 🚀 api 🚀
This API hosts the recommendation model.
The contents of the folder are as follows:
- app.py: The FastAPI Python script.
- Dockerfile: Builds the Docker image necessary to run the API on a remote server.
- requirements.txt: A text file that enables the installation of all required Python packages to run the API script.

# 🚀 webapp 🚀
This is a webapp created to showcase the results of the predictions.<br>
The contents of the folder are as follows:
- streamlit.py: Python script of the streamlit app. It queries a SQL Database and retrieves the results of the recommendations. It also requests posters from an API to display them along the results. It auto-refreshes every 30 seconds.
- Dockerfile: Builds the Docker image necessary to run the webapp on a remote server or locally.
- .streamlit: Streamlit configuration folder.
- run.sh: Shell command used to run the app.

