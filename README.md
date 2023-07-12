![Screenshot](jedhaXnetflix.png)
# 🍿 Netflix-recommendation-engine 🍿
This repository houses all resources related to the project presentation on the Netflix recommendation engine.
<br><br>
Below is a description of the content and purpose of each folder in this repository.<br>
<br>

# 💾 data 💾
This is the data source folder. For convenience, and to be able to push the repository to github, only the movie_titles file is displayed.
A link to all the data on my google drive:
https://drive.google.com/drive/folders/1saDjScbiCCmy6Ho2ilo8rstZEX5i3iNJ?usp=drive_link
This data can be accessed as well on:
https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data

# ⚙️ preprocess ⚙️ 
This folder contains multiple scripts that process the data from Kaggle.
- The *preprocess.py* script is designed to transform the Netflix Prize dataset into a more manageable and useful format for further analysis or machine learning tasks. The script is memory-efficient as it processes the data in chunks, making it suitable for large datasets.
- The *filter.py* script reads the processed data from "processed_data.csv". It filters out movies and customers based on the 70th percentile of the count of ratings for movies and customers. The data is then renamed and sorted by date, and the sorted data is saved to "filtered_data_sorted.csv".
- The *get_sample.py* filters a DataFrame by year and month. It saves the filtered data to a CSV file named    
"sample_data_{suffix}.csv", where "suffix" is based on the filter parameters.
- The *upload.py* script connects to the project's S3 bucket and uploads the specified file.

# 🚀 mlflow 🚀
This folder contains the Dockerfile, requirements.txt as well as the necessary bash command to run the mlflow tracking server. The env variables are sourced from a secrets.sh.
In order to run your own tracking server, you need to provide the necessary credentials as shown in the run.sh file.

# 🧠 training 🧠 
This folder contains a notebook for model training.
Using mlflow, we trained a matrix factorization model from the recommendation specialized library **surprise lib**. Then using mlflow Model Registry, we have registered our model, in order to use it to make predictions.
This allows to implement later on an automated training cycle, in which we train the model on new aquired data (from the netflix API), retrain the model the same way and use the latest version of the model.
In order to reduce the training time, we've chosen only the data from the last three months of 2005. We've applied further filtering (from the data, only the 85th percentile is kept).
In a real life scenario, we would not apply further filtering since it has alreay been done. We would enlarge our timeframe as well, to get more data (e.g: all ratings from 2005)

# 📡 kafka_streaming 📡 
This is the folder containing the Kafka streaming process. It includes the following files:
- ccloud.lib & python.config: Configuration files for the Kafka producer and consumer, enabling communication through Confluent, the fully managed, cloud-native event streaming platform powered by Apache Kafka.
- Dockerfile: A personalized Docker image that installs necessary dependencies on top of the Kafka-based image provided by Jedha. This corresponds to the build of the Docker image used during the presentation.
- producer.py: A Python script that calls the model's inference API, and produces messages in a TOPIC to be later consumed by the consumer.
- consumer.py: A Python script that receives the Netflix API's data, produced by producer.py every 12 seconds and calls the model (from the mlflow registry), makes the predictions and sends the data to a Postgres database.
- requirements.txt: A text file that enables the installation of all required Python packages to run the python scripts.
- run.sh: shell command to launch the Docker container that allows to run the kafka scripts.
- _python.config: this file is the same as the python.config, however I removed the confluent credentials for security reasons.

# 🚀 webapp 🚀
This is a webapp created to showcase the results of the predictions.<br>
The contents of the folder are as follows:
- streamlit.py: Python script of the streamlit app. It queries a SQL Database and retrieves the results of the recommendations. It also requests posters from an API to display them along the results on the click of a button.
- Dockerfile: Builds the Docker image necessary to run the webapp on a remote server or locally.
- .streamlit: Streamlit configuration folder.
- run.sh: Shell command used to run the app.