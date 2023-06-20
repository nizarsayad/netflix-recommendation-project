# ------ Import necessary libraries ------
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import torch
from tqdm import tqdm
from cf_step.metrics import recall_at_k, precision_at_k
from cf_step.utils import moving_avg
from torch.optim import SGD, Adam
from cf_step.networks import SimpleCF
from cf_step.step import Step
from torch.utils.data import TensorDataset, DataLoader
from cf_step.losses import FlatBCELoss, FlatCrossEntropyLoss, FlatMSELoss

# ------ Data Preparation ------

# Rename columns
df = pd.read_csv("./preprocess/filtered_data.csv", parse_dates=['date'], infer_datetime_format=True)
ratings_df = df.sample(frac=0.1).reset_index(drop=True)
ratings_df.rename(columns={'customer_id': 'user_id', 'date':'timestamp'}, inplace=True)

# Transform users and movies to categorical features
ratings_df['user_id'] = ratings_df['user_id'].astype('category')
ratings_df['movie_id'] = ratings_df['movie_id'].astype('category')

# Use the category codes to avoid creating separate vocabularies
ratings_df['user_code'] = ratings_df['user_id'].cat.codes.astype(int)
ratings_df['movie_code'] = ratings_df['movie_id'].cat.codes.astype(int)

# Determine the number of users and movies
n_users = len(ratings_df['user_code'].unique())
n_movies = len(ratings_df['movie_code'].unique())
print(f"Number of users: {n_users}")
print(f"Number of movies: {n_movies}")

# Sort data by timestamp
data_df = ratings_df.sort_values(by='timestamp')

# Encode ratings as binary preference (more or equal to 4 -> 1, less than 5 -> 0)
data_df['preference'] = np.where(data_df['rating'] >= 4, 1, 0)

# ------ Model Preparation ------

# Confidence function
def conf_func(x: torch.tensor, a: float = 1) -> torch.tensor:
    x[x == 5.] = a * 1.
    x[x == 4.] = a * .5
    x[x == 3.] = a * .01
    x[x == 2.] = a * .5
    x[x == 1.] = a * 1.
    return x.float()

# Initialize model
net = SimpleCF(n_users, n_movies, factors=256, init=torch.nn.init.uniform_, a=0., b=.1, binary=True)
loss = FlatBCELoss()
optimizer = Adam(net.parameters(), lr=6e-2)

# Determine execution device
device = "gpu" if torch.cuda.is_available() \
    else "mps" if getattr(torch,'has_mps',False) else "cpu"

model = Step(net, loss, optimizer, conf_func=conf_func, device=device)

# ------ Training Phase ------

# Prepare data for training
pct = int(data_df.shape[0] * .5)
bootstrapping_data = data_df[:pct]

# Define features and target
features = ['user_code', 'movie_code', 'rating']
target = ['preference']

# Create DataLoader for training
data_set = TensorDataset(torch.tensor(np.int32(bootstrapping_data[features].values)), torch.tensor(np.int32(bootstrapping_data[target].values)))
data_loader = DataLoader(data_set, batch_size=512, shuffle=False)

# Fit the model
model.batch_fit(data_loader)

# ------ Evaluation Phase ------

# Prepare data for evaluation
data_df_step = data_df.drop(bootstrapping_data.index)
data_df_step = data_df_step.reset_index(drop=True)

# Create DataLoader for evaluation
stream_data_set = TensorDataset(torch.tensor(np.int32(data_df_step[features].values)), torch.tensor(np.int32(data_df_step[target].values)))
stream_data_loader = DataLoader(stream_data_set, batch_size=1, shuffle=False)

# Initialize tracking variables
k = 10  # we keep only the# the code was cut off, picking up where we left off

# top 10 recommendations
recalls = []
known_users = []

# Loop through the data and evaluate the model
with tqdm(total=len(stream_data_loader)) as pbar:
    for idx, (features, preferences) in enumerate(stream_data_loader):
        itr = idx + 1
        
        user = features[:, 0]
        item = features[:, 1]
        rtng = features[:, 2]
        pref = preferences

        if user.item() in known_users and rtng.item() >= 4:
            predictions = model.predict(user, k)
            recall = recall_at_k(predictions.tolist(), item.tolist(), k)
            recalls.append(recall)
            model.step(user, item, rtng, pref)
        else:
            model.step(user, item, rtng, pref)
            
        known_users.append(user.item())
        pbar.update(1)


