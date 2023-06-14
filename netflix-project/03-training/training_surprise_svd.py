import pandas as pd
import numpy as np
from surprise import Reader, Dataset, SVD, accuracy, dump
from surprise.model_selection import GridSearchCV, train_test_split
import time



# ------ Data Preparation ------
# Start timer
print("Starting training...")
start_time = time.time()
df = pd.read_csv("../preprocess/filtered_data.csv", parse_dates=['date'], infer_datetime_format=True)
ratings_df = df.sample(frac=0.1).reset_index(drop=True)
del df
reader = Reader()
ratings_df = Dataset.load_from_df(ratings_df[['customer_id', 'movie_id', 'rating']], reader)


param_grid = {
    "n_epochs": [20,50,100], 
    "n_factors": [100, 200, 300]
    }


gs = GridSearchCV(SVD, param_grid, measures=["rmse", "mae"], cv=5)
gs.fit(ratings_df)

# Train the model with the best parameters
best_params_rmse = gs.best_params["rmse"]

model = SVD(**best_params_rmse)
trainset, testset = train_test_split(ratings_df, test_size=.25)
del ratings_df

model.fit(trainset)

# End timer
end_time = time.time()
print("Training completed.")
print(f"Total time: {end_time - start_time} seconds")

# Evaluate the model
print("Evaluating the model...")
predictions = model.test(testset)
print("RMSE: ", accuracy.rmse(predictions))
print("MAE: ", accuracy.mae(predictions))





# Save the model as a file
file_name = "./model_svd_surprise_v0.pkl"
dump.dump(file_name, model)


    


