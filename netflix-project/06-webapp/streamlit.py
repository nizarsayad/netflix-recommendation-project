import json
import time
import pandas as pd
import streamlit as st
import psycopg2
from sqlalchemy import create_engine
import requests
import os
from streamlit_autorefresh import st_autorefresh

### Streamlit config
st.set_page_config(
    page_title="Netflix recommendation",
    page_icon="🎬",
    layout="wide"
)


### Connection to the database
pgdb_uri = os.getenv("POSTGRES_DB_URI")
api_key = os.getenv("POSTER_API")
engine = create_engine(pgdb_uri)
# Query first row of the table sorted by descending order
df = pd.read_sql('SELECT * FROM top_movies ORDER BY request_time DESC LIMIT 1', engine)
# Get posters
movie_posters = []
for i in range(5):
    title = df.iloc[0,i+1]
    url = f'http://www.omdbapi.com/?apikey={api_key}&t={title}'
    response = requests.get(url).json()
    try:
        movie_posters.append(response['Poster'])
    except:
        continue

count = st_autorefresh(interval=3000, limit=100, key="fizzbuzzcounter")
### App streamlit
### Header 
header_left, title = st.columns([3,6])

with header_left:
    st.image("pngwing.com.png")

with title:
    st.title("Netflix recommendation 🎬")



st.markdown("""
        This dashboard is made to showcase the movies recommendation depending on which movies a user has seen previously.
    """)

st.header(f"Welcome to the Netflix recommendation dashboard, {df['customerid'][0]}")

st.subheader("Movies we think you might enjoy:")

### Body
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.subheader(f"{df['top1'][0]}")
    try:
        st.image(movie_posters[0])
    except:
        st.text("No poster available")

with col2:
    st.subheader(f"{df['top2'][0]}")
    try:
        st.image(movie_posters[1])
    except:
        st.text("No poster available")

with col3:
    st.subheader(f"{df['top3'][0]}")
    try:
        st.image(movie_posters[2])
    except:
        st.text("No poster available")
with col4:
    st.subheader(f"{df['top4'][0]}")
    try:
        st.image(movie_posters[3])
    except:
        st.text("No poster available")
with col5:
    st.subheader(f"{df['top5'][0]}")
    try:
        st.image(movie_posters[4])
    except:
        st.text("No poster available")

