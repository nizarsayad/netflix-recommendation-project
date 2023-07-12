import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import requests
import os

### Streamlit config
st.set_page_config(
    page_title="Netflix recommendation",
    page_icon="🎬",
    layout="wide"
)


### Connection to the database
pgdb_uri = os.getenv("POSTGRES_DB_URI")
api_key = os.getenv("POSTER_API")

@st.cache_data
def get_predictions():
    engine = create_engine(pgdb_uri)
    # Query first row of the table sorted by descending order
    movies = pd.read_sql('display_data', engine)
    engine.dispose()
    return movies

@st.cache_data
def get_poster(x):
    try:
        response = requests.get(x).json()
        poster = response['Poster']
    except:
        poster = "No poster available"
    return poster


movies = None

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


### Body
if st.button("Refresh"):
    movies = get_predictions()

if movies is not None:
    movies['poster'] = movies.apply(lambda x: f'http://www.omdbapi.com/?apikey={api_key}&t={x["movie_title"]}', axis=1)
    movies['poster'] = movies['poster'].apply(lambda x: get_poster(x))

    with st.container():
        st.header(f"Welcome to the Netflix recommendation dashboard, user {movies['user_id'][0]}")
        st.subheader("Movies we think you might enjoy:")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.subheader(f"{movies.loc[0, 'movie_title']}")
            try:
                st.image(movies.loc[0, 'poster'])
            except:
                st.write("No poster available")

        with col2:
            st.subheader(f"{movies.loc[1, 'movie_title']}")
            try:
                st.image(movies.loc[1, 'poster'])
            except:
                st.write("No poster available")

        with col3:
            st.subheader(f"{movies.loc[2, 'movie_title']}")
            try:
                st.image(movies.loc[2, 'poster'])
            except:
                st.write("No poster available")

        with col4:
            st.subheader(f"{movies.loc[3, 'movie_title']}")
            try:
                st.image(movies.loc[3, 'poster'])
            except:
                st.write("No poster available")

        with col5:
            st.subheader(f"{movies.loc[4, 'movie_title']}")
            try:
                st.image(movies.loc[4, 'poster'])
            except:
                st.write("No poster available")


