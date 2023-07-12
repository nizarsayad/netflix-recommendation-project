docker run -it \
-v "$(pwd):/home/app" \
-p 4000:4000 \
-e PORT=4000 \
-e POSTGRES_DB_URI=$POSTGRES_DB_URI \
-e POSTER_API=$POSTER_API \
nizarsayad/netflix-streamlit
