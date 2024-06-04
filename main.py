import os

import spotipy
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util
import cred

import pandas as pd
import sqlalchemy
import sqlite3

# EXTRACT

def access_api(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI): 
    scope = "user-read-recently-played", "user-library-read"

    sp_oauth = SpotifyOAuth(client_id=cred.CLIENT_ID, 
                            client_secret=cred.CLIENT_SECRET, 
                            redirect_uri=cred.REDIRECT_URI,
                            scope=scope)

    sp = Spotify(auth_manager=sp_oauth)

    try: 
        token = util.prompt_for_user_token(client_id=cred.CLIENT_ID, client_secret=cred.CLIENT_SECRET, 
                                    redirect_uri=cred.REDIRECT_URI, scope=scope)
    except: os.remove(f".cache-{cred.CLIENT_ID}")

    sp = spotipy.Spotify(auth=token)

    return sp

def fetch_recent_songs_info(sp): 

    # fetch recent songs info from Spotify Web API 
    recent_pull = sp.current_user_recently_played()

    artist_name = [i['track']['artists'][0]['name'] for i in recent_pull['items']]
    track_name = [i['track']['name'] for i in recent_pull['items']]
    album_name = [i['track']['album']['name'] for i in recent_pull['items']]
    played_at = [i['played_at'] for i in recent_pull['items']]

    # create dictionary to store data in Pandas tabular format
    recent_songs = {
        "artist_name": artist_name, 
        "track_name": track_name, 
        "album_name": album_name, 
        "played_at": played_at
    }

    songs_df = pd.DataFrame(recent_songs)

    return songs_df


# TRANSFORM

def data_validation(df): 

    # check if data set is empty 
   if df.empty: 
       print('No songs loaded. Finishing execution')
       return False
   
   # check if primary key is unique 
   if pd.Series(df['played_at']).is_unique: 
       pass
   else: 
       raise Exception("Primary Key check is violoated")
   
   # check for null values 
   if df.isnull().values.any(): 
       raise Exception("Data contains null values")
   
   # if all checks pass, return True 
   return True

 # LOAD

def create_sql_connection(df):
    engine = sqlalchemy.create_engine(cred.DATABASE_LOCATION)
    connection = sqlite3.connect("my_recent_tracks.sqlite")
    cursor = connection.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_recent_tracks(
        TRACK_NAME VARCHAR(20),
        ARTIST_NAME VARCHAR(20),
        ALBUM_NAME VARCHAR(20), 
        PLAYED_AT VARCHAR(50) PRIMARY KEY
    )
    """

    cursor.execute(sql_query)
    print("Database opened successfully")

    try:

        df.to_sql("my_recent_tracks", engine, index=False, if_exists='append')
    except: 
        ("Data already exists in database")

    connection.close()
    print("Close database")
      

if __name__ == '__main__': 
   
   track_df = fetch_recent_songs_info(access_api(cred.CLIENT_ID, 
                                        cred.CLIENT_SECRET, 
                                        cred.REDIRECT_URI))

   if data_validation(track_df):
          print("Data validation passed")
          
          create_sql_connection(track_df)

    
   
    




       












