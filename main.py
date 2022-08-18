# ###
# Collecting data from Spotify API to see recenetly played tracks and store / handle this data.

# This is a learning project idea from Karolina Sowinska on YouTube - https://www.youtube.com/c/KarolinaSowinska.

# This helps me understand how to use APIs, handle data, store data, understand dataframes, learn new libraries.

# It also helps me understand Python at a deeper level.
# ###

import sqlalchemy
import pandas as pd
import requests
import datetime
import sqlite3

# Info
DB_LOCATION = 'sqlite:///played_tracks.sqlite'

# Provide own info (personal info has been removed for security reasons) - https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=
USER_ID = 'xxxxxxxxxxx'
TOKEN = 'xxxxxxxxxxx'

def check_if_data_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs have been downloaded.")
        return False

    # Check Primary Key (unique ID, played_at as 'impossible' to play two songs at the same time - means duplicate data)
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key is violated.")

    # Check for null data
    if df.isnull().values.any():
        raise Exception("Null values found.")

    # # Check that timestamps are for yesterdays day
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #         raise Exception("At least one of the returned dates were not from the last 24 hours.")

    return True



if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=60)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix), headers=headers)

    song_data = r.json()

    song_names = []
    artist_names = []
    played_time = []
    timestamps = []

    for song in song_data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_time.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_time,
        "timestamp" : timestamps
    }

    # create dataframe with pandas
    song_df = pd.DataFrame(song_dict, columns= ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)


    # Validate 
    if check_if_data_valid(song_df):
        print("Data valid, proceding to load stage.")

    
    # Loading

    # Relational DB (MySQL, SQLite, etc) - Stored in tables & rows. They are built on realtions. - more reliable for larger data and complex queries
    # Non-Relational DB (MongoDB, DynamoDB) - Stored in JSON docs - more flexible. You can change once everything is defined (tables, schema etc.)

    engine = sqlalchemy.create_engine(DB_LOCATION)
    con = sqlite3.connect('played_tracks.sqlite')
    cursor = con.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database succesfully")

    try:
        song_df.to_sql("played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in this database")

    con.close()
    print("Database closed succesfully")