import pandas as pd
from datetime import datetime,timedelta
import sys
sys.path.append('/home/tanle/Documents/Spotify_project/airflow_spotify.py')
from common.base import session,engine,base
import os
import csv
yesterday=datetime.now()-timedelta(days=1)
yesterday=yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
yesterday=yesterday.date()
base_path='data/{}/raw'.format(yesterday)
raw_path=base_path+'/data.json'.format(yesterday)
track_path=base_path+'/Track/raw_track.csv'
album_path=base_path+'/Album/raw_album.csv'
artist_path=base_path+'/Artist/raw_artist.csv'
image_path=base_path+'/Image/raw_image.csv'

#Transform check empty data
def check_if_empty(df:pd.DataFrame)->bool:
    if df.empty:
        print("No song downloaded! Finishing Execution")
        return False
    return True
#Function check primary key of Track
def check_primary_key_track(df:pd.DataFrame)->bool:
    if pd.Series(df['track_id']).is_unique:
        pass
    else:
        return False

#Function check primary key of Album
def check_primary_key_album(df:pd.DataFrame)->bool:
    if pd.Series(df['album_id']).is_unique:
        pass
    else:
        return False
#Function check primary key of Artist
def check_primary_key_artists(df:pd.DataFrame)->bool:
    if pd.Series(df['artist_id']).is_unique:
        pass
    else:
        return False
#Function check primary key of Images
def check_primary_key_images(df:pd.DataFrame)->bool:
    if pd.Series(df['url']).is_unique:
        pass
    else:
        return False

#Function check if check primary key =False then drop dulication of primary key
def drop_duplication_track(df:pd.DataFrame)->pd.DataFrame:
    if check_primary_key_track(df)==False:
        df=df.drop_duplicates(subset=['track_id'])
    return df

def drop_duplication_album(df:pd.DataFrame)->pd.DataFrame:
    if check_primary_key_album(df)==False:
        df=df.drop_duplicates(subset=['album_id'])
    return df

def drop_duplication_artist(df:pd.DataFrame)->pd.DataFrame:
    if check_primary_key_artists(df)==False:
        df=df.drop_duplicates(subset=['artist_id'])
    return df
#Function check primary key of Images_artists
def drop_duplication_artist_track(df:pd.DataFrame)->pd.DataFrame:
    df=df[['track_id', 'artist_id']].value_counts().reset_index(name='count')
    return df[['track_id','artist_id']]
def drop_duplication_image(df:pd.DataFrame)->pd.DataFrame:
    if(check_primary_key_images(df)==False):
        df=df.drop_duplicates(subset='url')
    return df
def truncate_table_track_raw():
    """
    Ensure that track_raw table is alway in empty state before running any transformations.
    """
    session.execute('TRUNCATE TABLE track_raw')
    session.commit()
def truncate_table_album_raw():
    """
    Ensure that album_raw table is alway in empty state before running any transformations.
    """
    session.execute('TRUNCATE TABLE album_raw')
    session.commit()
def truncate_table_artist_raw():
    """
    Ensure that artist_raw table is alway in empty state before running any transformations.
    """
    session.execute('TRUNCATE TABLE artist_raw')
    session.commit()
def truncate_table_image_raw():
    """
    Ensure that image_raw table is alway in empty state before running any transformations.
    """
    session.execute('TRUNCATE TABLE image_raw')
    session.commit()
def truncate_table_artist_track_raw():
    """
    Ensure that artist_track_raw table is alway in empty state before running any transformations.
    """
    session.execute('TRUNCATE TABLE artist_track_raw')
    session.commit()
    
def transform_new_data_track():
    truncate_table_track_raw()
    with open(track_path,mode='r',encoding='utf_8') as csv_file:
        reader=csv.DictReader(csv_file)
        reader_df=pd.DataFrame(reader)
        data=drop_duplication_track(reader_df)
        data.to_sql('track_raw',engine,if_exists='append',index=False)

def transform_new_data_album():
    truncate_table_album_raw()
    with open(album_path,mode='r',encoding='utf_8') as csv_file:
        reader=csv.DictReader(csv_file)
        reader_df=pd.DataFrame(reader)
        data=drop_duplication_album(reader_df)
        data.to_sql('album_raw',engine,if_exists='append',index=False)
def transform_new_data_aritst():
    truncate_table_artist_raw()
    with open(artist_path,mode='r',encoding='utf_8') as csv_file:
        reader=csv.DictReader(csv_file)
        reader_df=pd.DataFrame(reader)
        data=drop_duplication_artist(reader_df)
        data.to_sql('artist_raw',engine,if_exists='append',index=False)
    
def transform_new_data_image():
    truncate_table_image_raw()
    with open(image_path,mode='r',encoding='utf_8') as csv_file:
        reader=csv.DictReader(csv_file)
        reader_df=pd.DataFrame(reader)
        data=drop_duplication_image(reader_df)
        data.to_sql('image_raw',engine,if_exists='append',index=False)