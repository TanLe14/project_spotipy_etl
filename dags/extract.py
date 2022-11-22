from airflow import DAG
from datetime import datetime,timedelta
from airflow.decorators import dag, task
import requests
from airflow.models.variable import Variable
import os
import json
import csv
from transform import transform_new_data_album,transform_new_data_track,transform_new_data_aritst,transform_new_data_image
from common.base import base,engine, session
from add_foreign import save_track_relation_album,save_track_relation_artist,save_album_relation_image,save_artist_relation_image
from load import insert_track,insert_album,insert_image,insert_artist,insert_album_image,insert_artist_image,insert_track_artist

yesterday=datetime.now()-timedelta(days=1)
yesterday=yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
after=int(datetime.timestamp(yesterday))
yesterday=yesterday.date()
base_path='data/{}/raw'.format(yesterday)
raw_path=base_path+'/data.json'.format(yesterday)
track_path=base_path+'/Track/raw_track.csv'
album_path=base_path+'/Album/raw_album.csv'
artist_path=base_path+'/Artist/raw_artist.csv'
image_path=base_path+'/Image/raw_image.csv'

default_args={
    'owner':'tanle',
    'email':['lexuantan2000@gmail.com'],
    'email_on_failure':True,
    'email_on retry':True,
    'retries':2,
    'retry_delay':timedelta(minutes=2)
}
def create_forlder_if_not_exists(path):
    os.makedirs(os.path.dirname(path),exist_ok=True)
@dag(
    dag_id='etl_process',
    default_args=default_args,
    description='This is dag implement elt processing',
    start_date=datetime(2022,11,10),
    schedule_interval='00 10 * * 1-6'
)
def etl_processing():
#Extract data from api, after save it into folder
    @task
    def refresh_token():
        url='https://accounts.spotify.com/api/token'
        data={
            'grant_type':'refresh_token',
            'refresh_token':Variable.get('refresh_token'),
            'client_id':Variable.get('client_id'),
            'client_secret': Variable.get('client_secret')
           }
        response=requests.post(url=url,data=data)
        response=response.json()
        return response['access_token']
    @task
    def get_recently_track(token):
        create_forlder_if_not_exists(raw_path)
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=token)
        }
        r=requests.get("https://api.spotify.com/v1/me/player/recently-played?limit={limit}&after={after}".format(limit=5,after=after),headers=headers)
        data=r.json()
        with open(raw_path, mode='w',encoding='utf_8')as file_data:
            json.dump(data,file_data)
    @task
        #Function save data raw of track in file csv
    def save_new_raw_data_track():
        with open(raw_path,mode='r',encoding='utf_8') as file_data:
            response=file_data.read()
            response=json.loads(response)
            create_forlder_if_not_exists(track_path)
            with open(track_path,mode='w',encoding='utf_8') as csv_writer:
                fieldnames=['track_id','track_name','disc_number','duration_ms','popularity','preview_url','uri']
                writer=csv.DictWriter(csv_writer,fieldnames=fieldnames)
                writer.writeheader()
                dic_track={}
                for song in response['items']:
                    dic_track['track_id']=(song['track']['id'])
                    dic_track['track_name']=(song['track']['name'])
                    dic_track['disc_number']=(song['track']['disc_number'])
                    dic_track['duration_ms']=(song['track']['duration_ms'])
                    dic_track['popularity']=(song['track']['popularity'])
                    dic_track['preview_url']=(song['track']['preview_url'])
                    dic_track['uri']=(song['track']['external_urls']['spotify'])
                    writer.writerow(dic_track)
    @task
    def save_new_raw_data_album():
        with open(raw_path,mode='r',encoding='utf_8')as file_data:
            response=file_data.read()
            response=json.loads(response)
            create_forlder_if_not_exists(album_path)
            with open(album_path,mode='w',encoding='utf_8') as csv_writer:
                fieldnames=['album_id','album_name','album_type','total_track','release_date','url']
                writer=csv.DictWriter(csv_writer,fieldnames=fieldnames)
                writer.writeheader()
                dic_album={}
                for song in response['items']:
                    dic_album['album_type']=song['track']['album']['album_type']
                    dic_album['total_track']=song['track']['album']['total_tracks']
                    dic_album['album_id']=song['track']['album']['id']
                    dic_album['album_name']=song['track']['album']['name']
                    dic_album['release_date']=song['track']['album']['release_date']
                    dic_album['url']=song['track']['album']['external_urls']['spotify']
                    writer.writerow(dic_album)
    @task
    def save_new_raw_data_artist(token):
        with open(raw_path,mode='r',encoding='utf_8') as file_data:
            response=file_data.read()
            response=json.loads(response)
            create_forlder_if_not_exists(artist_path)
            with open(artist_path,mode='w',encoding='utf_8') as csv_writer:
                fieldnames=['artist_id','artist_name','followers_total','genres','popularity','uri']
                writer=csv.DictWriter(csv_writer,fieldnames=fieldnames)
                writer.writeheader()
                dic_artist={}
                headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer {token}".format(token=token)
                } 
                for song in response['items']:
                    for i in range(song['track']['artists'].__len__()):
                        r=requests.get("https://api.spotify.com/v1/artists/{id}".format(id=song['track']['artists'][i]['id']),headers=headers)
                        data=r.json()
                        dic_artist['artist_id']=(data['id'])
                        dic_artist['artist_name']=(data['name'])
                        dic_artist['followers_total']=(data['followers']['total'])
                        dic_artist['genres']=(data['genres'])
                        dic_artist['popularity']=(data['popularity'])
                        dic_artist['uri']=(data['external_urls']['spotify'])
                        writer.writerow(dic_artist)
    @task 
    def save_new_raw_data_image(token):
        with open(raw_path,mode='r',encoding='utf_8') as file_data:
            response=file_data.read()
            response=json.loads(response)
            create_forlder_if_not_exists(image_path)
            with open(image_path,mode='w',encoding='utf_8') as csv_writer:
                fieldnames=['url','height','width']
                writer=csv.DictWriter(csv_writer,fieldnames=fieldnames)
                writer.writeheader()
                dic_image={}
                headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer {token}".format(token=token)
                } 
                for song in response['items']:
                    for i in range(song['track']['artists'].__len__()):
                        r=requests.get("https://api.spotify.com/v1/artists/{id}".format(id=song['track']['artists'][i]['id']),headers=headers)
                        data=r.json()
                        for j in range(data['images'].__len__()):
                            dic_image['url']=(data['images'][j]['url'])
                            dic_image['height']=(data['images'][j]['height'])
                            dic_image['width']=(data['images'][j]['width'])
                            writer.writerow(dic_image)
                    r=requests.get("https://api.spotify.com/v1/albums/{id}".format(id=song['track']['album']['id']),headers=headers)
                    data_image_album=r.json()
                    for i in range(data_image_album['images'].__len__()):
                        dic_image['url']=(data_image_album['images'][i]['url']) 
                        dic_image['height']=(data_image_album['images'][i]['height']) 
                        dic_image['width']=(data_image_album['images'][i]['width']) 
                        writer.writerow(dic_image)   

#Transform check data save it into table temp in database postgres.
    @task
    #Create table temp save data raw
    def create_table():
        base.metadata.create_all(bind=engine)
        
    @task
    #Function run function clean data
    def run():
        transform_new_data_track()
        transform_new_data_album()
        transform_new_data_aritst()
        transform_new_data_image()
#Function save data -> add foreign key database.
    @task
    def save_data_for_foreign_key(token):
        save_track_relation_album(token)
        save_track_relation_artist(token)
        save_album_relation_image(token)
        save_artist_relation_image(token)
#Load data from table temp in postgres database into main database .
    @task
    def save_clean_data_into_database():
        insert_image()
        insert_album()
        insert_artist()
        insert_track()
        insert_track_artist()
        insert_album_image()
        insert_artist_image()

    token=refresh_token()
    get_recently_track(token=token) >> [save_new_raw_data_track(),save_new_raw_data_album(),save_new_raw_data_artist(token=token),save_new_raw_data_image(token=token)] >>create_table() >> run()>>save_data_for_foreign_key(token)>>save_clean_data_into_database()


etl=etl_processing()
