from common.base import session,base,engine
from common.table import Track_Raw,Album_Raw,Artist_Raw
from common.table import track_relation_album,track_relation_artist,album_relation_image,artist_relation_image
import requests

def truncate_table_track_relation_album():
    session.execute("TRUNCATE TABLE track_album_raw")
    session.commit()

def truncate_table_track_relation_artist():
    session.execute("TRUNCATE TABLE track_artist_raw")
    session.commit()

def truncate_table_album_relation_image():
    session.execute("TRUNCATE TABLE album_image_raw")
    session.commit()

def truncate_table_artist_relation_image():
    session.execute("TRUNCATE TABLE artist_image_raw")
    session.commit()

def save_track_relation_album(token):
    truncate_table_track_relation_album()
    result=session.query(Track_Raw).all()
    temp_list=[]
    headers={
        'Accept':'application/json',
        'Content_Type':'application/json',
        'Authorization':'Bearer {token}'.format(token=token)
    }
    for row in result:
        r=requests.get('https://api.spotify.com/v1/tracks/{id}'.format(id=row.track_id),headers=headers)
        data=r.json()
        temp_list.append(
            track_relation_album(
                track_id=data['id'],
                album_id=data['album']['id']
            )
        )
    session.bulk_save_objects(temp_list)
    session.commit()

def save_track_relation_artist(token):
    truncate_table_track_relation_artist()
    result=session.query(Track_Raw).all()
    temp_list=[]
    headers={
        'Accept':'application/json',
        'Content_Type':'application/json',
        'Authorization':'Bearer {token}'.format(token=token)
    }
    for row in result:
        r=requests.get('https://api.spotify.com/v1/tracks/{id}'.format(id=row.track_id),headers=headers)
        data=r.json()
        for i in range(data['artists'].__len__()):
            temp_list.append(
                track_relation_artist(
                    track_id=data['id'],
                    artist_id=data['artists'][i]['id']
                )
        )
    session.bulk_save_objects(temp_list)
    session.commit()

def save_album_relation_image(token):
    truncate_table_album_relation_image()
    result=session.query(Album_Raw).all()
    temp_list=[]
    headers={
        'Accept':'application/json',
        'Content_Type':'application/json',
        'Authorization':'Bearer {token}'.format(token=token)
    }
    for row in result:
        r=requests.get('https://api.spotify.com/v1/albums/{id}'.format(id=row.album_id),headers=headers)
        data=r.json()
        for i in range(data['images'].__len__()):
            temp_list.append(
                album_relation_image(
                    album_id=data['id'],
                    uri_image=data['images'][i]['url']
                )
        )
    session.bulk_save_objects(temp_list)
    session.commit()

def save_artist_relation_image(token):
    truncate_table_artist_relation_image()
    result=session.query(Artist_Raw).all()
    temp_list=[]
    headers={
    'Accept':'application/json',
    'Content_Type':'application/json',
    'Authorization':'Bearer {token}'.format(token=token)
    }
    for row in result:
        r=requests.get('https://api.spotify.com/v1/artists/{id}'.format(id=row.artist_id),headers=headers)
        data=r.json()
        for i in range(data['images'].__len__()):
            temp_list.append(
                artist_relation_image(
                    artist_id=data['id'],
                    uri_image=data['images'][i]['url']
                )
        )
    session.bulk_save_objects(temp_list)
    session.commit()

    


