from sqlalchemy import create_engine, Column,Integer,String,ForeignKey,Date,ForeignKeyConstraint,PrimaryKeyConstraint
from common.base import base

class Track(base):
    __tablename__="track"
    track_id=Column(String,primary_key=True)
    track_name=Column(String)
    disc_number=Column(Integer)
    duration_ms=Column(Integer)
    popularity=Column(Integer)
    preview_url=Column(String)
    uri=Column(String)
    album_id=Column(String,ForeignKey("album.album_id"))

class Album(base):
    __tablename__="album"
    album_id=Column(String,primary_key=True)
    album_name=Column(String)
    album_type=Column(String)
    total_track=Column(Integer)
    release_date=Column(Date)
    url=Column(String)

class Image(base):
    __tablename__="image"
    url=Column(String,primary_key=True)
    height=Column(Integer)
    width=Column(Integer)

class Artist(base):
    __tablename__="artist"
    artist_id=Column(String,primary_key=True)
    artist_name=Column(String)
    followers_total=Column(Integer)
    genres=Column(String)
    popularity=Column(Integer)
    uri=Column(String)

class Artist_Image(base):
    __tablename__='artist_image'
    artist_id=Column(String,ForeignKey('artist.artist_id'),primary_key=True)
    uri_image=Column(String,ForeignKey('image.url'),primary_key=True)
class Album_Image(base):
    __tablename__='album_image'
    album_id=Column(String,ForeignKey('album.album_id'),primary_key=True)
    uri_image=Column(String,ForeignKey('image.url'),primary_key=True)

class Artist_Track(base):
    __tablename__="artist_track"
    track_id=Column(String,ForeignKey('track.track_id'),primary_key=True)
    artist_id=Column(String,ForeignKey('artist.artist_id'),primary_key=True)


class Track_Raw(base):
    __tablename__="track_raw"
    track_id=Column(String,primary_key=True)
    track_name=Column(String)
    disc_number=Column(Integer)
    duration_ms=Column(Integer)
    popularity=Column(Integer)
    preview_url=Column(String)
    uri=Column(String)


class Album_Raw(base):
    __tablename__="album_raw"
    album_id=Column(String,primary_key=True)
    album_name=Column(String)
    album_type=Column(String)
    total_track=Column(Integer)
    release_date=Column(Date)
    url=Column(String)

class Image_Raw(base):
    __tablename__="image_raw"
    url=Column(String,primary_key=True)
    height=Column(Integer)
    width=Column(Integer)

class Artist_Raw(base):
    __tablename__="artist_raw"
    artist_id=Column(String,primary_key=True)
    artist_name=Column(String)
    followers_total=Column(Integer)
    genres=Column(String)
    popularity=Column(Integer)
    uri=Column(String)



class track_relation_album(base):
    __tablename__="track_album_raw"
    track_id=Column(String,primary_key=True)
    album_id=Column(String,primary_key=True)

class album_relation_image(base):
    __tablename__="album_image_raw"
    album_id=Column(String,primary_key=True)
    uri_image=Column(String,primary_key=True)

class artist_relation_image(base):
    __tablename__="artist_image_raw"
    artist_id=Column(String,primary_key=True)
    uri_image=Column(String,primary_key=True)

class track_relation_artist(base):
    __tablename__="track_artist_raw"
    track_id=Column(String,primary_key=True)
    artist_id=Column(String,primary_key=True)


