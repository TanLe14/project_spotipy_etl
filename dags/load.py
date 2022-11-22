from common.base import session,base
from common.table import *
from sqlalchemy.dialects.postgresql import insert

def insert_track():
    clean_track_id=session.query(Track.track_id)
    track_to_insert=session.query(
        Track_Raw.track_id,
        Track_Raw.track_name,
        Track_Raw.disc_number,
        Track_Raw.duration_ms,
        Track_Raw.popularity,
        Track_Raw.preview_url,
        Track_Raw.uri,
        track_relation_album.album_id
    ).filter(Track_Raw.track_id==track_relation_album.track_id,~Track_Raw.track_id.in_(clean_track_id))
    add_track=insert(Track).from_select(
        ['track_id','track_name','disc_number','duration_ms','popularity','preview_url','uri','album_id'],
        track_to_insert
    )
    session.execute(add_track)
    session.commit()
def insert_image():
    clean_url_image=session.query(Image.url)
    image_to_insert=session.query(
        Image_Raw.url,
        Image_Raw.height,
        Image_Raw.width
    ).filter(~Image_Raw.url.in_(clean_url_image))
    add_image=insert(Image).from_select(
        ['url','height','width'],
        image_to_insert
    )
    session.execute(add_image)
    session.commit()
def insert_album():
    clean_album_id=session.query(Album.album_id)
    album_to_insert=session.query(
        Album_Raw.album_id,
        Album_Raw.album_name,
        Album_Raw.album_type,
        Album_Raw.total_track,
        Album_Raw.release_date,
        Album_Raw.url
    ).filter(~Album_Raw.album_id.in_(clean_album_id))
    add_album=insert(Album).from_select(
        ['album_id','album_name','album_type','total_track','release_date','url'],
        album_to_insert
    )
    session.execute(add_album)
    session.commit()
def insert_artist():
    clean_artist_id=session.query(Artist.artist_id)
    artist_to_insert=session.query(
        Artist_Raw.artist_id,
        Artist_Raw.artist_name,
        Artist_Raw.followers_total,
        Artist_Raw.genres,
        Artist_Raw.popularity,
        Artist_Raw.uri
    ).filter(~Artist_Raw.artist_id.in_(clean_artist_id))
    add_artist=insert(Artist).from_select(
        ['artist_id','artist_name','followers_total','genres','popularity','uri'],
        artist_to_insert
    )
    session.execute(add_artist)
    session.commit()
def insert_artist_image():
    clean_artist_image=session.query(Artist_Image.artist_id,Artist_Image.uri_image)
    artist_image_to_insert=session.query(
        artist_relation_image.artist_id,
        artist_relation_image.uri_image
    ).filter(~clean_artist_image.exists())
    add_artist_image=insert(Artist_Image).from_select(
        ['artist_id','uri_image'],
        artist_image_to_insert
    )
    session.execute(add_artist_image)
    session.commit()
def insert_album_image():
    clean_album_image=session.query(Album_Image.album_id,Album_Image.uri_image)
    album_image_to_insert=session.query(
        album_relation_image.album_id,
        album_relation_image.uri_image
    ).filter(~clean_album_image.exists())
    add_album_image=insert(Album_Image).from_select(
        ['album_id','uri_image'],
        album_image_to_insert
    )
    session.execute(add_album_image)
    session.commit()
def insert_track_artist():
    clean_track_artist=session.query(Artist_Track.artist_id,Artist_Track.track_id)
    track_artist_to_insert=session.query(
        track_relation_artist.artist_id,
        track_relation_artist.track_id
    ).filter(~clean_track_artist.exists())
    add_track_artist=insert(Artist_Track).from_select(
        ['artist_id','track_id'],
        track_artist_to_insert
    )
    session.execute(add_track_artist)
    session.commit()
    
