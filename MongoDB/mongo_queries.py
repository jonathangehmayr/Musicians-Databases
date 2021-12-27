import pymongo
from datetime import datetime

def get_genre(col:pymongo.collection.Collection,start_date:datetime, end_date:datetime)->str:
    '''
    Get the most sold album from a timeframe
    '''
    res=col.aggregate([
        {'$unwind': {'path': '$albums'}},
        {'$unwind': {'path': '$genres'}},
        {'$match': {'albums.release_date': {'$gte': start_date,'$lt': end_date}}}, 
        {'$group': {'_id': {'genre': '$genres.genre_name'},'genre_sales': {'$sum': '$albums.sales'}}},
        {'$sort': {'genre_sales': -1}},
        {'$project' : {'genres.genre_name':1} },#'genre':1,'genre_sales':1,
        {'$limit': 1}])
    genre=list(res)[0]['_id']['genre']
    return genre

def get_band(col:pymongo.collection.Collection,start_date:datetime, end_date:datetime, genre:str)->str:
    '''
    Get the the band with the most sales in a specific genre in a timeframe
    '''
    res=col.aggregate([
        {'$unwind': {'path': '$albums'}},
        {'$match': {'albums.release_date': {'$gte': start_date,'$lt': end_date},
                    'genres.genre_name':genre}}, 
        {'$group': {'_id': {'band': '$band_url'},'band_sales': {'$sum': '$albums.sales'}}},
        {'$sort': {'band_sales': -1}},
        {'$project' : {'bands.band_url':1} },
        {'$limit': 1}]) 
    band=list(res)[0]['_id']['band']
    return band

def get_highest_sales(col:pymongo.collection.Collection)->int:
    '''
    Get the overall highest sales
    '''
    res=col.aggregate([
        {'$unwind': {'path': '$albums'}},
        {'$sort': {'albums.sales': -1}},
        {'$project' : {'albums.sales':1} },
        {'$limit': 1}])
    highest_sales=list(res)[0]['albums']['sales']
    return highest_sales


def insert_album(col:pymongo.collection.Collection, band:str, sales:int)->None:
    '''
    Insert a new album for an existing band with higher sales than any other album
    '''
    date=datetime(2019,1,1,0,0,0,0)
    col.update_one(
        {'band_name': band},
        {'$push': 
             {'albums': 
                  {'album_name': 'nices album',
                   'release_date': date,
                   'description': 'not the best',
                   'running_time': 69.3,
                   'sales': sales+1}}})
   
  
#FIXME: deletion of specific albums does not work
def delete_album(col:pymongo.collection.Collection):
    band = col.find_one({'band_name':'The Offspring'})
    id_band=band.get('_id')
    
    deleted=col.update_one( 
            { '_id': id_band}, 
            { '$pull': {'albums': {'$elemMatch': {'album_name':'nices album'}}}}) 
    res=col.find_one({ 'band_name': 'The Offspring'})  
    return res      


if __name__=='__main__':
            
    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        col = client.musicians.bands
        
        #define dates for queries
        start_90s=datetime(1989,12,31,0,0,0,0)
        end_90s=datetime(2000,1,1,0,0,0,0)
        start_10s=datetime(2009,12,31,0,0,0,0)
        end_10s=datetime(2020,1,1,0,0,0,0)
        
        
        #1. Get the most successful band in the 2010s (01.01.2010 - 31.12.2019) in the most successful genre of the 1990s (01.01.1990 - 31.12.1999)                   
        genre=get_genre(col,start_90s, end_90s)
        band_10s=get_band(col,start_10s, end_10s, genre)
        
        #2. Add a new album to the most successful band of most successful genre in the 90s, so that it is more successful than all of the albums of the most successful band in this genre in the 10s 
        band_90s=get_band(col,start_90s, end_90s, genre)
        highest_sales=get_highest_sales(col)
        insert_album(col, band_90s,highest_sales)