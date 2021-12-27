import pymongo
from datetime import datetime
from datetime import timezone

pipeline_90s_sales = [
    {'$unwind': {'path': '$albums'}}, 
    {'$match': {'albums.release_date': {
                '$gte': datetime(1990, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                '$lt': datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}}}, 
    {'$group': {'_id': '$_id','band_sales': {'$sum': '$albums.sales'}}}]

pipeline_10s_sales = [
    {'$unwind': {'path': '$albums'}}, 
    {'$match': {'albums.release_date': {
                '$gte': datetime(2009, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
                '$lt': datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}}}, 
    {'$group': {'_id': '$_id','band_sales': {'$sum': '$albums.sales'}}}]

  
def add_field(col:pymongo.collection.Collection,pipeline:list,new_field:str):
    '''
    add additional field with sales of a band in a time area for optimizing predefined queries
    '''
    cur = col.aggregate(pipeline)
    len_cur=len(list(cur))
    cur = col.aggregate(pipeline)
    i=1
    for doc in cur:
        col.update_one({"_id": doc['_id']}, {'$set': {new_field: doc['band_sales']}})
        print ('Inserted {} of {} fields in database'.format(i,len_cur), end="\r")
        i+=1

def get_genre_optimized(col:pymongo.collection.Collection,start_date:datetime, end_date:datetime)->str:
    '''
    optimized query for returning the most successful genre in the 90s
    '''
    res=col.aggregate([
        {'$unwind': {'path': '$genres'}}, 
        {'$group': {'_id': {'genre': '$genres.genre_name'},'genre_sales': {'$sum': '$band_sales_90s'}}},
        {'$sort': {'genre_sales': -1}},
        {'$project' : {'genres.genre_name':1} },#'genre':1,'genre_sales':1,
        {'$limit': 1}])
    genre=list(res)[0]['_id']['genre']
    return genre

    
def get_band_optimized(col:pymongo.collection.Collection,start_date:datetime, end_date:datetime, genre:str)->str:
    '''
    optimized query for finding the most successful band in the 2010s in the mos successful genre in the 90s
    '''
    res=col.aggregate([
        {'$unwind': {'path': '$albums'}},
        {'$match': {'genres.genre_name':genre}}, 
        {'$group': {'_id': {'band': '$band_name'},'band_sales': {'$sum': '$band_sales_10s'}}},
        {'$sort': {'band_sales': -1}},
        {'$project' : {'bands.band_name':1} },
        {'$limit': 1}]) 
    band=list(res)[0]['_id']['band']
    return band 

def create_indexes(col:pymongo.collection.Collection)->None:
    '''
    Creation of indexes on the fields albums.release_date and genres
    '''
    col.create_index([('albums.release_date', pymongo.ASCENDING)], name='index_release_date')
    col.create_index([('genres', pymongo.ASCENDING)], name='index_genres')


def create_additional_fields(col:pymongo.collection.Collection)->None:
    '''
    Add fields that hold already summed sales in 1990s and 2010s for respective documents
    '''
    add_field(col,pipeline_90s_sales, 'band_sales_90s')
    add_field(col,pipeline_10s_sales, 'band_sales_10s')
    
if __name__=='__main__':
    #adding the new fields to the collection
    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        col = client.musicians.bands
        create_indexes(col)
        create_additional_fields(col)