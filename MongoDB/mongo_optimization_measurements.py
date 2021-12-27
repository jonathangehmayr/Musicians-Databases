import pymongo
import datetime
import time
from mongo_queries import get_genre, get_band
from mongo_optimization import get_genre_optimized, get_band_optimized, create_additional_fields, create_indexes


def measure_query_runtime(query, n_runs:int)->float:
    '''
    measuring runtime of query by running it n_runs. for
    every run the connection to the database is renewed
    '''
    list_runtimes=[]
    for i in range(n_runs):
        with pymongo.MongoClient('mongodb://localhost:27017/') as client:
            col = client.musicians.bands
            start=time.time()
            query(col)
            stop=time.time()
        runtime=stop-start
        list_runtimes.append(runtime)
    return sum(list_runtimes)/len(list_runtimes)

def query_bands(col:pymongo.collection.Collection):
    start_90s=datetime.datetime(1989,12,31,0,0,0,0)
    end_90s=datetime.datetime(2000,1,1,0,0,0,0)
    start_10s=datetime.datetime(2009,12,31,0,0,0,0)
    end_10s=datetime.datetime(2020,1,1,0,0,0,0)
    genre=get_genre(col,start_90s, end_90s)
    band_10s=get_band(col,start_10s, end_10s, genre)
 

def query_bands_optimized(col:pymongo.collection.Collection):
    start_90s=datetime.datetime(1989,12,31,0,0,0,0)
    end_90s=datetime.datetime(2000,1,1,0,0,0,0)
    start_10s=datetime.datetime(2009,12,31,0,0,0,0)
    end_10s=datetime.datetime(2020,1,1,0,0,0,0)
    genre=get_genre_optimized(col,start_90s, end_90s)
    band_10s=get_band_optimized(col,start_10s, end_10s, genre)


if __name__ == '__main__':
    
    n_runs=10
    
    #measuring runtime with not optimized query
    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        col = client.musicians.bands
        col.drop_indexes() 
    runtime_not_optimized=measure_query_runtime(query_bands, n_runs)
    print('Runtime not optimized: {}'.format(runtime_not_optimized))
    
    #measuring runtime creating indexes on release_date and genres
    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        col = client.musicians.bands
        create_indexes(col)
    runtime_with_indexes=measure_query_runtime(query_bands, n_runs) 
    print('Runtime with indexes: {}'.format(runtime_with_indexes))
    
    #measuring runtime using optimized queries
    runtime_optimized=measure_query_runtime(query_bands_optimized, n_runs) 
    print('Runtime optimized: {}'.format(runtime_optimized))
    
    
    
 
    
    
    
    