import sys
import pymongo
import datetime
from typing import List
import os
os.chdir('..')
from Postgres.postgres_data_insertion import load_music_data_to_df,try_parse,convert_df_to_list_of_tuples
os.chdir('./MongoDB')

def create_json_documents()->List:
    '''
    Filling of a list (list_docs) with json documnets regarding to the following structure
    
    document={
        'band_url': 'band_url',
        'band_name': 'band_name',
        'genres':[
            {
                'genre_name':'genre'
            },
                 ...   
        ],
        'members':[
            {
                'member_url':'url',
                'member_name':'name',
                'active':'True'
            },
                  ...  
        ],       
        'albums':[                         
            {
               'album_name':'name',
               'release_date':'date',
               'description': 'desc',
               'running_time': 'time',
               'sales':'sales_amount',
            },
                ...
        ]
    }
                       
    '''  
    
    list_docs=[]
    i=1
    for data_list in gen_data_list():
        band_url,band_name,list_genres,list_musicians,list_albums=data_list
    
        genres=[{'genre_name':'{}'.format(genre)} for genre in list_genres]
        
        musician_keys=['member_url','member_name', 'active']
        musicians=[dict(zip(musician_keys,list(musician))) for musician in list_musicians]
        
        album_keys=['album_name','release_date','description','running_time','sales']
        albums=[dict(zip(album_keys,list(album))) for album in list_albums]
        
        schema  = {'band_url': '{}'.format(band_url),
             'band_name': '{}'.format(band_name),
             'genres':genres,
             'members':musicians,
             'albums':albums
             }
        list_docs.append(schema)
        print ('Inserted {} of 10000 documents in database'.format(i), end="\r")
        
        i=i+1
    return list_docs


def gen_data_list()->List:
    '''
    Generator function extracting data from dataframes 
    as list that is intended for later conversion to json format.
    The structure of the list is following:
        
    l=[
       band_url:str,
       band_name:str,
       list_genres:List,
       list_musicians:List,
       list_albums:List        
    ]
    '''
    #loading of csv data into dataframes
    df_album,df_band_name,df_musicians,df_genre=load_music_data_to_df()
    
    for idx,band_url in enumerate(df_band_name[0]): #.iloc[:20]
        
        band_name=df_band_name.loc[df_band_name[0] == band_url][1].reset_index(drop=True)[0]
            
        list_genres=df_genre.loc[df_genre[0] == band_url][1].tolist()
            
        musicians=df_musicians.loc[df_musicians[0] == band_url].iloc[:,1:]
        list_musicians=convert_df_to_list_of_tuples(musicians,[str,str,bool])
        
        #album data contains a lot of different types which are explicitely parsed
        albums=df_album.loc[df_album[0] == band_url].iloc[:,1:]
        list_parse_func=[str,convert_to_datetime,str,float,int]
        list_albums=[tuple([(try_parse(list_parse_func[idx],val)) 
                       for idx,val in enumerate(row)])
                            for row in albums.to_numpy()]
 
        yield [band_url,band_name,list_genres,list_musicians,list_albums]
   
def convert_to_datetime(d):
    '''
    Conversion of value into MongoDB compatible datetime format
    '''
    return datetime.datetime.strptime(str(d), '%d/%m/%Y')

def insert():
    '''
    Inserting of a list of json document into a MongoDB
    '''
    with pymongo.MongoClient('mongodb://localhost:27017/') as client:
        col = client.musicians.bands
        list_docs=create_json_documents()
        col.drop() 
        ids = col.insert_many(list_docs)
        #print(ids.inserted_ids)

#insert()


    
