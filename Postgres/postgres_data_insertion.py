import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from typing import Tuple,List
import os


'''
CSV file content:
band-band_name.csv URIs,names
band-album_data.csv: URIs,album names,release date,abstract,running time,sales amount.
band-genre_name.csv: URIs,genre
band-member-member_name.csv: band URIs, current member URIs, names
band-former_member-member_name.csv: band URIs, former member URIs, names

Tables in schema: bands,albums, genres,has_genre,musicians,member_of, has_name
'''

def parse_files(filename:str)->pd.DataFrame:
    '''
    Custom reading and parsing of csv files, by splitting according to ';' delimeter
    and deletion of '"'
    '''
    df = pd.read_csv(filename, encoding='utf8')
    df=df.iloc[:,0].str.split(';',expand=True,)
    for i in range(len(df.columns)):
        df.iloc[:,i]=df.iloc[:,i].str.replace(r'\"', '',regex=True)
        df.iloc[:,i]=df.iloc[:,i].str.replace(r'\'', '',regex=True)
    return df

def load_music_data_to_df()->Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    '''
    Loading of csv files into dataframes.
    '''
    os.chdir('..')
    df_album = pd.read_csv('csv_files/band-album_data.csv', sep=';', header=None)
    df_band_name=parse_files('csv_files/band-band_name.csv')
    df_member=parse_files('csv_files/band-member-member_name.csv')
    df_former_member=parse_files('csv_files/band-former_member-member_name.csv')
    df_genre=parse_files('csv_files/band-genre_name.csv')
    os.chdir('./Postgres')
    
    #adding of 'active'column and merging of members and former members into df_musicians
    df_member['active']=[True]*len(df_member)
    df_former_member['active']=[False]*len(df_former_member)
    df_musicians=pd.concat([df_member,df_former_member],axis='rows')
    return df_album,df_band_name,df_musicians,df_genre

    
def connect()->psycopg2.extensions.connection:
    '''
    Building of a connection to Postgres database
    '''
    conn = psycopg2.connect(
    host="localhost",
    database="musicians",
    user="postgres",
    password="root")
    return conn

def convert_df_to_list_of_tuples(df:pd.DataFrame, list_parse_func:List)->List[tuple]:
    '''
    Conversion of a dataframe to a list of tuples and explicit type
    casting of every column of the datframe according to the type functions
    in list_parse_func
    ''' 
    values=[tuple([(try_parse(list_parse_func[idx],val)) 
                   for idx,val in enumerate(row)])
                        for row in df.to_numpy()]
    return values

def convert_to_date(d):
    '''
    Conversion of value to Postgres compatible date format
    '''
    return datetime.strptime(str(d), '%d/%m/%Y').date()

def try_parse(fun,x):
    '''
    Wrapper function for handling type casting exception
    '''
    try:
        return fun(x)
    except ValueError or TypeError:
        return None


def execute_values(conn:psycopg2.extensions.connection,cursor:psycopg2.extensions.connection.cursor,query:str,values:List[tuple],table:str='table',fetch:bool=True):  
    '''
    Bulk insert using the objects given for the query and values parameters. The
    kwarg fetch is used to determine if the query is returning values
    '''
    try:
        psycopg2.extras.execute_values(cursor, query, values,page_size=len(values))
        if fetch:
            res = cursor.fetchall()
            ids = [i for item in res for i in item]
            print('Inserted tuples into {} table.'.format(table))
            return ids     
        else:
            print('Inserted tuples into {} table.'.format(table))
            return None       
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1 


def create_has_genre_df(dict_band_ids:dict,dict_genre_ids:dict, df_band_name:pd.DataFrame, df_genre:pd.DataFrame)->pd.DataFrame:
     '''
     Creation of the dataframe df_has_genre holding the values for the has_genre table
     '''
     list_band_ids=[dict_band_ids[band] for band in df_genre[0].tolist() if band in df_band_name[0].tolist()]
     list_genre_ids=[dict_genre_ids[genre] for genre in df_genre[1].tolist()]
     df_has_genre=pd.DataFrame(list(zip(list_band_ids,list_genre_ids))).drop_duplicates()
     return df_has_genre


def create_member_of_df(dict_musician_ids:dict, dict_band_ids:dict, df_musicians:pd.DataFrame)->pd.DataFrame:
    '''
    Creation of the dataframe df_member_of holding the values for the member_of table
    '''
    list_musicians=[dict_musician_ids[musician] for musician in df_musicians[1].tolist()]
    list_bands=[dict_band_ids[band] for band in df_musicians[0].tolist()]
    list_active=df_musicians['active']
    df_member_of=pd.DataFrame(list(zip(list_musicians,list_bands,list_active))).drop_duplicates()
    return df_member_of
 
    
def create_has_name_df(dict_musician_ids:dict,df_musicians:pd.DataFrame)->pd.DataFrame:
    '''
     Creation of the dataframe df_has_name holding the values for the has_name table
    '''   
    df_has_name=df_musicians[[1,2]].drop_duplicates()
    df_has_name[1]=[dict_musician_ids[musician] for musician in df_has_name[1].tolist()]
    return df_has_name


def insert():
    '''
    Loading of the data from the csv files and insertion into the corresponding tables
    '''
    df_album,df_band_name,df_musicians,df_genre=load_music_data_to_df()
    with connect() as conn:
        with conn.cursor() as cursor:
            # Insertion of data into bands table
            query_band="INSERT INTO bands(band_url,band_name) VALUES %s RETURNING band_id"
            values_band=convert_df_to_list_of_tuples(df_band_name,[str,str])
            ids_band=execute_values(conn,cursor,query_band,values_band,table='bands',fetch=True)
            
            # Insertion of data into albums table
            query_album="INSERT INTO albums(band_url,album_name,release_date,description,running_time,sales) VALUES %s RETURNING album_id"
            values_album=convert_df_to_list_of_tuples(df_album,[str,str,convert_to_date,str,float,int])
            _=execute_values(conn,cursor,query_album,values_album,table='albums',fetch=False)

            # Insertion of data into genres table
            query_genre="INSERT INTO genres(genre_name) VALUES %s RETURNING genre_id"
            values_genre=convert_df_to_list_of_tuples(df_genre[1].drop_duplicates().to_frame(),[str])
            ids_genre=execute_values(conn,cursor,query_genre,values_genre,table='genres',fetch=True)

            # Insertion of data into musicians table
            query_musician="INSERT INTO musicians(musician_url) VALUES %s RETURNING musician_id"
            values_musician=convert_df_to_list_of_tuples(df_musicians[1].drop_duplicates().to_frame(),[str])
            ids_musicians=execute_values(conn,cursor,query_musician,values_musician,table='musicians',fetch=True)

            # Initialization of dictionaries from returned ids to form foreign keys in other tables
            dict_musician_ids=dict(zip(df_musicians[1].drop_duplicates().tolist(),ids_musicians))
            dict_band_ids=dict(zip(df_band_name[0].tolist(),ids_band))
            dict_genre_ids=dict(zip(df_genre[1].drop_duplicates().tolist(),ids_genre))
            
            # Insertion of data into has_genre table
            df_has_genre=create_has_genre_df(dict_band_ids,dict_genre_ids, df_band_name, df_genre)
            query_has_genre="INSERT INTO has_genre(band_id,genre_id) VALUES %s"
            values_has_genre=values_band=convert_df_to_list_of_tuples(df_has_genre,[int,int])
            _=execute_values(conn,cursor,query_has_genre,values_has_genre,table='has_genre',fetch=False)
            
            # Insertion of data into member_of table
            df_member_of=create_member_of_df(dict_musician_ids, dict_band_ids, df_musicians)
            query_member_of="INSERT INTO member_of(musician_id,band_id,active) VALUES %s"
            values_member_of=convert_df_to_list_of_tuples(df_member_of,[int,int,bool])
            _=execute_values(conn,cursor,query_member_of,values_member_of,table='member_of',fetch=False)
            
            # Insertion of data into has_name table
            df_has_name=create_has_name_df(dict_musician_ids,df_musicians)
            query_has_name="INSERT INTO has_name(musician_id,musician_name) VALUES %s"
            values_has_name=convert_df_to_list_of_tuples(df_has_name,[int,str])
            _=execute_values(conn,cursor,query_has_name,values_has_name,table='has_name',fetch=False)
            print('Filling of database with data done.')
#insert()