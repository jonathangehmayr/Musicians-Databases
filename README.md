# Musicians-Databases

The Muscians-Database repo is a university project and contains scripts for creating a Postgres and MongoDB database for modeling musician data in SQL and NoSQL. Both databases have been stepwise optimized, whereas for each optimization step the performance was measured.

## Project structure

The repo is structured in the following way:

    
    ├── Musicians-Databases
    ├── MongoDB                                                       # MongoDB scripts
    │   ├── __init__.py                                               # package initialization
    |   ├── mongo_data_insertion.py                                   # Insertion of documnets in MongoDB
    |   ├── mongo_database_optimization_walk_through.ipynb            # Explanatory jupyter notebook
    |   ├── mongo_optimization.py                                     # Stepwise optimization for MongoDB
    |   ├── mongo_optimization_measurements.py                        # Execution time measurements for different optimization levels MongoDB
    │   └── mongo_queries.py                                          # Defined Queries for MongoDB
    ├── Postgres                                                      # Postgres scripts
    │   ├── __init__.py                                               # package initialization
    │   ├── postgres_create_tables.sql                                # Table Creation for Postgres
    │   ├── postgres_data_insertion.py                                # Insertion of tuples into Postgres tables
    │   ├── postgres_database_optimization_walk_through.ipynb         # Explanatory jupyter notebook
    │   ├── postgres_optimization.sql                                 # Stepwise optimization for Postgres
    │   ├── postgres_optimization_measurements.py                     # Execution time measurements for different optimization levels Postgres
    │   └── postgres_queries.sql                                      # Defined Queries for Postgres
    ├── csv_files                                                     # Csv files containing data for the databases
    |   ├── band-album_data.csv                                       # Data source 1
    |   ├── band-band_name.csv                                        # Data source 2
    |   ├── band-former_member-member_name.csv                        # Data source 3
    │   ├── band-genre_name.csv                                       # Data source 4
    │   └── band-member-member_name.csv                               # Data source 5
    ├── repo_pics                                                     # images needed in repo
    |   └── er_diagram.png                                            # ER diagram
    ├── README.md
    └── .gitignore
    
    
## Project Description

### Data 

The data inserted into the databases was queried from [DBpedia](https://www.dbpedia.org/), and supplied by the university. The [csv files](https://github.com/jonathangehmayr/Musicians-Databases/tree/main/csv_files) hold subsequently listed variables:
  
    └── csv_files
        ├── band-band_name.csv  
        │   ├── Band URIs
        │   └── Band names                      
        ├── band-album_data.csv
        │   ├── Band URIs
        │   ├── Album names
        │   ├── Release dates
        │   ├── Abstracts
        │   ├── Running times
        │   └── Sales              
        ├── band-genre_name.csv 
        │   ├── Band URIs 
        │   └── Genre names         
        ├── band-member-member_name.csv: 
        │   ├── Band URIs 
        │   ├── Current member URIs 
        │   └── Member names                  
        ├── band-former_member-member_name.csv:  
        │   ├── Band URIs 
        │   ├── Former member URIs 
        │   └── Former member names  
        └── mongo_queries.py 
        
### Databases
 
In this project a Postgres and MongoDB database were created. For the data in the [csv files](https://github.com/jonathangehmayr/Musicians-Databases/tree/main/csv_files) database schemas were constructed that support two complex queries:

  1. **Get the most successful band in the 2010s (01.01.2010 - 31.12.2019) in the most successful genre of the 1990s (01.01.1990 - 31.12.1999)**

  2. **Add a new album to the most successful band of most successful genre in the 1990s, so that it is more successful than all of the albums of the most successful band in this genre in the 2010s**

After creating the database the goal was to optimize the databases. For each database a stepwise optimization procedure was designed. For each optimization level the performance was measured by taking the execution time of a query. For this task only the first complex query was used as the computations of both queris are similar.

#### Postgres

For the relational database the data was modelled according to the following ER-diagram:

![alt text](https://github.com/jonathangehmayr/Musicians-Databases/blob/main/repo_pics/er_diagram.png)

The performance measurements for Postgres were taken for each of the listed optimization levels:
  1. **No optimization**
  2. **Optimization by creating indexes on `albums.release_date` and `albums.band_urls`**
  3. **Optimization by creating a materialized view on the common join operations**
  4. **Optimization by indexing the materialized view of the most common joins**
  5. **Optimization by precomputing the results of the 1. complex query and saving it as a materialized view**


#### MongoDB

The non relational database was modelled using a single collection with documents in below JSON format:

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

The performance measurements for MongoDB were taken for each of the listed optimization levels:
1. **No optimization**
2. **Optimization by adding indexes on the fields `albums.release_date` and `genres`**
3. **Optimization by precomputing the sum of sales in the 1990s and 2010s and adding additional fields to relevant documents**


## Exemplary Notebooks

Applying the scripts in this repo is explained with two jupyter notebooks, one for Postgres and one for the MongoDB respectively:

* [Notebook](https://github.com/jonathangehmayr/Musicians-Databases/blob/main/Postgres/postgres_database_optimization_walk_through.ipynb) for Postgres
* [Notebook](https://github.com/jonathangehmayr/Musicians-Databases/blob/main/MongoDB/mongo_database_optimization_walk_through.ipynb) for MongoDB

    
