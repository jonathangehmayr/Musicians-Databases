import pandas as pd
import psycopg2
import psycopg2.extras
from time import time


def measure_query_runtime(query:str, n_runs:int)->float:
    '''
    Measuring the mean runtime of a query for n_runs
    '''
    list_runtimes=[]
    for i in range(n_runs):
        with psycopg2.connect(dbname='musicians', user='postgres', password='root') as conn:
            cur = conn.cursor()
            start=time()
            cur.execute(query)
            stop=time()
        runtime=stop-start
        list_runtimes.append(runtime)
    return sum(list_runtimes)/len(list_runtimes)


def run_query(query:str)->None:
    '''
    Execution of a query for Postgres
    '''
    with psycopg2.connect(dbname='musicians', user='postgres', password='root') as conn:
        cur = conn.cursor()
        cur.execute(query)
        
        
if __name__=='__main__':
    query_drop_indexes='''SELECT drop_indexes();'''
    query_create_indexes='''SELECT create_indexes();'''
    query_create_index_view='''SELECT create_index_on_view();'''
    query_not_optimized='''SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s('31-12-2009'::date, '01-01-2020'::date);'''
    query_optimized_view='''SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_optimzed_view_joins('31-12-2009'::date, '01-01-2020'::date);'''
    query_fully_optimized='''SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_fully_optimized()'''
    
    n_runs=10
        
      
    #runtime not optimized
    run_query(query_drop_indexes)
    runtime_not_optimized=measure_query_runtime(query_not_optimized, n_runs)
    print('Runtime not optimized: {}'.format(runtime_not_optimized))
    
    
    #runtime with indexes
    run_query(query_create_indexes)
    runtime_with_indexes=measure_query_runtime(query_not_optimized, n_runs)
    print('Runtime with indexes: {}'.format(runtime_with_indexes))
    
    #runtime with view on joins
    runtime_with_view=measure_query_runtime(query_optimized_view, n_runs)
    print('Runtime with view on joins: {}'.format(runtime_with_view))
    
    
    #runtime with index on view on joins:
    run_query(query_create_index_view)
    runtime_with_view_and_index=measure_query_runtime(query_optimized_view, n_runs)
    print('Runtime with index on view on joins: {}'.format(runtime_with_view_and_index))
    
    
    #runtime fully optimized query
    runtime_fully_optimized=measure_query_runtime(query_fully_optimized, n_runs)
    print('Runtime fully optimized: {}'.format(runtime_fully_optimized))
    
