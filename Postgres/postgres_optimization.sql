/*
Optimization of the musicians database is tested in stages:
	1. without optimization
	2. by creating of indexes on albums.release_date and albums.band_urls
	3. by buidling a materialized with on the common join operations and
	4. by indexing the materialized view of the most common joins
	5. by precomputing the results of the 1. complex query and saving it as a materialized view

For the actual measurements see the relevant Python script
*/


--2.
/*Initializing indexes on albums.release_date and albums.band_urls*/
create or replace function create_indexes()
returns void
language plpgsql
as
$$
begin 
	DROP INDEX IF EXISTS index_album_release_date;
	CREATE INDEX index_album_release_date ON albums USING btree (release_date)
	WHERE sales IS NOT NULL;

	DROP INDEX IF EXISTS index_album_band_urls;
	CREATE INDEX index_album_band_urls ON albums USING btree (band_url);
end; 
$$;
--SELECT create_indexes()


--3.
/*Materialized view on the most common joins*/
DROP MATERIALIZED VIEW IF EXISTS view_band_genre_sales_release_date;
CREATE MATERIALIZED VIEW view_band_genre_sales_release_date
AS SELECT g.genre_id, b.band_id,b.band_url, a.release_date, a.sales 
FROM genres AS g
JOIN has_genre AS hg
ON g.genre_id = hg.genre_id
JOIN bands AS b
ON b.band_id = hg.band_id
JOIN albums AS a
ON a.band_url = b.band_url
WHERE a.sales IS NOT NULL;


/*Optimized function by querying the materialized view with the most common joins*/
create or replace function get_most_successful_genre_id_90s_optimized_view_joins()
returns genres.genre_id%type
language plpgsql
as
$$
declare
   	genre_id genres.genre_id%type;
begin 
	SELECT v.genre_id INTO genre_id
	FROM view_band_genre_sales_release_date as v
	WHERE v.release_date BETWEEN '1989-12-31' AND '2000-01-01'
	GROUP BY v.genre_id ORDER BY SUM(sales) DESC
	LIMIT 1;
	RETURN genre_id;
end; 
$$;
SELECT get_most_successful_genre_id_90s_optimized_view_joins();

/*Optimized function by querying the materialized view with the most common joins*/
create or replace function get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_optimzed_view_joins(start_date date,end_date date)
returns bands.band_url%type
language plpgsql
as
$$
declare
   most_successful_genre genres.genre_id%type;
   band_url bands.band_url%type;
begin 
    SELECT get_most_successful_genre_id_90s_optimized_view_joins() INTO most_successful_genre;
	SELECT v.band_url into band_url
	FROM view_band_genre_sales_release_date AS v
	WHERE v.release_date BETWEEN '2009-12-31' AND '2020-01-01'
	AND v.genre_id=most_successfuL_genre
	GROUP BY v.band_url ORDER BY SUM(sales) DESC
	LIMIT 1;
	RETURN band_url;	
end; 
$$;
SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_optimzed_view_joins('31-12-2009'::date, '01-01-2020'::date);



--4.
/*Initialize index on the materialized view of 3.*/
create or replace function create_index_on_view()
returns void
language plpgsql
as
$$
begin 
	DROP INDEX IF EXISTS index_view_date;
	CREATE INDEX index_view_date ON view_band_genre_sales_release_date USING btree (release_date);
end; 
$$;
SELECT create_index_on_view();

--5.
/*Materilized view saving the results of complex query 1.*/
DROP MATERIALIZED VIEW IF EXISTS view_most_successful_band;
CREATE MATERIALIZED VIEW view_most_successful_band
AS SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_optimzed_view_joins('31-12-2009'::date, '01-01-2020'::date) AS band_url;

/*Function for querying the materialized view holding the result of complex query 1.*/
create or replace function get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_fully_optimized()
returns view_most_successful_band.band_url%type
language plpgsql
as
$$
declare
	url view_most_successful_band.band_url%type;
begin 
	SELECT band_url FROM view_most_successful_band INTO url;
	RETURN url;
end; 
$$;
SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s_fully_optimized();


/*For test purposes the created indexes must droppable*/
create or replace function drop_indexes()
returns void
language plpgsql
as
$$
begin 
	DROP INDEX IF EXISTS index_album_release_date;
	DROP INDEX IF EXISTS index_album_band_urls;
	DROP INDEX IF EXISTS index_view_date;
end; 
$$;
--SELECT drop_indexes();



/*
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY indexname;
*/



