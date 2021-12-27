
--ROLLBACK;

--1. Get the most successful band in the 2010s (01.01.2010 - 31.12.2019) in the most successful genre of the 1990s (01.01.1990 - 31.12.1999)
create or replace function get_most_successful_genre_id_90s() 
returns genres.genre_id%type
language plpgsql
as
$$
declare
   genre_id genres.genre_id%type;
begin 
	SELECT g.genre_id INTO genre_id
	FROM albums AS a
	JOIN bands AS b ON a.band_url=b.band_url
	JOIN has_genre AS hg ON b.band_id=hg.band_id
	JOIN genres AS g ON hg.genre_id=g.genre_id
	WHERE release_date BETWEEN '1989-12-31' AND '2000-01-01'
	AND sales IS NOT NULL
	GROUP BY g.genre_id ORDER BY SUM(sales) DESC
	LIMIT 1;
	RETURN genre_id;
end; 
$$;
SELECT get_most_successful_genre_id_90s();


create or replace function get_most_succesful_band_in_timeframe_in_most_successful_genre_90s(start_date date,end_date date)
returns bands.band_url%type
language plpgsql
as
$$
declare
   most_successful_genre genres.genre_id%type;
   band_url bands.band_url%type;
begin 
    SELECT get_most_successful_genre_id_90s() INTO most_successful_genre;
	SELECT b.band_url into band_url
	FROM albums AS a
	JOIN bands AS b ON a.band_url=b.band_url
	JOIN has_genre AS hg ON b.band_id=hg.band_id
	WHERE hg.genre_id=most_successfuL_genre
	AND release_date BETWEEN start_date AND end_date
	AND sales IS NOT NULL
	GROUP BY b.band_url ORDER BY SUM(sales) DESC
	LIMIT 1;
	RETURN band_url;
end; 
$$;
SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s('31-12-2009'::date, '01-01-2020'::date);



--2. Add a new album to the most successful band of most successful genre in the 90s, so that it is more successful than all of the albums of the most successful band in this genre in the 10s

create or replace function get_highest_sales_of_all_albums()
returns albums.sales%type
language plpgsql
as
$$
declare
    highest_sales albums.sales%type;
begin 
	SELECT albums.sales INTO highest_sales
	FROM albums AS a
	WHERE sales IS NOT NULL
	ORDER BY a.sales DESC
	LIMIT 1;
	RETURN highest_sales;
end; 
$$;
SELECT get_highest_sales_of_all_albums();



create or replace function insert_album_in_10s_for_most_successful_band_of_most_succesful_genre_in_90s()
returns	 albums
language plpgsql
as
$$
declare
    inserted_album albums%rowtype;
	highest_sales albums.sales%type;
    succesful_band_url bands.band_url%type;
begin 
	SELECT get_most_succesful_band_in_timeframe_in_most_successful_genre_90s('31-12-1989'::date, '01-01-2000'::date) INTO succesful_band_url;
	SELECT get_highest_sales_of_all_albums() INTO highest_sales;
	INSERT INTO albums(band_url,album_name,release_date,description,running_time,sales) 
	VALUES(succesful_band_url, 'nice album name', '2019-04-20','nice description', 50.7, highest_sales+1)
	RETURNING * into inserted_album;
	RETURN inserted_album;
end; 
$$;
SELECT insert_album_in_10s_for_most_successful_band_of_most_succesful_genre_in_90s();


/*
DELETE FROM albums
WHERE album_name='nice album name';

SELECT album_name
FROM albums
WHERE album_name='nice album name';
*/
