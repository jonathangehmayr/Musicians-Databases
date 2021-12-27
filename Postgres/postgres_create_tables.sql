DROP TABLE IF EXISTS bands CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS has_genre CASCADE;
DROP TABLE IF EXISTS musicians CASCADE;
DROP TABLE IF EXISTS member_of CASCADE;
DROP TABLE IF EXISTS albums CASCADE;
/*DROP TABLE IF EXISTS publishes CASCADE;*/
DROP TABLE IF EXISTS has_name CASCADE;


/*use UNIQUE constraint for genres etc when ready*/
CREATE TABLE bands (
	band_id 	SERIAL 	PRIMARY KEY,
	band_url 	TEXT 	NOT NULL,
	band_name 	TEXT 	NOT NULL
);

CREATE TABLE genres(
	genre_id	SERIAL PRIMARY KEY,
	genre_name	TEXT 	NOT NULL
);

CREATE TABLE has_genre(
	band_id 	SERIAL 	REFERENCES bands,
	genre_id 	SERIAL 	REFERENCES genres,
	PRIMARY KEY (band_id, genre_id)
);

CREATE TABLE musicians(
	musician_id 	SERIAL	PRIMARY KEY,
	musician_url	TEXT
);

CREATE TABLE has_name(
	musician_id 	SERIAL	 REFERENCES musicians ON DELETE CASCADE,
	musician_name 	TEXT,
	PRIMARY KEY (musician_id, musician_name)
);

CREATE TABLE member_of(
	musician_id	SERIAL REFERENCES musicians,
	band_id		SERIAL REFERENCES bands,
	active 		BOOL,
	PRIMARY KEY (musician_id, band_id,active)

);

CREATE TABLE albums(
	album_id		SERIAL	PRIMARY KEY,
	band_url		TEXT NOT NULL,
	album_name 		TEXT NOT NULL,
	release_date	DATE,
	description		TEXT,
	running_time	DOUBLE PRECISION,
	sales			BIGINT
);

/*
CREATE TABLE publishes(
	band_id SERIAL REFERENCES bands,
	album_id SERIAL REFERENCES albums,
	PRIMARY KEY (band_id, album_id)
);*/


