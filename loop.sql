DROP TABLE IF EXISTS movies_copy;

CREATE TABLE movies_copy AS
SELECT *
FROM movies;

DELETE FROM movies_copy;

DO $$
	DECLARE
    	start_movie_id      movies_copy.movie_id%TYPE;
    	series_title        movies_copy.series_title%TYPE;
		start_released_year movies_copy.released_year%TYPE;
		rand_certificate    movies_copy.certificate%TYPE;
		rand_runtime        movies_copy.runtime%TYPE;
		overview            movies_copy.overview%TYPE;
		director            movies_copy.director%TYPE;
		rand_gross          movies_copy.gross%TYPE;

	BEGIN
    	start_movie_id      := 0;
		series_title        := 'Good film: volume №';
		start_released_year := 2000;
		overview            := 'Standard overview №';
		director            := 'Director №';
		
    	FOR counter IN 1..20
        	LOOP
				rand_certificate := CHR(CAST(TRUNC(RANDOM() * (90 - 65 + 1) + 65) AS integer));
				rand_runtime     := CAST(TRUNC(RANDOM() * (180 - 60 + 1) + 60) AS integer);
				rand_gross       := CAST(TRUNC(RANDOM() * (1000000 - 150000 + 1) + 150000) AS integer);
            	INSERT INTO movies_copy (movie_id, series_title, released_year, certificate, runtime, overview, director, gross)
             	VALUES (start_movie_id + counter, series_title || counter, start_released_year + counter, rand_certificate,
					    rand_runtime, overview || counter, director || counter, rand_gross);
         	END LOOP;
 	END;
$$;

SELECT *
FROM movies_copy;