CREATE TABLE movies
(
  movie_id INT NOT NULL,
  series_title VARCHAR(70) NOT NULL,
  released_year NUMERIC(4, 0) NOT NULL,
  certificate VARCHAR(10),
  runtime NUMERIC(3, 0) NOT NULL,
  overview VARCHAR(320) NOT NULL,
  director VARCHAR(40) NOT NULL,
  gross INT,
  PRIMARY KEY (movie_id)
);

CREATE TABLE genres
(
  genre_id INT NOT NULL,
  name VARCHAR(20) NOT NULL,
  PRIMARY KEY (genre_id),
  UNIQUE (name)
);

CREATE TABLE imdb_ratings
(
  rating_id INT NOT NULL,
  value NUMERIC(3, 1) NOT NULL,
  number_of_votes INT NOT NULL,
  update_date DATE NOT NULL,
  metascore NUMERIC(3, 0),
  movie_id INT NOT NULL,
  PRIMARY KEY (rating_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

CREATE TABLE movie_has_genre
(
  genre_id INT NOT NULL,
  movie_id INT NOT NULL,
  PRIMARY KEY (genre_id, movie_id),
  FOREIGN KEY (genre_id) REFERENCES genres(genre_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);