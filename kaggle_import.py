import csv
import psycopg2
import os
from datetime import datetime

username = 'Prokopiuk_Dmytro'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

CSV_FILE = 'imdb_top_1000.csv'

# Query to empty all tables
empty_tables = '''
DELETE FROM imdb_ratings CASCADE;
DELETE FROM movie_has_genre CASCADE;
DELETE FROM genres CASCADE;
DELETE FROM movies CASCADE;
'''

movie_insert = '''
INSERT INTO movies (movie_id, series_title, released_year, certificate, runtime, overview, director, gross)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

imdb_rating_insert = '''
INSERT INTO imdb_ratings (rating_id, value, number_of_votes, update_date, metascore, movie_id)
VALUES (%s, %s, %s, %s, %s, %s)
'''

genre_insert = '''
INSERT INTO genres (genre_id, name)
VALUES (%s, %s)
'''

movie_has_genre_insert = '''
INSERT INTO movie_has_genre(genre_id, movie_id)
VALUES (%s, %s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

def certificate_or_none(certificate_str: str) -> str:
    if certificate_str != '':
        return certificate_str
    else:
        return None

def runtime_without_prefix(runtime_str: str) -> int:
    prefix = ' min'
    return int(runtime_str[:-len(prefix)])

def gross_without_commas_or_none(gross_str: str) -> int:
    if gross_str != '':
        return int(gross_str.replace(',', ''))
    else:
        return None

def metascore_or_none(metascore_str: str) -> int:
    if metascore_str != '':
        return int(metascore_str)
    else:
        return None

with conn:
    cur = conn.cursor()
    cur.execute(empty_tables)

    # Getting date of last modification of csv file
    csv_file_timestamp = os.path.getmtime('imdb_top_1000.csv')
    csv_file_date = str(datetime.fromtimestamp(csv_file_timestamp).date())

    ROWS_TO_READ = 20
    with open(CSV_FILE, 'r') as file:
        reader = csv.DictReader(file)
        
        # i is used to read ROWS_TO_READ rows and as a movie_id
        i = 1
        # rating_id_offset is added to i to form rating_id
        rating_id_offset = 1000
        # id of next genre that would be added into table
        iteration_genre_id = 10001
        # {genre_name: genre_id} dictionary
        genres = dict()
        for row in reader:
            movie_values = (
                i,
                row['Series_Title'],
                int(row['Released_Year']),
                certificate_or_none(row['Certificate']),
                runtime_without_prefix(row['Runtime']),
                row['Overview'],
                row['Director'],
                gross_without_commas_or_none(row['Gross'])
            )
            imdb_rating_values = (
                rating_id_offset + i,
                float(row['IMDB_Rating']),
                int(row['No_of_Votes']),
                csv_file_date,
                metascore_or_none(row['Meta_score']),
                i,
            )

            # Insert new values for movie
            cur.execute(movie_insert, movie_values)
            # Insert new values for movie's rating on imdb
            cur.execute(imdb_rating_insert, imdb_rating_values)

            # Loop through genres of current movie
            for genre_name in row['Genre'].split(', '):
                inserted_genre_id = genres.setdefault(genre_name, iteration_genre_id)
                if inserted_genre_id == iteration_genre_id:
                    # This means we've just found a new genre,
                    # so we should insert it into table and increment iteration_genre_id for next genres
                    cur.execute(genre_insert, (inserted_genre_id, genre_name))
                    iteration_genre_id += 1

                # Insert new movie-genre relationship to movie_has_genre table
                cur.execute(movie_has_genre_insert, (inserted_genre_id, i))
            
            if i == ROWS_TO_READ:
                break

            i += 1

    conn.commit()
