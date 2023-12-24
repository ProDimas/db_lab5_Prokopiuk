import csv
import psycopg2
import os

username = 'Prokopiuk_Dmytro'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

TABLES = [
    'movies',
    'genres',
    'imdb_ratings',
    'movie_has_genre'
]

FOLDER_NAME = 'csv_export'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    if not os.path.exists(FOLDER_NAME):
        os.mkdir(FOLDER_NAME)

    for table_name in TABLES:
        cur.execute('SELECT * FROM ' + table_name)
        fields = tuple(map(lambda x: x.name, cur.description))
        with open(FOLDER_NAME + '/' + table_name + '.csv', 'w') as outfile:
            writer = csv.writer(outfile, lineterminator='\n')
            writer.writerow(fields)
            for row in cur:
                writer.writerow(list(map(str, row)))