import json
import psycopg2

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

OUTPUT_FILE = 'json_export.json'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

data = {}
with conn:
    cur = conn.cursor()
    
    for table_name in TABLES:
        cur.execute('SELECT * FROM ' + table_name)
        fields = tuple(map(lambda x: x.name, cur.description))
        data[table_name] = list(map(lambda row: dict(zip(fields, row)), cur.fetchall()))

with open(OUTPUT_FILE, 'w') as outf:
    json.dump(data, outf, default = str)
    