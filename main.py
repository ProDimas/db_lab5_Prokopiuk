import psycopg2
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
from matplotlib.gridspec import GridSpec

username = 'Prokopiuk_Dmytro'
password = '123'
database = 'db_lab3'
host = 'localhost'
port = '5432'

# Query 2a
create_view_1 = '''
CREATE OR REPLACE VIEW GenresAmountsOfMovies AS
SELECT name, COUNT(movie_id)
FROM genres LEFT JOIN movie_has_genre USING (genre_id)
GROUP BY genre_id;
'''
query_1 = '''
SELECT *
FROM GenresAmountsOfMovies;
'''

# Query 2b
create_view_2 = '''
CREATE OR REPLACE VIEW TitlesGrosses AS
SELECT series_title, gross
FROM movies
WHERE NOT gross IS NULL;
'''
query_2 = '''
SELECT *
FROM TitlesGrosses;
'''

# Query 2c
create_view_3 = '''
CREATE OR REPLACE VIEW GenresAvarageRatingsValues AS
SELECT name, AVG(value)
FROM genres RIGHT JOIN movie_has_genre USING (genre_id)
			RIGHT JOIN imdb_ratings USING (movie_id)
WHERE update_date = '2021-02-01'
GROUP BY name;
'''
query_3 = '''
SELECT *
FROM GenresAvarageRatingsValues;
'''

# Dropping views
drop_views = '''
DROP VIEW IF EXISTS GenresAmountsOfMovies;
DROP VIEW IF EXISTS TitlesGrosses;
DROP VIEW IF EXISTS GenresAvarageRatingsValues;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()
    figure = plt.figure(tight_layout=True)
    spec = GridSpec(2, 2, figure=figure)
    bar1_ax = figure.add_subplot(spec[0, 0])
    bar2_ax = figure.add_subplot(spec[0, 1])
    pie_ax = figure.add_subplot(spec[1, 0:2])

    # Query 2a
    cur.execute(create_view_1)
    cur.execute(query_1)

    genres, movies_num = list(zip(*cur.fetchall()))

    bars1 = bar1_ax.bar(range(len(genres)), movies_num)
    bar1_ax.bar_label(bars1, movies_num)
    bar1_ax.set_xticks(ticks=range(len(genres)), labels=genres, rotation=90)
    bar1_ax.set_xlabel('Назва жанру')
    bar1_ax.set_ylabel('Кількість фільмів')
    bar1_ax.set_title('2a) Жанри та кількості фільмів у відповідних жанрах')

    # Query 2b
    cur.execute(create_view_2)
    cur.execute(query_2)

    titles, gross = list(zip(*cur.fetchall()))

    pie_ax.pie(gross, labels=titles, autopct='%1.1f%%')
    pie_ax.set_title('2b) Частки касових зборів кожного фільму')

    # Query 2с
    cur.execute(create_view_3)
    cur.execute(query_3)

    genres, avg_values = list(zip(*cur.fetchall()))
    avg_values = list(map(lambda x: round(float(x), 3), avg_values))

    bars2 = bar2_ax.bar(range(len(genres)), avg_values)
    bar2_ax.bar_label(bars2, avg_values)
    bar2_ax.set_xticks(ticks=range(len(genres)), labels=genres, rotation=90)
    locator_base = 0.05
    bar2_ax.set_ylim(min(avg_values) - locator_base, max(avg_values) + locator_base)
    bar2_ax.yaxis.set_major_locator(MultipleLocator(locator_base, min(avg_values)))
    bar2_ax.set_xlabel('Назва жанру')
    bar2_ax.set_ylabel('Середнє арифметичне оцінок')
    bar2_ax.set_title('2c) Жанри та середні арифметичні оцінок на IMDB фільмів відповідних жанрів')

    cur.execute(drop_views)

mng = plt.get_current_fig_manager()
mng.resize(1500, 700)
plt.show()