import datetime as dt
import logging
import os
import os.path
from contextlib import contextmanager
from time import sleep

import backoff
import elasticsearch
import psycopg2
import redis
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from elasticsearch import helpers
from queries import make_query, make_prequery


@backoff.on_exception(backoff.expo, BaseException)
def etl_part2(
        pg_objects: list or tuple,
        es: elasticsearch.client.Elasticsearch, state: str) -> None:

    def gendata(index):
        for doc in data_to_load:
            record = {'_id': doc['uuid'],
                      '_op_type': 'index',
                      '_index': index,
                      **doc}
            yield record

    data_to_load = []

    for data in pg_objects:
        # Подготовка объекта в ElasticSearch
        if state == 'film_work':
            object = {
                'uuid': data['id'],
                'imdb_rating': data['rating'],
                'genre': data['genres'],
                'title': data['title'],
                'description': data['description'],
                'directors_names': data['directors_names'],
                'actors_names': data['actors_names'],
                'writers_names': data['writers_names'],
                'directors': data['directors'],
                'actors': data['actors'],
                'writers': data['writers']
            }
        elif state == 'genre':
            object = {
                'uuid': data['gid'],
                'name': data['name']
            }
        data_to_load.append(object)

    # Отправка пачки объектов в ElasticSearch
    if state == 'film_work':
        helpers.bulk(es, gendata('movies'))
    elif state == 'genre':
        helpers.bulk(es, gendata('genres'))


@backoff.on_exception(backoff.expo, BaseException)
def etl_part1(
        pg_conn: _connection, es: elasticsearch.client.Elasticsearch) -> None:
    """Считывание состояния последнего обновления в хранилище
       Детектирование более новых записей в БД относительно состояния
       Обновление соответствующих записей в ElasticSearch"""
    logging.info('Начало etl')

    # Подключение к хранилищу, получение состояния
    r = redis.Redis()  # (host=os.environ.get('REDIS_HOST'), port=os.environ.get('REDIS_PORT'))
    if not r.exists('film_work') == 1:
        r.set(name='film_work', value=dt.datetime.min.isoformat())
    if not r.exists('person') == 1:
        r.set(name='person', value=dt.datetime.min.isoformat())
    if not r.exists('genre') == 1:
        r.set(name='genre', value=dt.datetime.min.isoformat())

    state_fw = dt.datetime.fromisoformat(r.get('film_work').decode("utf-8"))
    state_g = dt.datetime.fromisoformat(r.get('genre').decode("utf-8"))
    state_p = dt.datetime.fromisoformat(r.get('person').decode("utf-8"))

    states = {
        'film_work': state_fw,
        'genre': state_g,
        'person': state_p}

    # Подключение к БД, обнаружение обновленных относительно состояния записей
    pg_cursor = pg_conn.cursor()

    for state, date in states.items():
        # Берем все id фильмов, где изменился жанр или человек пачками
        if state == 'genre' or state == 'person':
            films_ids = []
            genres = []
            # Заодно берем сами жанры или персон и обновляем
            pg_cursor.execute(make_prequery(state, date))
            while True:
                rows = pg_cursor.fetchmany(100)
                if len(rows) > 0:
                    for row in rows:
                        films_ids.append(row['fid'])
                        if state == 'genre':
                            genre = {'gid': row['gid'],
                                     'name': row['name']}
                            if genre not in genres:
                                genres.append(genre)
                else:
                    break
            # Обновляем изменившиеся жанры в индексе genres
            etl_part2(genres, es, state)

            # По id фильмов берем полную информацию для отправки пачками
            for films in range(0, len(films_ids), 10):
                pg_cursor.execute(make_query("WHERE fw.id IN {0}".format(
                    tuple(films_ids[films:films + 10]))))
                # Тут можно использовать fetchall(), т.к. цикл по 10 объектов
                etl_part2(pg_cursor.fetchall(), es, 'film_work')

            r.set(name=state, value=dt.datetime.utcnow().isoformat())

        # Берем все обновленные фильмы пачками
        elif state == 'film_work':
            all_films = []
            pg_cursor.execute(make_query("WHERE fw.updated_at > '{0}'".format(
                date)))
            while True:
                rows = pg_cursor.fetchmany(100)
                if len(rows) > 0:
                    for row in rows:
                        all_films.append(row)
                else:
                    break
            # Отправляем данные на отправку пачками
            for films in range(0, len(all_films), 10):
                etl_part2(tuple(all_films[films:films + 10]), es, state)
            r.set(name=state, value=dt.datetime.utcnow().isoformat())


@backoff.on_exception(backoff.expo, BaseException)
def try_connect(dsl: dict, es_dsl: dict) -> None:
    """Подключение к PostgreSQL и ElasticSearch"""
    @contextmanager
    def pg_context(dsl: dict):
        pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        try:
            yield pg_conn
        finally:
            pg_conn.close()

    es = elasticsearch.Elasticsearch([es_dsl])
    logging.info('Подключение к ElasicSearch выполнено')

    with pg_context(dsl) as pg_conn, pg_conn:
        logging.info('Подключение к БД выполнено')
        etl_part1(pg_conn, es)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='main.log',
        format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
        encoding='utf-8'
    )

    dsl = {
        'dbname': 'movies_database',  # os.environ.get('POSTGRES_DB'),
        'user': 'app',  # os.environ.get('POSTGRES_USER'),
        'password': '123qwe',  # os.environ.get('POSTGRES_PASSWORD'),
        'host': '127.0.0.1',  # os.environ.get('DB_HOST', '127.0.0.1'),
        'port': 5432  # os.environ.get('DB_PORT', 5432)
    }

    es_dsl = {
        'host': 'localhost',  # os.environ.get('ES_HOST', 'localhost'),
        'port': 9200  # os.environ.get('ES_PORT', 9200)
        }

    while True:
        try_connect(dsl, es_dsl)
        sleep(10)
