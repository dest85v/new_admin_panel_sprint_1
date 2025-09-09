from typing import Union
from db_data_classes import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
from pgsql_context_manager import PostgresSaver
from sqlite_context_manger import SQLiteLoader
import sqlite3
from psycopg import connection as pg_connection
import logging

BATCH_SIZE = 100


# Словарь, наименование таблицы для переноса данных и датакласс с описанием строки
# Таблицы должны быть заранее упорядоченны с учётом зависимостей
tables_for_load = {
    'film_work': FilmWork,
    'person': Person,
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork,
}


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: pg_connection, tables_for_load: dict[str, Union[FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork]]):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(sqlite_conn, BATCH_SIZE)

    for table_name, table_class in tables_for_load.items():
        logging.info('Перенос данных таблицы %s', table_name)
        for batch in sqlite_loader.transform_data(table_name, table_class):
            postgres_saver.save_data(batch, table_name, table_class)
