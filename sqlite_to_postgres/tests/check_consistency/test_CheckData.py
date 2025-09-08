import pytest
import os
import sqlite3
#from contextlib import closing
from dataclasses import dataclass, fields  #, astuple
#from typing import Generator, Any, Union, Tuple
from uuid import UUID
from datetime import datetime, date

import psycopg
from psycopg import ClientCursor  #, connection as pg_connection
from psycopg.rows import dict_row
#from psycopg import errors as pg_errors


BATCH_SIZE = 100


@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str
    creation_date: date
    file_path: str
    rating: float
    type: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created: datetime
    modified: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)
        if isinstance(self.modified, str):
            self.modified = datetime.fromisoformat(self.modified)


@dataclass
class GenreFilmWork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.genre_id, str):
            self.genre_id = UUID(self.genre_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)


@dataclass
class PersonFilmWork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime

    # этим методом мы гарантируем, что в поле id у нас будет именно UUID
    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)
        if isinstance(self.person_id, str):
            self.person_id = UUID(self.person_id)
        if isinstance(self.film_work_id, str):
            self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.created, str):
            self.created = datetime.fromisoformat(self.created)


@pytest.fixture(scope="function")
def db_connection_sqlite():
    conn = sqlite3.connect('./sqlite_to_postgres/db.sqlite')
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture(scope="function")
def db_connection_pgsql():
    dsl = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }
    conn = psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=ClientCursor
        )
    yield conn
    conn.rollback()
    conn.close()


class TestLoadedData:
    # Словарь, наименование таблицы для переноса данных и датакласс с описанием строки
    # Таблицы должны быть заранее упорядоченны с учётом зависимостей
    tables_for_load = {
        'film_work': FilmWork,
        'person': Person,
        'genre': Genre,
        'genre_film_work': GenreFilmWork,
        'person_film_work': PersonFilmWork,
    }

    # Проверка соответсвия количества строк в оригинальной таблице и таблице назначения
    @pytest.mark.parametrize('table_name', list(tables_for_load.keys()))
    def test_check_count_rows_between_orig_dest_count(self, table_name, db_connection_sqlite, db_connection_pgsql):
        sqlite_cursor = db_connection_sqlite.cursor()
        pgsql_cursor = db_connection_pgsql.cursor()
        sqlite_cursor.execute(f"SELECT COUNT(*) AS ROWS_CNT FROM {table_name}")
        sqlite_result = sqlite_cursor.fetchone()
        pgsql_cursor.execute(f"SELECT COUNT(*) AS ROWS_CNT FROM content.{table_name}")
        pgsql_result = pgsql_cursor.fetchone()
        assert sqlite_result[0] == pgsql_result['rows_cnt']

    # Проверка соответсвия каждой строки в оригинальной таблице и таблице назначения
    @pytest.mark.parametrize('table_name', list(tables_for_load.keys()))
    def test_check_each_rows_between_orig_dest_count(self, table_name, db_connection_sqlite, db_connection_pgsql):
        row_dataclass = self.tables_for_load[table_name]
        columns = [x.name for x in fields(row_dataclass)]
        column_list = ', '.join(columns)
        sqlite_cursor = db_connection_sqlite.cursor()
        pgsql_cursor = db_connection_pgsql.cursor()
        sqlite_cursor.execute(f"SELECT {column_list.replace('created', 'created_at').replace('modified', 'updated_at')} FROM {table_name} ORDER BY id")
        pgsql_cursor.execute(f"SELECT {column_list} FROM content.{table_name} ORDER BY id")
        while sqlite_results := sqlite_cursor.fetchmany(BATCH_SIZE):
            pgsql_results = pgsql_cursor.fetchmany(BATCH_SIZE)
            sqlite_dataclasses = [row_dataclass(*row_data) for row_data in sqlite_results]
            pgsql_dataclasses = [row_dataclass(**row_data) for row_data in pgsql_results]
            for sqlite_item, pgsql_item in zip(sqlite_dataclasses, pgsql_dataclasses):
                assert sqlite_item == pgsql_item
