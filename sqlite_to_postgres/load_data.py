import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass, astuple, fields
from typing import Generator, Any, Union, Tuple
from uuid import UUID
from datetime import datetime, date

import psycopg
from psycopg import ClientCursor, connection as pg_connection
from psycopg.rows import dict_row
from psycopg import errors as pg_errors

import logging

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

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


# Словарь, наименование таблицы для переноса данных и датакласс с описанием строки
# Таблицы должны быть заранее упорядоченны с учётом зависимостей
tables_for_load = {
    'film_work': FilmWork,
    'person': Person,
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    'person_film_work': PersonFilmWork,
}


class PostgresSaver:
    _connection = None

    def __init__(self, connection: pg_connection):
        self._connection = connection

    def save_data(self, batch: Tuple[Any, ...], table_name: str, row_class: dataclass):
        columns = [x.name for x in fields(row_class)]
        column_list = ', '.join(columns)
        tmp_list = ', '.join(['%s'] * len(columns))
        query = f'INSERT INTO content.{table_name} ({column_list}) VALUES ({tmp_list}) ON CONFLICT (id) DO NOTHING'
        batch_as_tuples = [astuple(data_row) for data_row in batch]
        try:
            with closing(self._connection.cursor(row_factory=dict_row)) as _cursor:
                _cursor.executemany(query, batch_as_tuples)
        except pg_errors.Error as e:
            logger.error(f'Ошибка записи в Postgres таблицу {table_name}: {e}')
            raise


class SQLiteLoader:
    _connection = None

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def extract_data(self, sqlite_cursor: sqlite3.Cursor, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
        try:
            sqlite_cursor.execute(f'SELECT * FROM {table_name}')
            while results := sqlite_cursor.fetchmany(BATCH_SIZE):
                yield results
        except sqlite3.Error as e:
            logger.error(f'Ошибка чтения из SQLite таблицы {table_name}: {e}')
            raise

    def transform_data(self, table_name: str, row_class: dataclass) -> Generator[list[Union[FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork]], None, None]:
        try:
            with closing(self._connection.cursor()) as _cursor:
                for batch in self.extract_data(_cursor, table_name):
                    yield [row_class(*row_data) for row_data in batch]
        except Exception as e:
            logger.error(f'Ошибка преобразования данных для таблицы {table_name}: {e}')
            raise


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: pg_connection, tables_for_load: dict[str, Union[FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork]]):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(sqlite_conn)

    for table_name, table_class in tables_for_load.items():
        try:
            logger.info(f'Перенос данных таблицы {table_name}')
            for batch in sqlite_loader.transform_data(table_name, table_class):
                postgres_saver.save_data(batch, table_name, table_class)
        except Exception as e:
            logger.error(f'Ошибка при переносе таблицы {table_name}: {e}')
            raise


if __name__ == '__main__':
    dsl = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
    }

    try:
        with closing(sqlite3.connect('db.sqlite')) as sqlite_conn, closing(psycopg.connect(
            **dsl, row_factory=dict_row, cursor_factory=ClientCursor
        )) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn, tables_for_load)
            pg_conn.commit()
            logger.info('🎉 Данные успешно перенесены !!!')
    except sqlite3.Error as e:
        logger.error(f'Ошибка подключения к SQLite: {e}')
    except pg_errors.Error as e:
        logger.error(f'Ошибка подключения к Postgres: {e}')
    except Exception as e:
        logger.error(f'Неизвестная ошибка: {e}')
