import sqlite3
from typing import Generator, Union
from dataclasses import dataclass
from contextlib import closing
from db_data_classes import FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork
import logging


class SQLiteLoader:
    _connection = None

    def __init__(self, connection: sqlite3.Connection, batch_size: int):
        self._connection = connection
        self._batch_size = batch_size

    def extract_data(self, sqlite_cursor: sqlite3.Cursor, table_name: str) -> Generator[list[sqlite3.Row], None, None]:
        try:
            sqlite_cursor.execute(f'SELECT * FROM {table_name}')
            while results := sqlite_cursor.fetchmany(self._batch_size):
                yield results
        except sqlite3.OperationalError as e:
            logging.error('Ошибка выполнения операции: %s', e)
            raise
        except sqlite3.ProgrammingError as e:
            logging.error('Ошибка в SQL-запросе: %s', e)
            raise
        except sqlite3.Error as e:
            logging.error('Ошибка чтения из SQLite таблицы %s: %s', table_name, e)
            raise

    def transform_data(self, table_name: str, row_class: dataclass) -> Generator[list[Union[FilmWork, Person, Genre, GenreFilmWork, PersonFilmWork]], None, None]:
        with closing(self._connection.cursor()) as _cursor:
            for batch in self.extract_data(_cursor, table_name):
                yield [row_class(*row_data) for row_data in batch]
