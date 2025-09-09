from psycopg import connection as pg_connection
from psycopg.rows import dict_row
from psycopg import errors as pg_errors
from typing import Any, Tuple
from contextlib import closing
from dataclasses import dataclass, astuple, fields
import logging


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
        except pg_errors.OperationalError as e:
            logging.error('Ошибка выполнения операции: %s', e)
            raise
        except pg_errors.ProgrammingError as e:
            logging.error('Ошибка в SQL-запросе: %s', e)
            raise
        except pg_errors.IntegrityError as e:
            logging.error('Нарушение целостности данных: %s', e)
            raise
        except pg_errors.DataError as e:
            logging.error('Ошибка данных: %s', e)
            raise
        except pg_errors.InternalError as e:
            logging.error('Внутренняя ошибка PostgreSQL: %s', e)
            raise
        except pg_errors.Error as e:
            logging.error('Ошибка записи в Postgres таблицу %s: %s', table_name, e)
            raise
