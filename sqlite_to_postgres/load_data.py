import os
import sqlite3
from contextlib import closing

import psycopg
from psycopg import ClientCursor
from psycopg.rows import dict_row
from psycopg import errors as pg_errors

from data_loader import load_from_sqlite, tables_for_load

import logging

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)


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
    except PermissionError as e:
        logger.error('Ошибка доступа: %s', e)
    except sqlite3.Error as e:
        logger.error('Ошибка SQLite: %s', e)
    except psycopg.OperationalError as e:
        logger.error('Ошибка подключения к базе данных: %s', e)
        logger.error('Проверьте параметры подключения и доступность сервера')
    except psycopg.InterfaceError as e:
        logger.error('Ошибка интерфейса: %s', e)
        logger.error('Проверьте корректность параметров подключения')
    except pg_errors.Error as e:
        logger.error('Ошибка подключения к Postgres: %s', e)
    except Exception as e: # Для перехвата всех необработанных исключений
        logger.error('Неизвестная ошибка: %s', e)
