import contextlib
import sqlite3
import logging.config
import psycopg2
from psycopg2 import Error
import psycopg2.extras
from config import user, password, host, port, database

# profile Decorator
import time
from functools import wraps
from memory_profiler import memory_usage

# profile Decorator

# Set up logging
logging.config.fileConfig("logging.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)


# profile Decorator
# https://hakibenita.com/fast-load-data-python-postgresql
def profile(func):
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        global profile_list
        # start measure time
        t0 = time.perf_counter()

        # Measure memory
        mem, out = memory_usage((func, args, kwargs), retval=True,
                                timeout=200, interval=1e-7)
        elapsed = time.perf_counter() - t0

        # Func name and string with arguments
        func_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        about_func = f'{func.__name__}({func_kwargs_str})'
        logger.debug(f'{about_func}')

        func_mem = f'Time   {elapsed:0.4} | Memory {max(mem) - min(mem)}'
        logger.debug(f'{func_mem}')
        profile_list.append((about_func, func_mem))
        return out

    return wrapped_func


# profile Decorator

def get_connection():
    # Connection to local PostgreSQL DB
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    connection.autocommit = True
    logger.info('PostgreSQL connected!')
    return connection


def create_table_kvartiry_vtorichka(cursor):
    sql_create_table_kvartiry_vtorichka = '''
                            CREATE TABLE IF NOT EXISTS kvartiry_vtorichka (
                            id SERIAL PRIMARY KEY, 
                            data_item_id BIGINT NOT NULL,
                            item_id VARCHAR(255) NOT NULL,
                            item_url VARCHAR(512) NOT NULL,
                            item_title VARCHAR(255),
                            item_type VARCHAR(255),
                            item_number_of_rooms INTEGER,
                            item_area REAL,
                            item_floor_house VARCHAR(255),
                            item_floor INTEGER,
                            item_floors_in_house INTEGER,
                            item_price INTEGER,
                            item_currency VARCHAR(255) NOT NULL,
                            item_address VARCHAR(512),
                            item_city VARCHAR(255),
                            property_type VARCHAR(255) NOT NULL,
                            item_date TIMESTAMP NOT NULL,
                            item_add_date TIMESTAMP NOT NULL 
                            );'''
    cursor.execute(sql_create_table_kvartiry_vtorichka)
    logger.info('Table kvartiry_vtorichka created!')


def create_table_kvartiry_novostroyka(cursor):
    sql_create_table_kvartiry_novostroyka = '''
                            CREATE TABLE IF NOT EXISTS kvartiry_novostroyka (
                            id SERIAL PRIMARY KEY, 
                            data_item_id BIGINT NOT NULL,
                            item_id VARCHAR(255) NOT NULL,
                            item_url VARCHAR(512) NOT NULL,
                            item_title VARCHAR(255),
                            item_type VARCHAR(255),
                            item_number_of_rooms INTEGER,
                            item_area REAL,
                            item_floor_house VARCHAR(255),
                            item_floor INTEGER,
                            item_floors_in_house INTEGER,
                            item_price INTEGER,
                            item_currency VARCHAR(255) NOT NULL,
                            item_development_name VARCHAR(255),
                            item_address VARCHAR(255),
                            item_city VARCHAR(255),
                            property_type VARCHAR(255) NOT NULL,
                            item_date TIMESTAMP NOT NULL,
                            item_add_date TIMESTAMP NOT NULL
                            );'''
    cursor.execute(sql_create_table_kvartiry_novostroyka)
    logger.info('Table kvartiry_novostroyka created!')


def create_table_doma_dachi_kottedzhi(cursor):
    sql_create_table_doma_dachi_kottedzhi = '''
                            CREATE TABLE IF NOT EXISTS doma_dachi_kottedzhi (
                            id SERIAL PRIMARY KEY, 
                            data_item_id BIGINT NOT NULL,
                            item_id VARCHAR(255) NOT NULL,
                            item_url VARCHAR(512),
                            item_title VARCHAR(255),
                            item_type VARCHAR(255),
                            item_area REAL,
                            item_land_area REAL,
                            item_price INTEGER,
                            item_currency VARCHAR(255) NOT NULL,
                            item_address VARCHAR(255),
                            item_city VARCHAR(255),
                            property_type VARCHAR(255) NOT NULL,
                            item_date TIMESTAMP NOT NULL,
                            item_add_date TIMESTAMP NOT NULL
                            );'''
    cursor.execute(sql_create_table_doma_dachi_kottedzhi)
    logger.info('Table doma_dachi_kottedzhi created!')


def drop_all_tables(cursor):
    sql_delete_all_tables = f'DROP TABLE IF EXISTS kvartiry_vtorichka, kvartiry_novostroyka, doma_dachi_kottedzhi'
    cursor.execute(sql_delete_all_tables)
    logger.debug(f'All of 3 tables are removed from DB!')


def drop_table(cursor, table):
    sql_delete_table = f'DROP TABLE IF EXISTS {table}'
    cursor.execute(sql_delete_table)
    logger.debug(f'Table {table} is removed from DB!')


@profile
def insert_one_by_one_kvartiry_vtorichka(connection, items):
    global row_kvartiry_vtorichka, table_list
    row_kvartiry_vtorichka = 0
    with connection.cursor() as cursor:
        for item in items:
            # logger.debug(f'item: {item}')
            row_kvartiry_vtorichka += 1
            logger.debug(f'row: {row_kvartiry_vtorichka}')
            cursor.execute("""
                           INSERT INTO kvartiry_vtorichka
                           (data_item_id, 
                            item_id, 
                            item_url, 
                            item_title,
                            item_type, 
                            item_number_of_rooms, 
                            item_area, 
                            item_floor_house, 
                            item_floor, 
                            item_floors_in_house, 
                            item_price, 
                            item_currency, 
                            item_address, 
                            item_city,
                            property_type,
                            item_date, 
                            item_add_date) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, item
                           )
    table_list.append(('kvartiry_vtorichka', row_kvartiry_vtorichka))

@profile
def insert_one_by_one_kvartiry_novostroyka(connection, items):
    global row_kvartiry_novostroyka, table_list
    row_kvartiry_novostroyka = 0
    with connection.cursor() as cursor:
        for item in items:
            # logger.debug(f'item: {item}')
            row_kvartiry_novostroyka += 1
            logger.debug(f'row: {row_kvartiry_novostroyka}')
            cursor.execute("""
                           INSERT INTO kvartiry_novostroyka
                           (data_item_id, 
                            item_id, 
                            item_url, 
                            item_title,
                            item_type, 
                            item_number_of_rooms, 
                            item_area, 
                            item_floor_house, 
                            item_floor, 
                            item_floors_in_house, 
                            item_price, 
                            item_currency,
                            item_development_name,
                            item_address, 
                            item_city,
                            property_type,
                            item_date, 
                            item_add_date) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, item
                           )
    table_list.append(('kvartiry_novostroyka', row_kvartiry_novostroyka))

@profile
def insert_one_by_one_doma_dachi_kottedzhi(connection, items):
    global row_doma_dachi_kottedzhi, table_list
    row_doma_dachi_kottedzhi = 0
    with connection.cursor() as cursor:
        for item in items:
            # logger.debug(f'item: {item}')
            row_doma_dachi_kottedzhi += 1
            logger.debug(f'row: {row_doma_dachi_kottedzhi}')
            cursor.execute("""
                           INSERT INTO doma_dachi_kottedzhi
                           (data_item_id, 
                            item_id, 
                            item_url, 
                            item_title,
                            item_type, 
                            item_area,
                            item_land_area, 
                            item_price, 
                            item_currency,
                            item_address, 
                            item_city,
                            property_type,
                            item_date, 
                            item_add_date) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, item
                           )
    table_list.append(('doma_dachi_kottedzhi', row_doma_dachi_kottedzhi))

@profile
def insert_execute_batch_kvartiry_vtorichka(connection, items):
    with connection.cursor() as cursor:
        psycopg2.extras.execute_batch(cursor,
                                      """
                                       INSERT INTO kvartiry_vtorichka
                                       (data_item_id, 
                                        item_id, 
                                        item_url, 
                                        item_title,
                                        item_type, 
                                        item_number_of_rooms, 
                                        item_area, 
                                        item_floor_house, 
                                        item_floor, 
                                        item_floors_in_house, 
                                        item_price, 
                                        item_currency, 
                                        item_address, 
                                        item_city,
                                        property_type,
                                        item_date, 
                                        item_add_date) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                        """, items
                                                      )


@profile
def insert_execute_batch_iterator_kvartiry_vtorichka(connection, items, page_size=10000):
    with connection.cursor() as cursor:
        iter_items = (item for item in items)
        psycopg2.extras.execute_batch(cursor,
                                      """
                                       INSERT INTO kvartiry_vtorichka
                                       (data_item_id, 
                                        item_id, 
                                        item_url, 
                                        item_title,
                                        item_type, 
                                        item_number_of_rooms, 
                                        item_area, 
                                        item_floor_house, 
                                        item_floor, 
                                        item_floors_in_house, 
                                        item_price, 
                                        item_currency, 
                                        item_address, 
                                        item_city,
                                        property_type,
                                        item_date, 
                                        item_add_date) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                        """, iter_items, page_size=page_size
                                                      )


###############################################################################
################################# SQLite ######################################
###############################################################################
def execute_sql_query_sqlite(sql, data=None):
    with contextlib.closing(sqlite3.connect(
            'D:\\Python_projects\\AvitoRU_SileniumWebScraping\\avito_database.sqlite3',
            detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES
    )
    ) as connection, connection, contextlib.closing(
        connection.cursor()) as cursor:
        if data is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, data)
        return cursor.fetchall()


def get_from_sqlite_kvartiry_vtorichka():
    sql = f'''
          SELECT 
                data_item_id, 
                item_id, 
                item_url, 
                item_title,
                item_type, 
                item_number_of_rooms, 
                item_area, 
                item_floor_house, 
                item_floor, 
                item_floors_in_house, 
                item_price, 
                item_currency, 
                item_address, 
                item_city,
                property_type,
                item_date, 
                item_add_date
          FROM kvartiry_vtorichka;
          '''
    all_items = execute_sql_query_sqlite(sql)
    logger.debug(f'Table kvartiry_vtorichka | Total rows: {len(all_items)}')
    return all_items


def get_from_sqlite_kvartiry_novostroyka():
    sql = f'''
          SELECT 
                data_item_id, 
                item_id, 
                item_url, 
                item_title,
                item_type, 
                item_number_of_rooms, 
                item_area, 
                item_floor_house, 
                item_floor, 
                item_floors_in_house, 
                item_price, 
                item_currency,
                item_development_name,
                item_address, 
                item_city,
                property_type,
                item_date, 
                item_add_date
          FROM kvartiry_novostroyka;
          '''
    all_items = execute_sql_query_sqlite(sql)
    logger.debug(f'Table kvartiry_novostroyka | Total rows: {len(all_items)}')
    return all_items


def get_from_sqlite_doma_dachi_kottedzhi():
    sql = f'''
          SELECT 
                data_item_id, 
                item_id, 
                item_url, 
                item_title,
                item_type, 
                item_area,
                item_land_area, 
                item_price, 
                item_currency,
                item_address, 
                item_city,
                property_type,
                item_date, 
                item_add_date
          FROM doma_dachi_kottedzhi;
          '''
    all_items = execute_sql_query_sqlite(sql)
    logger.debug(f'Table doma_dachi_kottedzhi | Total rows: {len(all_items)}')
    return all_items


if __name__ == '__main__':
    profile_list = []
    table_list = []
    row_kvartiry_vtorichka = 0
    row_kvartiry_novostroyka = 0
    row_doma_dachi_kottedzhi = 0

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            drop_table(cursor, 'kvartiry_vtorichka')
            create_table_kvartiry_vtorichka(cursor)

            drop_table(cursor, 'kvartiry_novostroyka')
            create_table_kvartiry_novostroyka(cursor)

            drop_table(cursor, 'doma_dachi_kottedzhi')
            create_table_doma_dachi_kottedzhi(cursor)

        sqlite_items = get_from_sqlite_kvartiry_vtorichka()
        insert_one_by_one_kvartiry_vtorichka(connection, sqlite_items)
        # insert_execute_batch_kvartiry_vtorichka(connection, sqlite_items)
        # insert_execute_batch_iterator_kvartiry_vtorichka(connection, sqlite_items)

        sqlite_items = get_from_sqlite_kvartiry_novostroyka()
        insert_one_by_one_kvartiry_novostroyka(connection, sqlite_items)

        sqlite_items = get_from_sqlite_doma_dachi_kottedzhi()
        insert_one_by_one_doma_dachi_kottedzhi(connection, sqlite_items)

        for table, details in zip(table_list, profile_list):
            logger.debug(f'Table {table[0]} total rows: {table[1]}')
            logger.debug(f'{details[0]} | {details[1]}')

    except (Exception, Error) as error:
        logger.debug("Error during work with PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.info("Connection with PostgreSQL closed!")
