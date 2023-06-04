# -*- coding: cp1251 -*-

import psycopg2
import logging
from psycopg2 import OperationalError

from src.times import get_time

# Конфигурация логгера
logging.basicConfig(level=logging.INFO)


# Функция для подключения к БД

def db_create_connection(database, user, password, host, port):
    db_connection = None
    try:
        db_connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        logging.info("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        logging.error(f"The error '{e}' occurred")
    return db_connection


# Функция выполнения запроса БД

def execute_query(connect, query):
    connect.autocommit = True
    cursor = connect.cursor()
    try:
        cursor.execute(query)
        logging.info("Query executed successfully")
    except OperationalError as e:
        logging.error(f"The error '{e}' occurred")


# Функция выполнения запроса к БД
def execute_read_query(connect, query):
    cursor = connect.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        logging.error(f"The error '{e}' occurred")


def insert_data(connect, id):
    # Отправка данных в БД
    cursor = connect.cursor()
    cursor.execute("INSERT INTO requests (id) SELECT generate_series(1," + str(id) + ");")
    connect.commit()
    cursor.close()
    logging.info("Data inserted successfully")


def select_data(connect):
    # Получение таблицы из БД
    select_records = "SELECT * FROM requests"
    records = execute_read_query(connect, select_records)

    for record in records:
        logging.info(record)


def delete_data(connection):
    # Удаление таблицы из БД
    delete_records = "DELETE FROM requests WHERE id>-1;"
    execute_query(connection, delete_records)
    logging.info("Data deleted successfully")
