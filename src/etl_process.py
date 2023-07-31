import pandas as pd
from sqlalchemy import create_engine, text
import json
import datetime
import psycopg2

# Задание констант для подключения к PostgreSQL
DB_TYPE = 'postgresql'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Создаем строку подключения к бд
connection_string = f"{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def check_connection():
    try:
        connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
        cursor = connection.cursor()
        cursor.execute('SELECT 1;')
        result = cursor.fetchone()
        print("Connection to the database is successful.")
        print("Result of the query:", result)
        cursor.close()
        connection.close()
    except Exception as e:
        print("Error:", e)
if __name__ == "__main__":
    # Создание схемы logs и таблицы logs.logs_table, если их нет
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE SCHEMA IF NOT EXISTS logs;
        '''))

        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS logs.logs_table (
                log_id SERIAL PRIMARY KEY,
                log_time TIMESTAMP,
                table_name VARCHAR(50),
                log_data JSONB,
                status VARCHAR(10)
            );
        '''))

    # Чтение данных из CSV-файлов
    csv_files = [
        '../task_1.1/md_ledger_account_s.csv',
        '../task_1.1/md_account_d.csv',
        '../task_1.1/ft_balance_f.csv',
        '../task_1.1/ft_posting_f.csv',
        '../task_1.1/md_currency_d.csv',
        '../task_1.1/md_exchange_rate_d.csv',
    ]
    print(csv_files)
    check_connection()
