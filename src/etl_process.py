import pandas as pd
import numpy as np
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
def load_data_with_logging(csv_files):
    for csv_file in csv_files:
        table_name = csv_file.split('/')[-1].split('.')[0].upper()
        table_name = table_name.lower()
        df = pd.read_csv(csv_file, sep=';', encoding='cp866')
        print(table_name)
        print("Data from CSV file:")
        print(df)

        try:
            # Запись данных в таблицу в схеме DS
            df.to_sql(table_name, engine, schema='ds', if_exists='replace', index=False)
            log = pd.DataFrame({
                'log_time': [str(datetime.datetime.now()) for i in range(len(df))],
                'name_of_table': [table_name for i in range(len(df))],
                'log_data': df.apply(lambda x: json.dumps({i: j for i, j in x.astype(object).replace(np.nan, None).items()}), axis=1),
                'status': ['success' for i in range(len(df))]
            })
            log.to_sql('logs', engine, schema='logs', if_exists='append', index=False)
        except Exception as e:
            # Логирование ошибки загрузки в таблицу logs.logs_table
            log_data = {
                'log_time': str(datetime.datetime.now()),
                'name_of_table':str(table_name),
                'log_data': str(e),
                'status': 'fail'
            }
            log_entry = pd.DataFrame([log_data])
            log_entry.to_sql('logs', engine, schema='logs', if_exists='append', index=False)

if __name__ == "__main__":
    #Создание схемы logs и таблицы logs.logs_table, если их нет
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE SCHEMA IF NOT EXISTS logs;
        '''))

        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS logs.logs (
                log_id SERIAL PRIMARY KEY,
                log_time TIMESTAMP,
                name_of_table VARCHAR(30),
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
    # Проверка соединения
    check_connection()
    # Загрузка данных и логирование
    load_data_with_logging(csv_files)