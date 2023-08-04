import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table
import json
import datetime
import psycopg2
import pg8000

DB_TYPE = 'postgresql'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

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

def get_primary_key_columns(table_name):
    primary_key_dict = {
    'ft_balance_f': ['on_date', 'account_rk'],
    'ft_posting_f': ['oper_date', 'credit_account_rk', 'debet_account_rk'],
    'md_account_d': ['data_actual_date', 'account_rk'],
    'md_currency_d': ['currency_rk', 'data_actual_date'],
    'md_exchange_rate_d': ['data_actual_date', 'currency_rk'],
    'md_ledger_account_s': ['ledger_account', 'start_date'],
    }
    return primary_key_dict.get(table_name, [])
def create_temp_table_with_same_structure(table_name, temp_table_name):
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE ds.{temp_table_name} AS SELECT * FROM ds.{table_name} WHERE false;")
    conn.commit()
    cursor.close()
    conn.close()
def delete_temp_table(temp_table_name):
    conn = engine.connect().connection
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE ds.{temp_table_name}')
    conn.commit()
    cursor.close()
    conn.close()
def execute_query(query, data=None):
    conn = engine.connect().connection
    cur = conn.cursor()
    if data is not None:
        cur.executemany(query, data)
    else:
        cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
def load_data_with_logging(csv_files):
    for csv_file in csv_files:
        table_name = csv_file.split('/')[-1].split('.')[0].upper()
        table_name = table_name.lower()
        df = pd.read_csv(csv_file, sep=';', encoding='cp866', index_col=0, header=0)
        df.columns = df.columns.str.lower()

        print(table_name)
        print("Data from CSV file:")
        print(df)

        try:
            # 1. Создаем временную таблицу stg и вставляем данные из файла
            temp_table_name = f'temp_{table_name}'
            create_temp_table_with_same_structure(table_name, temp_table_name)
            # 2. Обновляем существующие данные в таблице dst
            primary_keys = get_primary_key_columns(table_name)
            set_clause = ', '.join(f'{col} = s.{col}' for col in df.columns if col not in primary_keys)
            where_clause = ' AND '.join(f's.{col} = d.{col}' for col in primary_keys)
            print(where_clause)
            update_query = text(f"""
                UPDATE ds.{table_name} AS d
                SET {set_clause}
                FROM ds.{temp_table_name} AS s
                WHERE {where_clause};
            """)
            print(update_query)
            with engine.connect() as conn:
                conn.execute(update_query)
                conn.close()
            # 3. Выводим данные, которые можно добавить
            primary_keys = get_primary_key_columns(table_name)
            # Use the primary key columns in the NOT IN clause
            select_query = f"""
            SELECT *
            FROM ds.{temp_table_name} AS s
            WHERE NOT EXISTS (
                SELECT 1 FROM ds.{table_name} AS d
                WHERE { ' AND '.join(f's.{col} = d.{col}' for col in primary_keys) }
                );
            """
            insert_data = pd.read_sql(select_query, engine)
            # 4. Добавляем данные в таблицу dst
            insert_data.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)

            # 5. Логируем успех
            log_data = df.apply(lambda x: {i: j for i, j in x.astype(object).replace(pd.NA, None).items()}, axis=1)
            # log_data = str(log_data)
            log_data = log_data.apply(json.dumps)
            log = pd.DataFrame({
                'log_time': [str(datetime.datetime.now() - datetime.timedelta(seconds=5)) for _ in range(len(df))],
                'name_of_table': [table_name for _ in range(len(df))],
                'log_data': log_data,
                'status': ['success' for _ in range(len(df))]
            })
            insert_log_query = f"INSERT INTO logs.logs VALUES ({','.join(['%s' for _ in range(len(log.columns))])})"
            print("Вот билиберда! ", insert_log_query)
            execute_query(insert_log_query, [tuple(row) for row in log.values])
            print(log.log_data)
            # log.to_sql('logs', engine, schema='logs', if_exists='append', index=False, dtype={'log_data': 'JSONB'})

            # 6. Удаляем временную таблицу
            delete_temp_table(temp_table_name)

        except Exception as e:
            log_data = {
                'log_time': str(datetime.datetime.now()),
                'name_of_table': str(table_name),
                'log_data': json.dumps(str(e)),
                'status': 'fail'
            }
            print(log_data)
            log_entry = pd.DataFrame([log_data])
            log_entry.to_sql('logs', engine, schema='logs', if_exists='append', index=False)
            delete_temp_table(temp_table_name)

if __name__ == "__main__":
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

    # '../task_1.1/md_ledger_account_s.csv', работает
    #     '../task_1.1/md_account_d.csv', работает
    #     '../task_1.1/ft_balance_f.csv', работает
    #     '../task_1.1/ft_posting_f.csv', работает
    #     '../task_1.1/md_currency_d.csv',
    #     '../task_1.1/md_exchange_rate_d.csv',
    csv_files = [
        '../task_1.1/md_ledger_account_s.csv',
        '../task_1.1/md_account_d.csv',
        '../task_1.1/ft_balance_f.csv',
        '../task_1.1/ft_posting_f.csv',
        '../task_1.1/md_currency_d.csv',
        '../task_1.1/md_exchange_rate_d.csv',
    ]
    csv_test_files = [
        '../task_1.1/md_currency_d.csv',
    ]
    check_connection()
    load_data_with_logging(csv_test_files)