import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import json
import datetime
import psycopg2

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
            df.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)
            # # Update strategy: Use 'append' for FT_BALANCE_F to preserve history, and 'replace' for others
            # if table_name == 'ds.ft_balance_f':
            #     df.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)
            # else:
            #     df.to_sql(table_name, engine, schema='ds', if_exists='replace', index=False)

            log = pd.DataFrame({
                'log_time': [str(datetime.datetime.now() - datetime.timedelta(seconds=5)) for _ in range(len(df))],
                'name_of_table': [table_name for _ in range(len(df))],
                'log_data': df.apply(lambda x: json.dumps({i: j for i, j in x.astype(object).replace(np.nan, None).items()}), axis=1),
                'status': ['success' for _ in range(len(df))]
            })
            log.to_sql('logs', engine, schema='logs', if_exists='append', index=False)
        except Exception as e:
            log_data = {
                'log_time': str(datetime.datetime.now()),
                'name_of_table': str(table_name),
                'log_data': json.dumps(str(e)),
                'status': 'fail'
            }
            log_entry = pd.DataFrame([log_data])
            log_entry.to_sql('logs', engine, schema='logs', if_exists='append', index=False)



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