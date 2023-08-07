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
def insert_data_for_posting_ft(table_name, temp_table_name):
    conn = engine.connect().connection
    cur = conn.cursor()
    # Insert new rows
    if table_name == 'ft_balance_f':
        new_insert_query = f"""
            INSERT INTO ds.{table_name} (on_date, account_rk, currency_rk, balance_out)
            SELECT * FROM ds.{temp_table_name}
            ON CONFLICT DO NOTHING;
        """
    elif table_name == 'ft_posting_f':
        new_insert_query = f"""
            INSERT INTO ds.{table_name} (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
            SELECT * FROM ds.{temp_table_name}
            ON CONFLICT DO NOTHING;
        """
    cur.execute(new_insert_query)
    # Update existing rows
    if table_name == 'ft_balance_f':
        new_update_query = f"""UPDATE ds.ft_balance_f AS d
                SET currency_rk = s.currency_rk, balance_out = s.balance_out
                FROM ds.temp_ft_balance_f AS s
                WHERE s.on_date = d.on_date AND s.account_rk = d.account_rk;"""
    elif table_name == 'ft_posting_f':
        new_update_query = f"""
            UPDATE ds.{table_name} AS dst
            SET credit_amount = src.credit_amount,
                debet_amount = src.debet_amount
            FROM ds.{temp_table_name} AS src
            WHERE dst.oper_date = src.oper_date
                AND dst.credit_account_rk = src.credit_account_rk
                AND dst.debet_account_rk = src.debet_account_rk;
        """
    cur.execute(new_update_query)
    
    conn.commit()
    cur.close()
    conn.close()
def load_data_with_logging(csv_files):
    for csv_file in csv_files:
        table_name = csv_file.split('/')[-1].split('.')[0].upper()
        table_name = table_name.lower()
        df = pd.read_csv(csv_file, sep=';', encoding='cp866', index_col=0, header=0)
        df.columns = df.columns.str.lower()
        if table_name == 'md_ledger_account_s':
            target_column = 'pair_account'
            df[target_column] = df[target_column].apply(lambda x: str(x)[:5] if pd.notna(x) and isinstance(x, (int, float)) else None if pd.isna(x) else x).astype(object)
            # column_series = df['pair_account']
        print(table_name)
        print("Data from CSV file:")
        print(df)
        
        try:
            # 1. Создаем временную таблицу stg и вставляем данные из файла
            temp_table_name = f'temp_{table_name}'
            create_temp_table_with_same_structure(table_name, temp_table_name)
            if table_name == 'ft_balance_f':
                df['on_date'] = pd.to_datetime(df['on_date'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
            df.to_sql(temp_table_name, engine, schema='ds', if_exists = 'append', index = False)
            # 2. Обновляем существующие данные в таблице dst
            primary_keys = get_primary_key_columns(table_name)
            set_clause = ', '.join(f'{col} = s.{col}' for col in df.columns if col not in primary_keys)
            where_clause = ' AND '.join(f's.{col} = d.{col}' for col in primary_keys)
            update_query = text(f"""
                UPDATE ds.{table_name} AS d
                SET {set_clause}
                FROM ds.{temp_table_name} AS s
                WHERE {where_clause};
            """)
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
            if table_name == 'ft_posting_f' or table_name == 'ft_balance_f':
                insert_data_for_posting_ft(table_name, temp_table_name)
            else:
                insert_data.drop_duplicates(inplace=True)
                insert_data.to_sql(table_name, engine, schema='ds', if_exists='append', index=False)
            # 5. Логируем успех
            log = pd.DataFrame({
                'log_time': [str(datetime.datetime.now() - datetime.timedelta(seconds=5)) for _ in range(len(df))],
                'name_of_table': [table_name for _ in range(len(df))],
                'log_data': df.apply(lambda x: json.dumps(str({i: j for i, j in x.astype(object).replace(np.nan, None).items()})), axis=1),
                'status': ['success' for _ in range(len(df))]
            })
            log.to_sql('logs', engine, schema='logs', if_exists='append', index=False)
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
    csv_files = [
        '../../task_1.1/md_ledger_account_s.csv',
        '../../task_1.1/md_account_d.csv',
        '../../task_1.1/ft_balance_f.csv',
        '../../task_1.1/ft_posting_f.csv',
        '../../task_1.1/md_currency_d.csv',
        '../../task_1.1/md_exchange_rate_d.csv',
    ]
    new_data = [
        '../../task_1.1/ft_balance_f.csv',
    ]
    check_connection()
    # load_data_with_logging(csv_files)
    load_data_with_logging(new_data)