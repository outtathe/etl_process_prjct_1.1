import psycopg2
import pandas as pd

# Параметры подключения к базе данных
DB_TYPE = 'postgresql'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Путь к файлу с SQL-скриптом для создания функции dm.writelog
create_writelog_file_path = "../../task_1.2/for_postgresql/procedure_writeog.sql"

# Путь к файлу с SQL-скриптом для заполнения витрины оборотов
fill_account_turnover_file_path = "../../task_1.2/for_postgresql/procedure_fill_account_turnover_f.sql"

# Путь к файлу с SQL-скриптом для заполнения витрины 101
fill_f101_round_f_path = "../../task_1.2/for_postgresql/procedure_fill_f101_round_f.sql"
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

def execute_sql_script(sql_file_path, connection):
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()
    cursor = connection.cursor()

    try:
        cursor.execute(sql_script)
        connection.commit()
    except Exception as e:
        print(f"Ошибка при выполнении SQL-скрипта: {e}")
        connection.rollback()
    finally:
        cursor.close()
def create_procedures():
    # Установка соединения с базой данных
    connection = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    # Создание процедур
    execute_sql_script(create_writelog_file_path, connection)
    execute_sql_script(fill_account_turnover_file_path, connection)
    execute_sql_script(fill_f101_round_f_path, connection)

    connection.close()
if __name__ == "__main__":
    check_connection()
    create_procedures()
    # Установка соединения с базой данных
    connection = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    execute_sql_script(create_writelog_file_path, connection)

    days_of_january_2018 = pd.date_range(start='2018-01-01', end='2018-01-31')
    for day in days_of_january_2018:
        execute_sql_script(fill_account_turnover_file_path, connection)
        print(f"SQL-скрипт выполнен для {day}")
    connection.close()

    print("Все SQL-скрипты выполнены успешно.")
