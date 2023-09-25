import psycopg2
import pandas as pd
import csv

# Параметры подключения к базе данных
DB_TYPE = 'postgresql'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Название таблицы и имя выходного CSV-файла
TABLE_NAME = 'dm.dm_f101_round_f'
COPY_TABLE_NAME = 'dm.dm_f101_round_f_v2'
CSV_FILE = 'output.csv'

# Функция для выполнения запроса к базе данных и выгрузки данных в CSV
def export_data_to_csv(connection, cursor):
    query = f"SELECT * FROM {TABLE_NAME}"
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]

    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([''] + column_names)  # Пустая ячейка для номеров строк
        row_number = 1

        for row in cursor:
            csv_writer.writerow([row_number] + list(row))
            row_number += 1
def create_copy_table(connection, cursor):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {COPY_TABLE_NAME} (LIKE {TABLE_NAME} INCLUDING ALL)")
        connection.commit()
        print(f"Создана копия таблицы {COPY_TABLE_NAME}.")

    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        print("Ошибка при создании копии таблицы:", error)
def import_data_from_csv(connection, cursor):
    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # Пропуск первой строки с заголовками

            for row in csv_reader:
                row_number = int(row[0])
                values = [None if value == '' else value for value in row[1:]]

                placeholders = ', '.join(['%s'] * len(values))
                insert_query = f"INSERT INTO {COPY_TABLE_NAME} VALUES ({placeholders})"
                cursor.execute(insert_query, values)

                print(f"Строка {row_number} успешно добавлена в таблицу.")

            connection.commit()
            print("Данные успешно загружены в таблицу.")

    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        print("Ошибка при загрузке данных:", error)

if __name__ == '__main__':
    # Подключение к базе данных
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        cursor = connection.cursor()

        # export_data_to_csv(connection, cursor)
        create_copy_table(connection, cursor)
        import_data_from_csv(connection, cursor)

        cursor.close()
        connection.close()

        print("Данные успешно выгружены в CSV-файл.")

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с базой данных:", error)
