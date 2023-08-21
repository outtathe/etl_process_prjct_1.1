import psycopg2
import csv

DB_TYPE = 'postgresql'
DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

target_date = '2018-01-15'

connection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = connection.cursor()

cursor.callproc('ds.get_credit_debit_info', (target_date,))
result = cursor.fetchall()

cursor.close()
connection.close()

csv_file_path = 'credit_debit_info.csv'

with open(csv_file_path, 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['oper_date', 'max_credit_amount', 'min_credit_amount', 'max_debit_amount', 'min_debit_amount'])
    csv_writer.writerows(result)

print(f"Результат сохранен в файл: {csv_file_path}")
