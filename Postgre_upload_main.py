import os
import bz2
import shutil
import psycopg2
from psycopg2 import OperationalError, sql
from sqlalchemy import create_engine
from datetime import datetime
import argparse
import csv

def print_with_time(message):
    current_time = datetime.now().strftime("[%H:%M:%S]")
    print(f"{current_time} {message}")

def create_connection():
    try:
        # Параметри підключення
        connection = psycopg2.connect(
            dbname="dbname",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )
        print_with_time("Підключення до PostgreSQL успішне!")
        return connection
    except OperationalError as e:
        print_with_time(f"Помилка підключення: {e}")
        return None

def decompress_file(input_file_path, output_file_path, chunk_size=1024*1024):
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with bz2.open(input_file_path, 'rb') as file:
        with open(output_file_path, 'wb') as out_file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                out_file.write(chunk)
    print_with_time(f"Розпаковано: {output_file_path}")

def load_csv_to_postgresql(engine, csv_file_path, table_name):
    try:
        # Відкриваємо з'єднання з базою даних
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Створюємо таблицю якщо її не існує
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = next(reader)  # Перша строка - назви колонок
            columns = [col.replace(':', '_').replace('-', '_') for col in columns]
            create_table_query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(
                    sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns
                )
            )
            cursor.execute(create_table_query)
            conn.commit()
        
        # Пакетне завантаження даних
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Пропускаємо заголовок
            batch_size = 100000  # Розмір пакету
            batch = []
            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            for i, row in enumerate(reader, start=1):
                batch.append(row)
                if i % batch_size == 0:
                    cursor.executemany(insert_query, batch)
                    batch = []
                    print_with_time(f"{i} рядків завантажено до {table_name}")
            if batch:
                cursor.executemany(insert_query, batch)
                print_with_time(f"{i} рядків завантажено до {table_name}")
        
        # Закриваємо з'єднання з базою даних
        conn.commit()
        cursor.close()
        conn.close()
        print_with_time(f"Дані з {csv_file_path} успішно завантажені до таблиці {table_name}")
    except Exception as e:
        print_with_time(f"Помилка завантаження {csv_file_path} до таблиці {table_name}: {e}")

if __name__ == "__main__":
    # Парсимо аргументи командного рядка
    parser = argparse.ArgumentParser(description='Завантаження CSV файлів до PostgreSQL')
    parser.add_argument('-d', '--directory', type=str, required=True, help='Каталог із архівами')
    args = parser.parse_args()

    input_directory = args.directory
    output_directory = 'unzipped'
    
    # Створюємо вихідний каталог, якщо він не існує
    os.makedirs(output_directory, exist_ok=True)
    
    # Підключаємось до бази даних
    connection = create_connection()
    if connection is not None:
        # Створюємо engine для SQLAlchemy
        engine = create_engine('postgresql+psycopg2://postgres:123@localhost:5432/csv_test')

        # Розпаковуємо та завантажуємо файли до бази даних
        for filename in os.listdir(input_directory):
            if filename.endswith(".bz2"):
                input_file_path = os.path.join(input_directory, filename)
                output_file_path = os.path.join(output_directory, filename[:-4])  # Видаляємо .bz2 розширення
                
                decompress_file(input_file_path, output_file_path)
                
                table_name = filename[:-8]  # Видаляємо .csv.bz2 розширення
                print_with_time(f"Завантажуємо дані з {output_file_path} до таблиці {table_name}")
                load_csv_to_postgresql(engine, output_file_path, table_name)
        
        # Закриваємо з'єднання з базою даних
        connection.close()
