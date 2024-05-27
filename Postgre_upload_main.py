import os
import bz2
import shutil
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import argparse

def print_with_time(message):
    current_time = datetime.now().strftime("[%H:%M:%S]")
    print(f"{current_time} {message}")

def create_connection():
    try:
        # Параметри підключення
        connection = psycopg2.connect(
            dbname="csv_test",
            user="postgres",
            password="123",
            host="localhost",
            port="5432"
        )
        print_with_time("Підключення до PostgreSQL успішне!")
        return connection
    except OperationalError as e:
        print_with_time(f"Помилка підключення: {e}")
        return None

def decompress_files(input_directory, output_directory):
    # Створюємо вихідний каталог, якщо він не існує
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for filename in os.listdir(input_directory):
        if filename.endswith(".bz2"):
            input_file_path = os.path.join(input_directory, filename)
            output_file_path = os.path.join(output_directory, filename[:-4])  # Видаляємо .bz2 розширення
            
            with bz2.open(input_file_path, 'rb') as file:
                with open(output_file_path, 'wb') as out_file:
                    shutil.copyfileobj(file, out_file)
            
            print_with_time(f"Розпаковано: {output_file_path}")

def load_csv_to_postgresql(engine, csv_file_path, table_name):
    try:
        # Завантажуємо CSV файл у DataFrame
        df = pd.read_csv(csv_file_path)
        print_with_time(f"CSV файл {csv_file_path} завантажено у DataFrame")

        # Замінюємо ':' та '-' на '_' у назвах стовпців
        df.columns = df.columns.str.replace(':', '_').str.replace('-', '_')
        print_with_time(f"Назви стовпців змінено для {csv_file_path}")

        # Завантажуємо DataFrame до PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
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
    
    # Розпаковуємо файли
    decompress_files(input_directory, output_directory)
    
    # Підключаємось до бази даних
    connection = create_connection()
    if connection is not None:
        # Створюємо engine для SQLAlchemy
        engine = create_engine('postgresql+psycopg2://postgres:123@localhost:5432/csv_test')

        # Завантажуємо розпаковані файли до бази даних
        for filename in os.listdir(output_directory):
            if filename.endswith(".csv"):
                csv_file_path = os.path.join(output_directory, filename)
                table_name = filename[:-4]  # Видаляємо .csv розширення
                print_with_time(f"Завантажуємо дані з {csv_file_path} до таблиці {table_name}")
                load_csv_to_postgresql(engine, csv_file_path, table_name)
        
        # Закриваємо з'єднання з базою даних
        connection.close()
