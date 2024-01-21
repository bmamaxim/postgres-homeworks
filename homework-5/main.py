import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE {db_name}')
    finally:
        conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    try:
        with open(script_file, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            cur.execute(sql_script)
    finally:
        cur.close()


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    try:
        cur.execute("""CREATE TABLE suppliers
                (
                    company_name varchar NOT NULL,
                    contact varchar,
                    address varchar,
                    phone varchar,
                    fax varchar,
                    homepage varchar,
                    products varchar
                )""")
    finally:
        cur.close()


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    try:
        with next(suppliers):
            for supplier in suppliers:
                cur.execute(
                    "INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products)"
                    " VALUES (%s,%s, %s, %s, %s, %s, %)",
                    supplier)
    finally:
        cur.close()


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("""ALTER TABLE ONLY products
                ADD CONSTRAINT fk_products_products FOREIGN KEY (supplier_id) REFERENCES products"""
                )
    cur.close()


if __name__ == '__main__':
    main()
