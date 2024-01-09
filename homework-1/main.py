"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

# соединениие с базой данных
conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='123456'
)

try:
    with conn:
        with conn.cursor() as cur:
            with open("north_data/employees_data.csv", 'r', newline='') as file:
                employees = csv.reader(file)
                next(employees)
                for employee in employees:
                    cur.execute(
                        "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes)"
                        " VALUES (%s,%s, %s, %s, %s, %s)",
                        employee)

        with conn.cursor() as cur:
            with open("north_data/customers_data.csv", 'r', newline='') as file:
                customers = csv.reader(file)
                next(customers)
                for customer in customers:
                    cur.execute(
                        "INSERT INTO customers (customer_id, company_name, contact_name)"
                        " VALUES (%s,%s, %s)",
                        customer)

        with conn.cursor() as cur:
            with open("north_data/orders_data.csv", 'r', newline='') as file:
                orders = csv.reader(file)
                next(orders)
                for order in orders:
                    cur.execute(
                        "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)"
                        " VALUES (%s, %s, %s, %s, %s)",
                        order)

finally:
    conn.close()