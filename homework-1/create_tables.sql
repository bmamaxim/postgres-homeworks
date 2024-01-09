-- SQL-команды для создания таблиц

CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name text,
	last_name text,
	title text,
	birth_date date,
	notes text
);

CREATE TABLE customers
(
	customer_id text PRIMARY KEY,
	company_name text,
	contact_name text
);

CREATE TABLE orders
(
	order_id int,
	customer_id text references customers(customer_id),
	employee_id int references employees(employee_id),
	order_date date,
	ship_city text
)
