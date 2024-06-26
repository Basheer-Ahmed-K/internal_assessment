-- Q1: Retrieve the duplicates based on PK in the below table 
SELECT customer_id, product_id, sale_date
FROM customers_sales
GROUP BY customer_id, product_id, sale_date
HAVING COUNT(*) > 1;

-- Q2: Find the highest-paid employee in Each Department
SELECT department_name,MAX(salary)
FROM employee_tbl e
INNER JOIN department_tbl d on e.department_id = d.department_id
GROUP BY d.department_name

-- Q3: Find the Total Revenue Generated Each Month
SELECT *, MONTH(order_date) as month, SUM(order_total)
FROM orders_tbl
GROUP BY month

