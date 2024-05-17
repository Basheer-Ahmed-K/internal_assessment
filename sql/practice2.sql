-- Q1: Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).
SELECT w0.id 
FROM Weather w0, Weather w1
DATE_DIFF(recordDate) = 1 AND w0.temprature > w1.temprature

-- Q2: Write a solution to report the product_name, year, and price for each sale_id in the Sales table.

SELECT product_name, year, price
FROM sales s LEFT JOIN product p ON s.product_id = p.product_id