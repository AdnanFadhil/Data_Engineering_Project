-- Question 9 list id, title, price, and created at dari table Products, dengan kriteria
-- price antara 30 - 50, dan urutkan dari transaksi terbaru - terlama?
SELECT
	p.id AS product_id,
	p.title,
	p.price,
	p.created_at AS product_created_at,
	o.created_at  AS order_created_at
FROM
	products p
JOIN
	orders o
ON 
	p.id = o.product_id
WHERE
	p.price >= 30 AND 
	p.price <= 50
ORDER BY
	order_created_at DESC;