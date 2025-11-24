-- Question 5 berapa jumlah total Order di table Order, dari setiap title di table Product yang memiliki rating >=4?
-- Gunakan Konsep CTE, Join table Order dengan table Product, Hitung total menggunakan aggregate SUM, 
-- dan grouping berdasarkan kolom Title dari table Product. Urutkan dari total transaksi tertinggi - terendah.
WITH order_product AS (
	SELECT 
		p.title,
		p.rating,
		o.total
	FROM 
		orders o
	JOIN
		products p
		ON o.product_id = p.id
)
SELECT 
	title,
	rating,
	sum(total) AS jumlah_total
FROM 
	order_product
WHERE
	rating >=4
GROUP BY
	title,
	rating
ORDER BY 
	jumlah_total DESC;