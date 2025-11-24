-- Question 4 berapa jumlah Order dari transaksi di Table Order, berdasarkan kolom category dari tabel produk?
-- Gunakan Konsep CTE, Join table Order dengan table Product, 
-- Hitung total menggunakan aggregate SUM, dan grouping berdasarkan kolom Category dari table Product. 
-- Urutkan dari total transaksi tertinggi - terendah.
WITH order_product AS (
	SELECT
		o.total,
		p.category
	FROM 
		orders o
	JOIN 
		products p
		ON o.product_id = p.id
)
SELECT 
	sum(total) AS jumlah_total,
	category
FROM 
	order_product
GROUP BY category
ORDER BY jumlah_total DESC;