-- Question 3: Hitung 10 product yang sering memberikan discount di tabel Order berdasarkan produk Title dari table Products?
-- Join table Order dengan table produk untuk mendapatkan produk Title, aggragete Count untuk mendapatkan total order dan grouping berdasarkan
-- kolom Title dari Table Products. Urutkan dari total transaksi tertinggi - terendah.
-- Tamplikan 10 product teratas.
SELECT 
	p.title,
	count(o.id) AS total_order
FROM 
	orders o
JOIN 
	products p ON o.product_id = p.id
GROUP BY p.title
ORDER BY total_order DESC
LIMIT 10;
