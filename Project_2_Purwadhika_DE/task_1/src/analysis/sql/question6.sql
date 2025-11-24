-- Question 6 list reviews berdasarkan ketagori produk = ‘Doohickey’, dimana rating <= 3 dan urutkan berdasarkan created_at table review
-- Gunakan Konsep CTE, Join table Reviews dengan table Product, select kolom
-- created_at, body dan rating dari table reviews, dan filter product category dan
-- rating reviews <= 3. Urutkan dari kolom created terbaru - terlama.
WITH review_products AS (
	SELECT
		p.category,
		r.created_at,
		r.body,
		r.rating
	FROM 
		products p
	JOIN 
		reviews r
	ON
		p.id = r.product_id 
)
SELECT 
	category,
	created_at,
	body,
	rating
FROM
	review_products
WHERE
	category= 'Doohickey' AND
	rating <=3
ORDER BY created_at DESC;