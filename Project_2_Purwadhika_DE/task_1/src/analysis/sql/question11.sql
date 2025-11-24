-- Question 11 Dapatkan list id, created_at, title, category, dan vendor dari table products, yang
-- memiliki title yang sama / title muncul dengan value yang sama sebanyak lebih
-- dari 1 kali.
-- Gunakan format cte dan row_number dalam mengerjakan soal berikut.
WITH product_rn AS (
    SELECT
        id,
        title,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY title ORDER BY created_at) AS rn,
        COUNT(*) OVER (PARTITION BY title) AS total_per_title
    FROM products
)
SELECT
	id,
	title,
	created_at,
	total_per_title
FROM product_rn
WHERE total_per_title > 1
  AND rn = 1
ORDER BY title;