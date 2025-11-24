-- Question 8 Hitung total user di table Users yang memiliki email dari gmail.com.
-- Rename kolom alias total_user_gmail.
WITH user_gmail AS (
	SELECT
		email
	FROM 
		users
	WHERE
		email LIKE '%@gmail.com%'
)
SELECT
	count(DISTINCT email) AS total_user_gmail
FROM 
	user_gmail