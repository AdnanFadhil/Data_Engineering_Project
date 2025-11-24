-- Question 10 Dapatkan list Name, Email, Address, Birtdate dari table Users yang lahir diatas tahun 1997?
-- Buatlah dalam format database views.
CREATE OR REPLACE VIEW users_1997 AS (
	SELECT 
		name,
		email,
		address,
		birth_date
	FROM 
		users
	WHERE
		birth_date >= '1997-01-01'
	ORDER BY
		name
) 