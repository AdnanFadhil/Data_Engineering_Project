-- Question 7 Berapa source di table Users?
-- Ambil unik data di kolom source dari table Users.
SELECT
	count(DISTINCT source)
FROM users;