-- Question 1: Berapa banyak transaksi di table orders?
-- Aggregate COUNT pada kolom Id.
SELECT count(DISTINCT id)
FROM orders;

