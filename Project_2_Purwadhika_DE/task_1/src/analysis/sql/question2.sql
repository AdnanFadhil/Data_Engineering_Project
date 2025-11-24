-- Question 2: Berapa jumlah Order dari seluruh transaksi di table orders?
-- Aggragate SUM pada kolom Total.
SELECT sum(total) AS jumlah_total
FROM orders;
