-- 1,000 synthetic accounts, currencies rotate GBP / EUR / USD, 100,000.00 opening balance each.
INSERT INTO accounts (account_id, currency, balance)
SELECT 'ACC-' || lpad(n::text, 6, '0'),
       (ARRAY['GBP', 'EUR', 'USD'])[1 + n % 3],
       100000.00
FROM generate_series(1, 1000) AS n;
