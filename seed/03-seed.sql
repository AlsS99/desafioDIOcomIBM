-- Seed deterministico: setseed(0.42) + modulos fixos nas chaves estrangeiras.
-- Contagens esperadas: 200 / 240 / 320 / 60 / 2000
-- (valores aleatorios podem variar entre versoes do Postgres)
SELECT setseed(0.42);

INSERT INTO customers (id, name, email, cpf)
SELECT 'cus-' || lpad(g::text, 4, '0'),
       'Cliente ' || g,
       'cliente' || g || '@exemplo.com',
       lpad((floor(random()*1e11))::bigint::text, 11, '0')
FROM generate_series(1, 200) g;

INSERT INTO accounts (id, customer_id, status, balance)
SELECT 'acc-' || lpad(g::text, 4, '0'),
       'cus-' || lpad((((g - 1) % 200) + 1)::text, 4, '0'),
       (ARRAY['ACTIVE','ACTIVE','ACTIVE','BLOCKED'])[1 + floor(random()*4)::int],
       round((random()*50000)::numeric, 2)
FROM generate_series(1, 240) g;

INSERT INTO cards (id, account_id, card_number, status)
SELECT 'crd-' || lpad(g::text, 4, '0'),
       'acc-' || lpad((((g - 1) % 240) + 1)::text, 4, '0'),
       lpad((floor(random()*1e16))::bigint::text, 16, '0'),
       'ACTIVE'
FROM generate_series(1, 320) g;

INSERT INTO merchants (id, name, category, country)
SELECT 'mer-' || lpad(g::text, 3, '0'),
       'Comerciante ' || g,
       (ARRAY['FOOD','TRAVEL','RETAIL','FUEL','GAMING'])[1 + floor(random()*5)::int],
       (ARRAY['BR','US','AR','PT'])[1 + floor(random()*4)::int]
FROM generate_series(1, 60) g;

INSERT INTO transactions (id, account_id, card_id, merchant_id, amount, created_at)
SELECT 'trx-' || lpad(g::text, 5, '0'),
       c.account_id,
       c.id,
       'mer-' || lpad((1 + ((g * 7) % 60))::text, 3, '0'),
       round((5 + random()*995)::numeric, 2),
       now() - (random() * interval '7 days')
FROM generate_series(1, 2000) g
JOIN cards c ON c.id = 'crd-' || lpad((1 + ((g * 13) % 320))::text, 4, '0');

-- Conferencia
SELECT 'customers' t, count(*) FROM customers
UNION ALL SELECT 'accounts', count(*) FROM accounts
UNION ALL SELECT 'cards', count(*) FROM cards
UNION ALL SELECT 'merchants', count(*) FROM merchants
UNION ALL SELECT 'transactions', count(*) FROM transactions;
