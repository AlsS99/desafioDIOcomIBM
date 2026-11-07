SET 'sql.tables.scan.idle-timeout' = '5s';

CREATE TABLE `desafio.accounts.pk` (
  PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY (id) INTO 1 BUCKETS
AS SELECT id, customer_id, status, balance, created_at
FROM `desafio.public.accounts`;

CREATE TABLE `desafio.transactions.pk` (
  PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY (id) INTO 1 BUCKETS
AS SELECT id, account_id, card_id, merchant_id, amount, created_at
FROM `desafio.public.transactions`;