SET 'sql.tables.scan.idle-timeout' = '5s';

CREATE TABLE `desafio.transactions.enriched` AS
SELECT t.id AS transaction_id, t.account_id, t.card_id, t.merchant_id,
       t.amount, t.created_at, a.customer_id, a.status AS account_status
FROM `desafio.transactions.pk` t
JOIN `desafio.accounts.pk` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.id;