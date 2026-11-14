-- Camada 3: tabelas do dominio de pagamentos (schema public)
CREATE TABLE customers (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT NOT NULL,
  cpf         TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
  id          TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL REFERENCES customers(id),
  status      TEXT NOT NULL,
  balance     NUMERIC(15,2) NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cards (
  id          TEXT PRIMARY KEY,
  account_id  TEXT NOT NULL REFERENCES accounts(id),
  card_number TEXT NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  category    TEXT NOT NULL,
  country     TEXT NOT NULL
);

CREATE TABLE transactions (
  id          TEXT PRIMARY KEY,
  account_id  TEXT NOT NULL REFERENCES accounts(id),
  card_id     TEXT NOT NULL REFERENCES cards(id),
  merchant_id TEXT NOT NULL REFERENCES merchants(id),
  amount      NUMERIC(15,2) NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
