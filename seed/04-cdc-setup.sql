-- Usuario do CDC com menor privilegio e publication.
-- ATENCAO: troque SUA_SENHA_FORTE por uma senha inventada NO EDITOR do Neon.
-- Nao versione a senha real; ela fica so no .env (PG_CDC_PASSWORD).
-- Se o CREATE ROLE ... REPLICATION for recusado, crie o papel em Roles -> New role
-- na interface do Neon e rode so os GRANT e o CREATE PUBLICATION.
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'SUA_SENHA_FORTE';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;
CREATE PUBLICATION desafio_pub FOR TABLE customers, accounts, cards, merchants, transactions;

-- Conferencia
SHOW wal_level;  -- esperado: logical
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'cdc_user';
SELECT pubname FROM pg_publication;
