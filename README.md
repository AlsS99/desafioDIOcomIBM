# desafio-pipeline-pagamentos

Pipeline de pagamentos em tempo real: **Neon (Postgres) → CDC (Debezium) → tópicos Avro → Flink SQL (enriquecimento + fraude) → consumo**, tudo no Confluent Cloud (AWS us-east-1, cluster Basic).


## Estrutura

```
.env.example   setup.sh   teardown.sh
schemas/       connectors/cdc.json   seed/   sql/   metrics/   evidencias/
```

## Desvios declarados em relação ao modelo

1. **Seed próprio.** Usei DDL e seed escritos por mim (`seed/`, `setseed(0.42)` + módulos fixos), no schema `public`, com as mesmas contagens do modelo (200/240/320/60/2000). Não usei o dataset fornecido.
2. **`accounts.pk` ficou STOPPED.** O statement foi criado e populou as 240 contas; depois parou, e o join temporal usou o estado já materializado. Em produção ele deveria ficar RUNNING. Evidência: `evidencias/08-statements-status.txt`.
3. **Conta pessoal no Flink e nas API keys de setup/métricas.** Só o consumo do alerta usa a service account.
4. **Evolução BACKWARD:** o modelo fala em "rejeitar remoção de campo obrigatório", mas em BACKWARD remover campo é *permitido*. Testei o que de fato é rejeitado (campo novo obrigatório sem default). Veja a Camada 2.
5. **`setup.sh` e `teardown.sh` não foram testados de ponta a ponta.** Os statements do Flink e os testes de compatibilidade são manuais (seção "Como subir").

## Camada 1: Fundação

Environment `desafio-final`, cluster Basic `desafio-basic` (aws/us-east-1). Identidades:

- `desafio-producer` (`sa-8mk6gxq`): WRITE/CREATE/DESCRIBE/DESCRIBE_CONFIGS em tópicos com prefixo `desafio` e `__debezium-heartbeat`, DESCRIBE no cluster.
- `desafio-consumer` (`sa-7pzm5vw`): READ/DESCRIBE em tópicos `desafio`, READ em grupos `desafio`.

Escolhi o prefixo `desafio` (sem hífen) porque os tópicos do conector são `desafio.public.pagameeentos`; com `desafio-` ficariam sem permissão.

Evidências: `evidencias/00-cluster.txt`, `00-environment.txt`, `01-service-accounts.txt`, `01-acl-producer.txt`, `01-acl-consumer.txt`.

## Camada 2: Contrato de dados

Subject de teste `desafio.contrato.accounts-value` (separado dos subjects do conector). `balance` como `bytes` + `decimal(15,2)`. Modo BACKWARD (`evidencias/02-config.json`).

| Teste | Schema | Resultado | Evidência |
|---|---|---|---|
| Campo novo **com default** | `accounts-v2-ok.avsc` | `is_compatible: true` | `02-compat-aceita.json` |
| Campo novo **obrigatório sem default** | `accounts-v3-ruim.avsc` | `is_compatible: false` | `02-compat-rejeita.json` |
| Remoção de campo | primeira versão do v3 | `is_compatible: true` (permitido em BACKWARD) | `02-compat-remocao-permitida.json` |

Subjects registrados (`evidencias/02-subjects.txt`):

| Origem | Subjects |
|---|---|
| Conector CDC | `desafio.public.{accounts,cards,customers,merchants,transactions}-value` e o heartbeat `__debezium-heartbeat-lcc-o36w3w9.desafio-value` |
| Flink | `desafio.accounts.pk-key` e `-value`, `desafio.transactions.append-value`, `desafio.transactions.enriched-value`, `desafio.fraud.detected-value` |
| Teste da Camada 2 | `desafio.contrato.accounts-value` |

Só `desafio.accounts.pk` tem subject de chave: é a única tabela criada com PK declarada. O conector não registra schema de chave.

**Tags PII/PCI: não aplicadas.** Os campos sensíveis do seed (`cpf` e `email` em `customers`, `card_number` em `cards`) trafegam em texto claro nos tópicos `desafio.public.pagamento`, sem tag e sem mascaramento. Em produção, o caminho seria criar as definições de tag no Stream Governance, marcar esses campos e mascarar o número do cartão antes de publicar.


## Camada 3: Ingestão (CDC)

Conector `PostgresCdcSourceV2` (`connectors/cdc.json`, sem secrets: variáveis via `envsubst`), usuário `cdc_user` (REPLICATION + só SELECT), `heartbeat.interval.ms=60000`, `after.state.only=false`, `decimal.handling.mode=precise`, publication `desafio_pub`, host **direto** do Neon (replicação não funciona pelo pooler).

Seed: `evidencias/03-contagem-seed.txt` (200 / 240 / 320 / 60 / 2000). Conector RUNNING: `03-conector.txt`, `03-conector-describe.txt` (senha mascarada); tópicos: `03-topicos.txt`.

Eventos capturados na tabela `merchants` (`evidencias/01-envelopes-cdc.txt`):

- `op=c`: `after` preenchido
- `op=u`: `before` e `after` preenchidos (REPLICA IDENTITY FULL)
- `op=d`: `before` preenchido, `after` nulo, seguido de tombstone (valor nulo)

O snapshot inicial chega como `op=r`.

## Camada 4: Processamento (Flink SQL)

Statements em `sql/`:

- `01-enriquecimento.sql`: `accounts.pk` (PK declarada) e `transactions.append`
- `02-enriquecimento-join.sql`: temporal join transações ⋈ contas (`sql.tables.scan.idle-timeout=5s`)
- `03-alerta-fraude-.sql`: `MATCH_RECOGNIZE`, `PARTITION BY card_id`, `PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND`

Dois ajustes que o pipeline exigiu:

- O conector não registra schema de **chave**, então as tabelas vinham sem PK. A PK é declarada no Flink (`accounts.pk`).
- `MATCH_RECOGNIZE` não aceita entrada com updates/deletes. As transações passam por uma tabela `append` antes do join.

**Alerta emitido** (3 transações do mesmo cartão, inseridas no mesmo comando, intervalo de milissegundos):

```json
{ "card_id": "crd-0001", "first_tx": "trx-F0001", "last_tx": "trx-F0003", "tx_count": 3 }
```

Evidências: `evidencias/04-alerta-fraude.txt`, `04-transacoes-origem-alerta.txt`, `04-enriched.md`.

Consumo com a service account `desafio-consumer` (grupo `desafio-fraude-grp`, Avro via Schema Registry, RBAC `DeveloperRead` só no subject do alerta): `05-consumer-alerta.txt`, `05-rbac-consumer.txt`.

## Camada 5: Operação, as quatro respostas

### ① Observabilidade (Metrics API)

| Medição | Valor | Evidência |
|---|---|---|
| Pico de `received_bytes` (janela de 1 h) | 591.518 bytes em 2026-11-03T17:00Z (coincide com a hora do snapshot do conector, pelo billing; inferência) | `06-received-bytes.json`, `06-pico.txt` |
| Lag de `desafio-fraude-grp` antes | 146 (05/11) | `06-consumer-lag.json` |
| Lag de `desafio-fraude-grp` depois | 0 (06/11, 18:54 a 19:06 UTC) | `06-consumer-lag-depois.json` |
| Lag de `desafio-fraude-teste` (grupo descartável que leu as 280 mensagens) | 104 | `06-consumer-lag-depois.json` |

Hipótese **não confirmada**: o lag residual vem de offsets lidos mas não commitados quando o consumer é interrompido com Ctrl+C. O tópico de alertas tem 280 mensagens (`05-consumer-total.txt`, assumindo uma por linha). O comando `consumer group lag summarize` só funciona em cluster Dedicated; no Basic a medição vem da Metrics API.

### ② Segurança

| Identidade | Permissões | Por quê |
|---|---|---|
| `cdc_user` (Postgres) | REPLICATION + SELECT nas 5 tabelas | Menor privilégio: o conector lê, não altera dados |
| `desafio-producer` | WRITE/CREATE/DESCRIBE(_CONFIGS) em `desafio` e `__debezium-heartbeat`; DESCRIBE no cluster | O conector cria e escreve tópicos e heartbeat; sem READ |
| `desafio-consumer` | READ/DESCRIBE em `desafio`, READ em grupos `desafio`, RBAC `DeveloperRead` no subject `desafio.fraud.detected-value` | Lê e decodifica só o alerta; sem WRITE |

Prova de menor privilégio: com a chave do consumer, escrever no tópico falha com `Broker: Topic authorization failed` (`evidencias/05-consumer-sem-write.txt`).

### ③ Custos

`confluent billing cost list` (`evidencias/07-billing.txt`):
③ Custos

confluent billing cost list (evidencias/07-billing.txt):

| Dia | Conector | Flink | Kafka | Bruto | Cobrado |
|---|---|---|---|---|---|
| 03/11 | US$ 3,50 | US$ 0,399 | US$ 0,001 | US$ 3,90 | US$ 0,00 (crédito promo) |
| 04/11 | US$ 12,00 | US$ 6,986 | US$ 0,543 | US$ 19,529 | US$ 0,00 (crédito promo) |
| 05/11 | US$ 12,00 | US$ 14,7385| US$ 3,148 | US$ 29,8865 |	US$ 0,00 (crédito promo)|
|06/11	| US$ 10,50 |	US$ 17,626 |US$ 5,8468 |	US$ 33,9728|US$ 0,00 (crédito promo)
|Total	| US$ 38,00 |	US$ 39,7495|US$ 9,5388 |	US$ 87,2883	|US$ 0,00

**Custo declarado: `US$ 87,29 bruto (03/11 a 06/11`**  US$ 0,00 cobrado, porque todo o valor saiu do crédito de teste (a linha PROMO_CREDIT de cada dia cobre exatamente o bruto). O valor pode ainda crescer: o billing demora a consolidar e o teardown foi feito depois de 06/11. Distribuição: conector ~43,5%, Flink ~45,5%, Kafka ~10,9% (a linha KAFKA_NUM_CKUS só aparece a partir de 04/11). O custo diário subiu de US$ 3,90 para US$ 33,97 porque o pipeline ficou ligado entre dias, com vários statements contínuos.

Duas alavancas: (1) teardown logo após coletar evidências, porque o custo é por hora (conector US$ 0,50 por task-hour, Flink US$ 0,21 por CFU-hour) e o pipeline ficou ligado entre dias; (2) parar statements contínuos que não precisam ficar ativos e limitar o `--max-cfu` do compute pool.

### ④ Confiabilidade

| Trecho | O que garante | Onde pode falhar |
|---|---|---|
| Postgres → slot de replicação | WAL lógico retido até o conector confirmar | Slot esquecido acumula WAL no Neon (por isso o teardown apaga `desafio_slot`) |
| Slot → conector | Offsets do Connect; snapshot e depois streaming | Reinício pode reentregar eventos (at-least-once) |
| Conector → Kafka | Escrita replicada no cluster | Falha de autorização (ACL/RBAC) para o conector |
| Kafka → Flink | Statements leem por offset | Statement parado (ex.: `accounts.pk`) deixa o lado temporal desatualizado |
| Flink → tópico de alerta | Regra reproduz o mesmo alerta para os mesmos dados | Dependência do watermark e do `scan.idle-timeout` |
| **Consumer → efeito** | **Nada** | **Elo fraco.** Entrega at-least-once; offset commitado depois do efeito |

A idempotência é código meu: o efeito de um alerta (ex.: bloquear o cartão) deve usar uma chave de deduplicação, como `card_id` + `first_tx` + `last_tx`.

## Por que cada peça

- **CDC e não batch:** captura insert/update/delete com latência baixa e sem consultas pesadas no banco; o `before` do UPDATE/DELETE exige REPLICA IDENTITY FULL.
- **Avro + Schema Registry:** contrato versionado, com compatibilidade verificada antes de publicar.
- **Flink SQL:** temporal join para enriquecer e `MATCH_RECOGNIZE` para a regra de padrão em janela.
- **Janela de 60 s e 3 transações:** regra de velocity simples; ajustável em `sql/03-fraude-velocity.sql`.
- **Cluster Basic:** suficiente para o volume e o mais barato.

## Como subir do zero

Pré-requisitos: Confluent CLI logada, conta no Neon com replicação lógica ativada (host **direto**), Git Bash (ou bash), Python.

1. `cp .env.example .env` e preencha os valores (nunca versione o `.env`).
2. No SQL Editor do Neon, rode `seed/01-ddl.sql`, `02-replica-identity.sql`, `03-seed.sql`, `04-cdc-setup.sql` (troque a senha do `cdc_user` na hora; ela fica só no `.env`).
3. `./setup.sh` cria ambiente, cluster, service accounts, ACLs, schema de teste, conector e compute pool.
4. No SQL workspace do Flink (catalog `desafio-final`, database `desafio-basic`), rode `sql/01`, `02` e `03`, **um statement por célula**.
5. Teste de compatibilidade (Camada 2) e inserts de fraude (`trx-F0001` a `trx-F0003`, mesmo cartão): comandos nas seções acima.

## Como derrubar tudo

1. Pare os statements do Flink e apague o conector.
2. **Antes de apagar os recursos**, o ambiente default não pode ser apagado quando é o único da organização, salve as listas vazias (`None found.`) em `evidencias/08-teardown-depois.txt`.
3. Apague compute pool, API keys, cluster, ambiente e service accounts (`./teardown.sh`).
4. No Neon: `SELECT pg_drop_replication_slot('desafio_slot');` ou apague o projeto.

Verificação: `Verificação (`evidencias/08-teardown-depois.txt`):

```
== connect cluster list
None found.
== flink statement list
None found.
