#!/usr/bin/env bash
# Sobe a base do pipeline. NAO testado de ponta a ponta: confira flags com --help.
# Manual depois do script: seed no Neon, statements do Flink, testes de compatibilidade.
set -euo pipefail
cd "$(dirname "$0")"

# Se a CLI nao estiver no PATH, ajuste (aqui ela fica na pasta acima do projeto)
command -v confluent >/dev/null || export PATH="$PATH:$(cd .. && pwd)"

set -a; source .env; set +a
for v in SR_URL PG_HOST PG_DB PG_CDC_PASSWORD; do
  [ -n "${!v:-}" ] || { echo "falta $v no .env"; exit 1; }
done

jid() { python -c "import json,sys;print(json.load(sys.stdin)['id'])"; }

# Camada 1: ambiente, cluster, identidades
ENV_ID=$(confluent environment create desafio-final -o json | jid)
confluent environment use "$ENV_ID"

CLUSTER_ID=$(confluent kafka cluster create desafio-basic --cloud aws --region us-east-1 --type basic -o json | jid)
confluent kafka cluster use "$CLUSTER_ID"

SA_PRODUCER_ID=$(confluent iam service-account create desafio-producer --description "escreve em desafio" -o json | jid)
SA_CONSUMER_ID=$(confluent iam service-account create desafio-consumer --description "le desafio" -o json | jid)
export SA_PRODUCER_ID

confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create,describe-configs --topic desafio --prefix
confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create,describe-configs --topic __debezium-heartbeat --prefix
confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations describe --cluster-scope
confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read,describe --topic desafio --prefix
confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read --consumer-group desafio --prefix

# Camada 2: schema de teste (os testes de compatibilidade sao manuais, ver README)
confluent schema-registry schema create --subject desafio.contrato.accounts-value --schema schemas/accounts-v1.avsc --type avro

# Camada 3: banco (manual: rode seed/*.sql no SQL Editor do Neon)
echo "Rode seed/01..04 no SQL Editor do Neon (host DIRETO, sem -pooler) e pressione Enter."
read -r

# Conector: gera o arquivo final (fora do Git) e cria
python -c "import os;print(os.path.expandvars(open('connectors/cdc.json').read()))" > connectors/cdc.final.json
if grep -q '\${' connectors/cdc.final.json; then echo "variavel sem valor em cdc.final.json"; exit 1; fi
confluent connect cluster create --config-file connectors/cdc.final.json
echo "Aguarde RUNNING: confluent connect cluster list"

# Camada 4: Flink (statements sao manuais: rode sql/01, 02, 03 no SQL workspace)
confluent flink region use --cloud aws --region us-east-1
confluent flink compute-pool create desafio-pool --cloud aws --region us-east-1 --max-cfu 5
echo "Pronto. Rode sql/01, 02 e 03 no SQL workspace, um statement por celula."
