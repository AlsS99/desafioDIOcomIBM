#!/usr/bin/env bash
# Derruba tudo. IRREVERSIVEL. Salve as evidencias antes (metricas, billing, consumo).
# NAO testado de ponta a ponta: confira flags com --help.
set -uo pipefail
cd "$(dirname "$0")"
command -v confluent >/dev/null || export PATH="$PATH:$(cd .. && pwd)"
mkdir -p evidencias

pyjson() { python -c "import json,sys;d=json.load(sys.stdin);print(' '.join($1))"; }

ENV_ID=$(confluent environment list -o json | pyjson "[e['id'] for e in d if e['name']=='desafio-final']")
[ -n "$ENV_ID" ] || { echo "ambiente desafio-final nao existe"; exit 0; }
confluent environment use "$ENV_ID"
confluent flink region use --cloud aws --region us-east-1

# PARTE A: statements e conector
for s in $(confluent flink statement list -o json | pyjson "[x['name'] for x in d]"); do
  confluent flink statement delete "$s" --force
done
for c in $(confluent connect cluster list -o json | pyjson "[x['id'] for x in d]"); do
  confluent connect cluster delete "$c" --force
done

# Evidencia "None found." (so existe ENQUANTO o ambiente existe)
{
  echo "== connect cluster list";    confluent connect cluster list
  echo "== flink statement list";    confluent flink statement list
} 2>&1 | tee evidencias/08-teardown-depois.txt

# PARTE B: o resto
for p in $(confluent flink compute-pool list -o json | pyjson "[x['id'] for x in d]"); do
  confluent flink compute-pool delete "$p" --force
done
for k in $(confluent api-key list -o json | pyjson "[x['key'] for x in d if x.get('description','').startswith('desafio')]"); do
  confluent api-key delete "$k" --force
done
for c in $(confluent kafka cluster list -o json | pyjson "[x['id'] for x in d]"); do
  confluent kafka cluster delete "$c" --force
done
confluent environment delete "$ENV_ID" --force
for sa in $(confluent iam service-account list -o json | pyjson "[x['id'] for x in d if x['name'].startswith('desafio')]"); do
  confluent iam service-account delete "$sa" --force
done

{
  confluent environment list
  confluent iam service-account list
} 2>&1 | tee evidencias/08-teardown-final.txt

echo "No Neon: SELECT pg_drop_replication_slot('desafio_slot'); (ou apague o projeto)"
echo "Apague tambem as API keys que nao comecam com 'desafio' (ex.: Schema Registry, metrics) com: confluent api-key list"
