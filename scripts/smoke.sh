#!/usr/bin/env bash
set -euo pipefail

API=${API:-http://localhost:8080}
PSQL=${PSQL:-psql postgres://khan:khan@localhost:5432/khan -tA}
ALICE=${ALICE:-101}
BOB=${BOB:-102}
RUN=$(date +%s%N)

post() {
  local path=$1 body=$2 key=${3:-}
  if [ -n "$key" ]; then
    curl -sS -X POST "$API$path" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" -d "$body"
  else
    curl -sS -X POST "$API$path" -H 'Content-Type: application/json' -d "$body"
  fi
}
field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }
fail() { echo "ОШИБКА: $*" >&2; exit 1; }
balance() { $PSQL -c "SELECT coalesce((SELECT $3 FROM balances WHERE account_id=$1 AND asset='$2'), 0)"; }

wait_status() {
  local id=$1 want=$2 status=""
  for _ in $(seq 1 100); do
    status=$(curl -sS "$API/v1/orders/$id" | field '["status"]')
    [ "$status" = "$want" ] && return 0
    sleep 0.1
  done
  fail "заявка $id в статусе $status, ожидался $want"
}

ALICE_USD_BEFORE=$(balance "$ALICE" USD available)
ALICE_ACME_BEFORE=$(balance "$ALICE" ACME available)
BOB_USD_BEFORE=$(balance "$BOB" USD available)
BOB_ACME_HELD_BEFORE=$(balance "$BOB" ACME held)

echo "== пополняем счета: Алисе 1000.00 USD, Бобу 10 ACME"
post /v1/deposits "{\"account_id\":$ALICE,\"asset\":\"USD\",\"amount\":100000}" >/dev/null
post /v1/deposits "{\"account_id\":$BOB,\"asset\":\"ACME\",\"amount\":10}" >/dev/null

echo "== Боб продаёт 5 ACME по 100.00"
SELL=$(post /v1/orders "{\"account_id\":$BOB,\"symbol\":\"ACME-USD\",\"side\":\"sell\",\"price\":\"100.00\",\"qty\":5}" "bob-$RUN")
echo "$SELL"
SELL_ID=$(echo "$SELL" | field '["id"]')
wait_status "$SELL_ID" open

echo "== Алиса покупает 3 ACME с лимитом 101.00"
BUY=$(post /v1/orders "{\"account_id\":$ALICE,\"symbol\":\"ACME-USD\",\"side\":\"buy\",\"price\":\"101.00\",\"qty\":3}" "alice-$RUN")
echo "$BUY"
BUY_ID=$(echo "$BUY" | field '["id"]')

echo "== повтор с тем же Idempotency-Key возвращает ту же заявку"
REPEAT=$(post /v1/orders "{\"account_id\":$ALICE,\"symbol\":\"ACME-USD\",\"side\":\"buy\",\"price\":\"101.00\",\"qty\":3}" "alice-$RUN")
[ "$(echo "$REPEAT" | field '["id"]')" = "$BUY_ID" ] || fail "повтор создал новую заявку"

wait_status "$BUY_ID" filled
wait_status "$SELL_ID" partially_filled

echo "== балансы Алисы"; curl -sS "$API/v1/accounts/$ALICE/balances"; echo
echo "== балансы Боба";  curl -sS "$API/v1/accounts/$BOB/balances"; echo

[ "$(balance "$ALICE" USD available)" = "$((ALICE_USD_BEFORE + 100000 - 30000))" ] || fail "Алиса должна заплатить 300.00 по цене продавца"
[ "$(balance "$ALICE" ACME available)" = "$((ALICE_ACME_BEFORE + 3))" ] || fail "Алиса должна получить 3 ACME"
[ "$(balance "$BOB" USD available)" = "$((BOB_USD_BEFORE + 30000))" ] || fail "Боб должен получить 300.00"
[ "$(balance "$BOB" ACME held)" = "$((BOB_ACME_HELD_BEFORE + 2))" ] || fail "у Боба в резерве должно остаться 2 ACME"

echo "== Боб отменяет остаток"
curl -sS -X DELETE "$API/v1/orders/$SELL_ID?account_id=$BOB"; echo
wait_status "$SELL_ID" cancelled
[ "$(balance "$BOB" ACME held)" = "$BOB_ACME_HELD_BEFORE" ] || fail "отмена не вернула резерв Боба"

echo "== сумма проводок по каждому активу должна быть 0"
BAD=$($PSQL -c "SELECT count(*) FROM (SELECT asset FROM ledger_entries GROUP BY asset HAVING sum(amount) <> 0) t")
[ "$BAD" = "0" ] || fail "проводки не сходятся"

echo "OK"
