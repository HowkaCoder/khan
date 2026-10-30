#!/usr/bin/env bash
set -euo pipefail

API=${API:-http://localhost:8080}
ENGINE=${ENGINE:-http://localhost:8081}
COMPOSE=${COMPOSE:-docker compose -f deploy/docker-compose.yml}
KILL_ENGINE=${KILL_ENGINE:-$COMPOSE kill -s SIGKILL engine}
START_ENGINE=${START_ENGINE:-$COMPOSE start engine}
PSQL=${PSQL:-psql postgres://khan:khan@localhost:5432/khan}
LOADGEN=${LOADGEN:-go run ./cmd/loadgen}

state() {
  curl -sS "$ENGINE/state" | python3 -c "import json,sys; s=json.load(sys.stdin); print(s['last_seq'], s['hash'], s['orders'])"
}

wait_engine_idle() {
  local previous="" current
  for _ in $(seq 1 120); do
    current=$(state 2>/dev/null || true)
    if [ -n "$current" ] && [ "$current" = "$previous" ]; then
      echo "$current"
      return
    fi
    previous=$current
    sleep 0.5
  done
  echo "движок не успокоился за минуту" >&2
  exit 1
}

wait_engine_down() {
  for _ in $(seq 1 20); do
    curl -s -o /dev/null "$ENGINE/state" || return 0
    sleep 0.25
  done
  echo "движок всё ещё отвечает после kill" >&2
  exit 1
}

wait_settlement_idle() {
  local previous="" current
  for _ in $(seq 1 120); do
    current=$($PSQL -tA -c "SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL) || ':' || (SELECT count(*) FROM trades) || ':' || (SELECT count(*) FROM processed_events)")
    if [ "${current%%:*}" = "0" ] && [ "$current" = "$previous" ]; then
      return
    fi
    previous=$current
    sleep 1
  done
  echo "settlement не успокоился за две минуты" >&2
  exit 1
}

echo "━━ Сценарий 1: падение без нагрузки, сравнение хэша ━━"
$LOADGEN -api "$API" -orders 3000 -c 16 -seed 1
$LOADGEN -api "$API" -orders 300 -c 4 -seed 2

read -r SEQ HASH ORDERS < <(wait_engine_idle)
SNAPSHOT_SEQ=$($PSQL -tA -c "SELECT coalesce(max(last_seq), 0) FROM book_snapshots")
echo "== до падения: last_seq=$SEQ, в стакане $ORDERS заявок"
echo "   hash=$HASH"
echo "   последний снапшот на seq $SNAPSHOT_SEQ, после рестарта нужно проиграть $((SEQ - SNAPSHOT_SEQ)) команд"

echo "== kill -9 движка"
eval "$KILL_ENGINE"
wait_engine_down

eval "$START_ENGINE"
for _ in $(seq 1 60); do
  read -r SEQ_AFTER HASH_AFTER ORDERS_AFTER < <(state 2>/dev/null || echo "0 - 0")
  [ "$SEQ_AFTER" = "$SEQ" ] && break
  sleep 0.5
done
echo "== после рестарта: last_seq=$SEQ_AFTER, в стакане $ORDERS_AFTER заявок"
echo "   hash=$HASH_AFTER"
[ "$HASH_AFTER" = "$HASH" ] || { echo "ОШИБКА: хэш стакана после восстановления другой" >&2; exit 1; }
echo "хэш совпал: снапшот и replay восстановили тот же стакан"

echo
echo "━━ Сценарий 2: падение под нагрузкой ━━"
$LOADGEN -api "$API" -orders 2000 -c 16 -seed 3 &
LOADGEN_PID=$!
sleep 1.5
echo "== kill -9 движка во время потока заявок"
eval "$KILL_ENGINE"
wait_engine_down
sleep 1
eval "$START_ENGINE"
wait $LOADGEN_PID
read -r SEQ2 _ ORDERS2 < <(wait_engine_idle)
echo "== движок догнал поток: last_seq=$SEQ2, в стакане $ORDERS2 заявок"
wait_settlement_idle

echo
echo "━━ Проверка денег ━━"
$PSQL -v ON_ERROR_STOP=1 < scripts/verify.sql
