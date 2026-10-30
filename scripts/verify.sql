\set ON_ERROR_STOP on
\pset footer off

CREATE TEMP VIEW check_ledger_sum AS
SELECT asset, sum(amount) AS total
FROM ledger_entries
GROUP BY asset
HAVING sum(amount) <> 0;

CREATE TEMP VIEW check_trade_entries AS
SELECT t.trade_id, count(l.id) AS entries
FROM trades t
LEFT JOIN ledger_entries l ON l.ref = t.trade_id
GROUP BY t.trade_id
HAVING count(l.id) <> 4;

CREATE TEMP VIEW check_held AS
WITH expected AS (
    SELECT o.account_id,
           CASE WHEN o.side = 'buy' THEN split_part(o.symbol, '-', 2)
                ELSE split_part(o.symbol, '-', 1) END AS asset,
           sum(CASE WHEN o.side = 'buy' THEN o.price ELSE 1 END
               * (o.qty - o.filled - o.cancelled_qty)) AS held
    FROM orders o
    WHERE NOT o.rejected
    GROUP BY 1, 2
)
SELECT coalesce(b.account_id, e.account_id) AS account_id,
       coalesce(b.asset, e.asset) AS asset,
       coalesce(b.held, 0) AS held,
       coalesce(e.held, 0) AS expected
FROM balances b
FULL JOIN expected e ON e.account_id = b.account_id AND e.asset = b.asset
WHERE coalesce(b.held, 0) <> coalesce(e.held, 0);

CREATE TEMP VIEW check_filled AS
SELECT o.id, o.filled, coalesce(sum(t.qty), 0) AS from_trades
FROM orders o
LEFT JOIN trades t ON t.buy_order = o.id OR t.sell_order = o.id
GROUP BY o.id, o.filled
HAVING o.filled <> coalesce(sum(t.qty), 0);

CREATE TEMP VIEW check_balance_vs_ledger AS
WITH ledger AS (
    SELECT account_id, asset, sum(amount) AS total
    FROM ledger_entries
    WHERE account_id <> 0
    GROUP BY 1, 2
)
SELECT coalesce(b.account_id, l.account_id) AS account_id,
       coalesce(b.asset, l.asset) AS asset,
       coalesce(b.available + b.held, 0) AS balance,
       coalesce(l.total, 0) AS ledger
FROM balances b
FULL JOIN ledger l ON l.account_id = b.account_id AND l.asset = b.asset
WHERE coalesce(b.available + b.held, 0) <> coalesce(l.total, 0);

\echo '1. Сумма всех проводок по каждому активу равна 0'
SELECT * FROM check_ledger_sum;
\echo '2. У каждой сделки ровно 4 проводки'
SELECT * FROM check_trade_entries;
\echo '3. held каждого счёта равен резерву по его активным заявкам'
SELECT * FROM check_held;
\echo '4. filled каждой заявки равен сумме её сделок'
SELECT * FROM check_filled;
\echo '5. available + held каждого счёта равны сумме его проводок'
SELECT * FROM check_balance_vs_ledger;

\echo 'Сводка (не проверка: pending и outbox_pending больше нуля, пока система обрабатывает поток)'
SELECT (SELECT count(*) FROM orders) AS orders,
       (SELECT count(*) FROM trades) AS trades,
       (SELECT count(*) FROM orders WHERE status = 'cancelled') AS cancelled,
       (SELECT count(*) FROM outbox WHERE published_at IS NULL) AS outbox_pending,
       (SELECT count(*) FROM orders WHERE status = 'pending') AS still_pending;

DO $$
DECLARE
    failed text := '';
BEGIN
    IF EXISTS (SELECT 1 FROM check_ledger_sum) THEN failed := failed || ' 1'; END IF;
    IF EXISTS (SELECT 1 FROM check_trade_entries) THEN failed := failed || ' 2'; END IF;
    IF EXISTS (SELECT 1 FROM check_held) THEN failed := failed || ' 3'; END IF;
    IF EXISTS (SELECT 1 FROM check_filled) THEN failed := failed || ' 4'; END IF;
    IF EXISTS (SELECT 1 FROM check_balance_vs_ledger) THEN failed := failed || ' 5'; END IF;
    IF failed <> '' THEN
        RAISE EXCEPTION 'нарушены финансовые инварианты:%', failed;
    END IF;
    RAISE NOTICE 'все финансовые инварианты выполнены';
END
$$;
