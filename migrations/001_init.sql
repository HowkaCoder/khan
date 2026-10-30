CREATE TABLE balances (
    account_id bigint NOT NULL,
    asset      text   NOT NULL,
    available  bigint NOT NULL DEFAULT 0 CHECK (available >= 0),
    held       bigint NOT NULL DEFAULT 0 CHECK (held >= 0),
    PRIMARY KEY (account_id, asset)
);

CREATE TABLE orders (
    id            text   PRIMARY KEY,
    account_id    bigint NOT NULL,
    symbol        text   NOT NULL,
    side          text   NOT NULL CHECK (side IN ('buy', 'sell')),
    price         bigint NOT NULL CHECK (price > 0),
    qty           bigint NOT NULL CHECK (qty > 0),
    filled        bigint NOT NULL DEFAULT 0,
    cancelled_qty bigint NOT NULL DEFAULT 0,
    accepted      boolean NOT NULL DEFAULT false,
    rejected      boolean NOT NULL DEFAULT false,
    status text GENERATED ALWAYS AS (
        CASE
            WHEN rejected          THEN 'rejected'
            WHEN filled = qty      THEN 'filled'
            WHEN cancelled_qty > 0 THEN 'cancelled'
            WHEN filled > 0        THEN 'partially_filled'
            WHEN accepted          THEN 'open'
            ELSE 'pending'
        END
    ) STORED,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (filled + cancelled_qty <= qty)
);

CREATE INDEX orders_active_by_account ON orders (account_id, created_at DESC)
    WHERE status IN ('pending', 'open', 'partially_filled');

CREATE TABLE idempotency_keys (
    account_id bigint NOT NULL,
    key        text   NOT NULL,
    order_id   text   NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, key)
);

CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    subject      text  NOT NULL,
    msg_id       text  NOT NULL UNIQUE,
    payload      bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE trades (
    trade_id   text   PRIMARY KEY,
    symbol     text   NOT NULL,
    seq        bigint NOT NULL,
    buy_order  text   NOT NULL REFERENCES orders (id),
    sell_order text   NOT NULL REFERENCES orders (id),
    price      bigint NOT NULL,
    qty        bigint NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
    event_id     text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id         bigserial PRIMARY KEY,
    ref        text   NOT NULL,
    account_id bigint NOT NULL,
    asset      text   NOT NULL,
    amount     bigint NOT NULL CHECK (amount <> 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_by_account ON ledger_entries (account_id, id DESC);

CREATE TABLE book_snapshots (
    symbol         text   NOT NULL,
    last_seq       bigint NOT NULL,
    state          bytea  NOT NULL,
    hash           text   NOT NULL,
    engine_version text   NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, last_seq)
);
