ALTER TABLE idempotency_keys ADD COLUMN request_hash text NOT NULL DEFAULT '';

ALTER TABLE orders ADD CONSTRAINT orders_progress_not_negative
    CHECK (filled >= 0 AND cancelled_qty >= 0);

ALTER TABLE orders ADD CONSTRAINT orders_not_rejected_and_accepted
    CHECK (NOT (rejected AND accepted));

ALTER TABLE trades ADD CONSTRAINT trades_positive
    CHECK (price > 0 AND qty > 0);
