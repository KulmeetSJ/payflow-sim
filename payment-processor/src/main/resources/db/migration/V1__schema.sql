-- Synthetic accounts and a double-entry ledger. All data is generated; no real formats.
CREATE TABLE accounts (
    account_id  VARCHAR(10)    PRIMARY KEY,           -- ACC-000001
    currency    CHAR(3)        NOT NULL,
    balance     NUMERIC(14, 2) NOT NULL CHECK (balance >= 0),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id      UUID           PRIMARY KEY,       -- = event id (derived from Idempotency-Key)
    idempotency_key VARCHAR(64)    NOT NULL,
    from_account    VARCHAR(10)    NOT NULL,
    to_account      VARCHAR(10)    NOT NULL,
    amount          NUMERIC(14, 2) NOT NULL,
    currency        CHAR(3)        NOT NULL,
    status          VARCHAR(16)    NOT NULL CHECK (status IN ('COMPLETED', 'REJECTED')),
    reject_reason   VARCHAR(64),
    accepted_at     TIMESTAMPTZ    NOT NULL,          -- gateway time; accepted -> committed = e2e latency
    committed_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    kafka_partition INT            NOT NULL,
    kafka_offset    BIGINT         NOT NULL
);
CREATE INDEX payments_from_account_idx ON payments (from_account, accepted_at);

CREATE TABLE ledger_entries (
    entry_id    BIGSERIAL      PRIMARY KEY,
    payment_id  UUID           NOT NULL REFERENCES payments (payment_id),
    account_id  VARCHAR(10)    NOT NULL REFERENCES accounts (account_id),
    direction   VARCHAR(6)     NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount      NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id);

-- Idempotent consumer: inserted in the SAME transaction as the ledger write. A redelivered event
-- hits the primary key and is skipped, so at-least-once delivery gives exactly-once effects.
CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
