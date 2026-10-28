CREATE TABLE idempotency_keys (
    scope        text        NOT NULL,
    key          text        NOT NULL,
    request_hash bytea       NOT NULL,
    status_code  int,
    response     bytea,
    created_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (scope, key)
);

CREATE INDEX idempotency_keys_created ON idempotency_keys (created_at);

CREATE TABLE inbox (
    consumer     text        NOT NULL,
    message_id   uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Written by the example consumer.
CREATE TABLE account_activity (
    account_id   uuid        PRIMARY KEY,
    postings     bigint      NOT NULL DEFAULT 0,
    last_entry   uuid,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
