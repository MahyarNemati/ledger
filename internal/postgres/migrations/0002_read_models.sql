CREATE TABLE projection_checkpoints (
    name     text PRIMARY KEY,
    position bigint NOT NULL DEFAULT 0
);

CREATE TABLE balances (
    account_id      uuid PRIMARY KEY,
    account_type    text   NOT NULL,
    currency        text   NOT NULL,
    debits          bigint NOT NULL DEFAULT 0,
    credits         bigint NOT NULL DEFAULT 0,
    pending_debits  bigint NOT NULL DEFAULT 0,
    pending_credits bigint NOT NULL DEFAULT 0,
    position        bigint NOT NULL
);

CREATE TABLE statement_lines (
    account_id    uuid        NOT NULL,
    position      bigint      NOT NULL,
    entry_id      uuid        NOT NULL,
    side          text        NOT NULL,
    amount        bigint      NOT NULL,
    balance_after bigint      NOT NULL,
    description   text        NOT NULL DEFAULT '',
    posted_at     timestamptz NOT NULL,
    PRIMARY KEY (account_id, position)
);
