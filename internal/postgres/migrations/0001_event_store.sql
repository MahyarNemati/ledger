-- Append-only event log. Rows are never updated or deleted.
CREATE TABLE streams (
    stream_id  text PRIMARY KEY,
    version    bigint NOT NULL CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE events (
    global_position bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        uuid        NOT NULL UNIQUE,
    stream_id       text        NOT NULL,
    version         bigint      NOT NULL CHECK (version > 0),
    event_type      text        NOT NULL,
    data            jsonb       NOT NULL,
    metadata        jsonb,
    tx_id           xid8        NOT NULL DEFAULT pg_current_xact_id(),
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream_id, version)
);

CREATE FUNCTION events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events table is append-only';
END;
$$;

CREATE TRIGGER events_no_update BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_append_only();

CREATE TABLE snapshots (
    stream_id  text PRIMARY KEY,
    version    bigint      NOT NULL,
    state      jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    seq          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id           uuid        NOT NULL UNIQUE,
    topic        text        NOT NULL,
    msg_key      text        NOT NULL,
    payload      jsonb       NOT NULL,
    headers      jsonb       NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts     int         NOT NULL DEFAULT 0,
    last_error   text
);

CREATE INDEX outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;
