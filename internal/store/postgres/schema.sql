CREATE TABLE IF NOT EXISTS processed_event (
    source          TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    event_kind      TEXT        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_id, event_kind)
);

CREATE TABLE IF NOT EXISTS delivery (
    pair_id         TEXT        NOT NULL,
    source_platform TEXT        NOT NULL,
    source_id       TEXT        NOT NULL,
    target_platform TEXT        NOT NULL,
    target_id       TEXT        NOT NULL,
    target_thread_root TEXT,
    source_thread_root TEXT,
    edit_count      INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, source_platform, source_id)
);

-- Deployments created before threads were resolved through the store have the
-- table without this column.
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS target_thread_root TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS source_thread_root TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS edit_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS delivery_target_idx
    ON delivery (target_platform, target_id);

CREATE TABLE IF NOT EXISTS delta_cursor (
    pair_id     TEXT        NOT NULL PRIMARY KEY,
    delta_link  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscription (
    pair_id         TEXT        PRIMARY KEY,
    subscription_id TEXT        NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reaction_delivery (
    pair_id           TEXT        NOT NULL,
    source_platform   TEXT        NOT NULL,
    source_author     TEXT        NOT NULL,
    source_message_id TEXT        NOT NULL,
    emoji             TEXT        NOT NULL,
    target_platform   TEXT        NOT NULL,
    target_message_id TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, source_platform, source_author, source_message_id, emoji)
);

CREATE INDEX IF NOT EXISTS reaction_delivery_target_idx
    ON reaction_delivery (pair_id, target_platform, target_message_id, created_at DESC);
