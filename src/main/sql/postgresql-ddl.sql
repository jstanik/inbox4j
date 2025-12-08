CREATE TABLE inbox_message (
    id            bigint      GENERATED ALWAYS AS IDENTITY CONSTRAINT inbox_message_pk PRIMARY KEY,
    version       integer     NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    channel       text        NOT NULL,
    status        text        NOT NULL,
    payload       bytea       NOT NULL,
    metadata      bytea,
    trace_context text,
    audit_log     text        NOT NULL
);

CREATE INDEX inbox_message_status ON inbox_message (status);

CREATE TABLE inbox_message_target (
    name             text   NOT NULL,
    inbox_message_fk bigint NOT NULL CONSTRAINT inbox_message_target_inbox_message_fk
                                                REFERENCES inbox_message(id) ON DELETE CASCADE,
    CONSTRAINT inbox_message_target_name_and_fk_unique UNIQUE (name, inbox_message_fk)
);

CREATE INDEX inbox_message_target_name ON inbox_message_target (name);


