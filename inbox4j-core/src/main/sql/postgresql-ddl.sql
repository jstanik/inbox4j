CREATE TABLE inbox_message (
    id            bigint      GENERATED ALWAYS AS IDENTITY CONSTRAINT inbox_message_pk PRIMARY KEY,
    version       integer     NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    channel       text        NOT NULL,
    status        text        NOT NULL,
    payload       bytea       NOT NULL,
    metadata      bytea,
    retry_at      timestamptz,
    trace_context text,
    audit_log     text        NOT NULL,
    CONSTRAINT retry_status_requires_retry_at_value_chk CHECK ((status = 'RETRY') = (retry_at IS NOT NULL))
);

CREATE INDEX inbox_message_status_idx ON inbox_message (status);
CREATE INDEX inbox_message_completed_at_idx ON inbox_message (updated_at) WHERE status = 'COMPLETED';

CREATE TABLE inbox_message_recipient (
    name             text   NOT NULL,
    inbox_message_fk bigint NOT NULL CONSTRAINT inbox_message_recipient_inbox_message_fk
                                                REFERENCES inbox_message(id) ON DELETE CASCADE,
    CONSTRAINT inbox_message_recipient_name_and_fk_unique UNIQUE (name, inbox_message_fk)
);

CREATE INDEX inbox_message_recipient_name ON inbox_message_recipient (name);


