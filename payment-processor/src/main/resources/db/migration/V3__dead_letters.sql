-- Which delivery attempt booked the payment: 1 = main topic, 2+ = a retry topic.
ALTER TABLE payments ADD COLUMN delivery_attempt INT NOT NULL DEFAULT 1;

-- Index of the DLT for the inspector endpoint. The DLT topic itself stays the source of truth;
-- (dlt_partition, dlt_offset) is unique so a redelivered dead letter is recorded once.
CREATE TABLE dead_letters (
    id                 BIGSERIAL    PRIMARY KEY,
    dlt_topic          VARCHAR(128) NOT NULL,
    dlt_partition      INT          NOT NULL,
    dlt_offset         BIGINT       NOT NULL,
    message_key        VARCHAR(64),
    payload            TEXT         NOT NULL,
    attempts           INT          NOT NULL,          -- processing attempts before it was dead-lettered
    original_topic     VARCHAR(128),
    original_partition INT,
    original_offset    BIGINT,
    exception_class    VARCHAR(255),
    exception_message  TEXT,
    stacktrace         TEXT,
    received_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    replay_count       INT          NOT NULL DEFAULT 0,
    last_replayed_at   TIMESTAMPTZ,
    UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);
CREATE INDEX dead_letters_pending_idx ON dead_letters (received_at) WHERE replay_count = 0;
