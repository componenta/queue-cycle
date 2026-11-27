CREATE TABLE {{ledger}} (
    row_id text COLLATE "C" PRIMARY KEY,
    queue_name text COLLATE "C" NOT NULL CHECK (octet_length(queue_name) BETWEEN 1 AND 128),
    logical_id text COLLATE "C" NOT NULL CHECK (octet_length(logical_id) BETWEEN 1 AND 255),
    raw_body bytea NOT NULL,
    raw_headers bytea NOT NULL,
    original_delay_ms bigint NOT NULL CHECK (original_delay_ms >= 0),
    state text NOT NULL CHECK (state IN ('ready', 'leased', 'failed', 'completed', 'discarded')),
    generation bigint NOT NULL CHECK (generation > 0),
    retry_count bigint NOT NULL CHECK (retry_count >= 0),
    delivery_count bigint NOT NULL CHECK (delivery_count >= 0),
    lifetime_deliveries bigint NOT NULL CHECK (lifetime_deliveries >= 0),
    redrive_count bigint NOT NULL CHECK (redrive_count >= 0),
    max_deliveries bigint NOT NULL CHECK (max_deliveries > 0),
    available_at timestamptz NOT NULL,
    lease_token text COLLATE "C",
    lease_until timestamptz,
    received_at timestamptz,
    current_failure_id text COLLATE "C",
    created_at timestamptz NOT NULL,
    terminal_at timestamptz,
    dedup_until timestamptz,
    UNIQUE (queue_name, logical_id),
    CHECK ((state = 'leased' AND lease_token IS NOT NULL AND lease_until IS NOT NULL AND received_at IS NOT NULL)
        OR (state <> 'leased' AND lease_token IS NULL AND lease_until IS NULL)),
    CHECK ((state = 'failed') = (current_failure_id IS NOT NULL)),
    CHECK ((state IN ('completed', 'discarded')) = (terminal_at IS NOT NULL)),
    CHECK (state IN ('completed', 'discarded') OR dedup_until IS NULL)
);
CREATE TABLE {{failures}} (
    failure_id text COLLATE "C" PRIMARY KEY,
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    ledger_id text COLLATE "C" REFERENCES {{ledger}} (row_id),
    queue_name text COLLATE "C" NOT NULL,
    logical_id text COLLATE "C" NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    raw_body bytea NOT NULL,
    raw_headers bytea NOT NULL,
    retry_count bigint NOT NULL CHECK (retry_count >= 0),
    delivery_count bigint NOT NULL CHECK (delivery_count > 0),
    received_at timestamptz NOT NULL,
    failed_at timestamptz NOT NULL,
    reason text NOT NULL CHECK (reason IN ('decode', 'handler', 'rejected', 'delivery_limit')),
    exception_type text,
    description text,
    status text NOT NULL CHECK (status IN ('pending', 'redriven', 'discarded')),
    redrive_delay_ms bigint,
    redrive_generation bigint,
    resolved_at timestamptz,
    retained_until timestamptz,
    CHECK ((status = 'pending' AND ledger_id IS NOT NULL AND resolved_at IS NULL)
        OR (status <> 'pending' AND resolved_at IS NOT NULL)),
    CHECK ((status = 'redriven' AND redrive_delay_ms IS NOT NULL AND redrive_generation IS NOT NULL)
        OR (status <> 'redriven' AND redrive_delay_ms IS NULL AND redrive_generation IS NULL))
);
CREATE INDEX queue_ready ON {{ledger}} (queue_name, available_at, row_id) WHERE state = 'ready';
CREATE INDEX queue_leased ON {{ledger}} (queue_name, lease_until, row_id) WHERE state = 'leased';
CREATE INDEX queue_terminal ON {{ledger}} (queue_name, dedup_until, row_id) WHERE state IN ('completed', 'discarded') AND dedup_until IS NOT NULL;
CREATE INDEX queue_failure_page ON {{failures}} (queue_name, sequence);
CREATE INDEX queue_failure_filtered_page ON {{failures}} (queue_name, status, sequence);
CREATE INDEX queue_failure_cleanup ON {{failures}} (queue_name, retained_until, failure_id) WHERE status <> 'pending' AND retained_until IS NOT NULL;
CREATE INDEX queue_failure_ledger ON {{failures}} (ledger_id, failure_id);
