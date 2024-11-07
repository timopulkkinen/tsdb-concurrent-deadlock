#!/bin/bash

# Drop and recreate database
psql postgres -c "DROP DATABASE IF EXISTS deadlock WITH (force);"
psql postgres -c "CREATE DATABASE deadlock;"
psql deadlock -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Create schema
psql deadlock << 'EOF'
CREATE TABLE main_entity (
    id bigserial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE op1 (
    id bigserial PRIMARY KEY,
    main_entity_id bigint REFERENCES main_entity(id),
    value numeric(20,2) NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE op1_event_data (
    id bigserial not null,
    parent_id bigint not null,
    main_entity_id bigint REFERENCES main_entity(id),
    status text NOT NULL,
    event_date timestamptz not null default date_trunc('day', now()),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, event_date),
    CONSTRAINT op1_event_data_unique UNIQUE (parent_id, status, event_date),
    CONSTRAINT op1_event_data_main_entity_fk FOREIGN KEY (main_entity_id) REFERENCES main_entity(id),
    CONSTRAINT op1_event_data_parent_fk FOREIGN KEY (parent_id) REFERENCES op1(id)
);

SELECT create_hypertable('op1_event_data', 'event_date', chunk_time_interval => interval '32 days');

ALTER TABLE op1_event_data SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'event_date DESC, parent_id, status, id',
    timescaledb.compress_segmentby = 'main_entity_id'
);

SELECT add_compression_policy('op1_event_data', INTERVAL '6 months');

CREATE TABLE transactions (
    id bigserial not null,
    parent_id bigint not null,
    main_entity_id bigint not null REFERENCES main_entity(id),
    value numeric(20,2) not null,
    created_at timestamptz not null default now(),
    record_date timestamptz not null default date_trunc('day', now()),
    CONSTRAINT transactions_main_entity_fk FOREIGN KEY (main_entity_id) REFERENCES main_entity(id),
    CONSTRAINT transactions_parent_fk FOREIGN KEY (parent_id) REFERENCES op1(id)
);

SELECT create_hypertable('transactions', 'record_date', chunk_time_interval => interval '1 day');

ALTER TABLE transactions SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'record_date, parent_id',
    timescaledb.compress_segmentby = 'main_entity_id'
);

SELECT add_compression_policy('transactions', INTERVAL '6 months');

-- Add indexes
CREATE INDEX transactions_main_entity_id_idx ON transactions(main_entity_id);
CREATE INDEX transactions_id_idx ON transactions(parent_id);
CREATE INDEX op1_event_data_main_entity_id_idx ON op1_event_data(main_entity_id);
CREATE INDEX op1_event_data_id_idx ON op1_event_data(parent_id);

CREATE OR REPLACE FUNCTION update_op1_status(
    _id bigint,
    _main_entity_id bigint,
    _new_status text
) RETURNS void AS $$
BEGIN
    INSERT INTO op1_event_data (parent_id, main_entity_id, status)
    VALUES (_id, _main_entity_id, _new_status);

    UPDATE op1 SET status = _new_status WHERE id = _id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_op1_and_transactions() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO transactions (parent_id, main_entity_id, value)
    VALUES (NEW.id, NEW.main_entity_id, NEW.value);

    PERFORM update_op1_status(NEW.id, NEW.main_entity_id, NEW.status);

    RETURN NEW;
END;
$$;

CREATE TRIGGER op1_insert_trigger
AFTER INSERT ON op1
FOR EACH ROW
EXECUTE PROCEDURE update_op1_and_transactions();

CREATE OR REPLACE FUNCTION simulate(
    _main_id bigint,
    _value numeric
) RETURNS void AS $$
BEGIN
    INSERT INTO op1 (main_entity_id, value, status)
    VALUES (_main_id, _value, 'created');

    -- Get the last inserted id
    PERFORM update_op1_status(currval('op1_id_seq'), _main_id, 'status1');
    PERFORM update_op1_status(currval('op1_id_seq'), _main_id, 'status2');
    PERFORM update_op1_status(currval('op1_id_seq'), _main_id, 'status3');
END;
$$ LANGUAGE plpgsql;

-- Setup test data
CREATE OR REPLACE FUNCTION setup_test_data() RETURNS SETOF bigint AS $$
BEGIN
    -- Clean up previous test data if exists
    TRUNCATE op1_event_data, transactions, op1, main_entity CASCADE;

    -- Create test entities and return their IDs
    RETURN QUERY INSERT INTO main_entity (name)
        SELECT 'test_entity_' || g
        FROM generate_series(1, 10) g
        RETURNING id;
END;
$$ LANGUAGE plpgsql;
EOF

# Setup test data
psql deadlock -c "SELECT setup_test_data();"

# Run concurrent tests with explicit numeric casting
psql deadlock -c "BEGIN; SELECT simulate(id, (10 + random() * 90)::numeric) FROM (SELECT id FROM main_entity WHERE id % 3 = 0) e; COMMIT;" &
psql deadlock -c "BEGIN; SELECT simulate(id, (10 + random() * 90)::numeric) FROM (SELECT id FROM main_entity WHERE id % 3 = 1) e; COMMIT;" &
psql deadlock -c "BEGIN; SELECT simulate(id, (10 + random() * 90)::numeric) FROM (SELECT id FROM main_entity WHERE id % 3 = 2) e; COMMIT;" &

# Wait for all background processes
wait

