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

CREATE TABLE parent_record (
    id bigserial PRIMARY KEY,
    main_entity_id bigint REFERENCES main_entity(id),
    amount numeric(20,2) NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE parent_event (
    id bigserial not null,
    parent_id bigint not null,
    main_entity_id bigint,
    status text NOT NULL,
    event_date timestamptz not null default date_trunc('day', now()),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, event_date),
    CONSTRAINT parent_event_unique UNIQUE (parent_id, status, event_date),
    CONSTRAINT parent_event_main_entity_fk FOREIGN KEY (main_entity_id) REFERENCES main_entity(id),
    CONSTRAINT parent_event_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_record(id)
);

SELECT create_hypertable('parent_event', 'event_date', chunk_time_interval => interval '32 days');

ALTER TABLE parent_event SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'event_date DESC, parent_id, status, id',
    timescaledb.compress_segmentby = 'main_entity_id'
);

SELECT add_compression_policy('parent_event', INTERVAL '6 months');

CREATE TABLE time_series_record (
    id bigserial not null,
    parent_id bigint not null,
    main_entity_id bigint not null,
    amount numeric(20,2) not null,
    created_at timestamptz not null default now(),
    record_date timestamptz not null default date_trunc('day', now()),
    CONSTRAINT time_series_record_main_entity_fk FOREIGN KEY (main_entity_id) REFERENCES main_entity(id),
    CONSTRAINT time_series_record_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_record(id)
);

SELECT create_hypertable('time_series_record', 'record_date', chunk_time_interval => interval '1 day');

ALTER TABLE time_series_record SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'record_date, parent_id',
    timescaledb.compress_segmentby = 'main_entity_id'
);

SELECT add_compression_policy('time_series_record', INTERVAL '6 months');

-- Add indexes
CREATE INDEX time_series_record_main_entity_id_idx ON time_series_record(main_entity_id);
CREATE INDEX time_series_record_parent_id_idx ON time_series_record(parent_id);
CREATE INDEX parent_event_main_entity_id_idx ON parent_event(main_entity_id);
CREATE INDEX parent_event_parent_id_idx ON parent_event(parent_id);

CREATE OR REPLACE FUNCTION update_parent_status(
    _parent_id bigint,
    _main_entity_id bigint,
    _new_status text
) RETURNS void AS $$
BEGIN
    INSERT INTO parent_event (parent_id, main_entity_id, status)
    VALUES (_parent_id, _main_entity_id, _new_status);

    UPDATE parent_record SET status = _new_status WHERE id = _parent_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_parent_and_timeseries() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO time_series_record (parent_id, main_entity_id, amount)
    VALUES (NEW.id, NEW.main_entity_id, NEW.amount);

    PERFORM update_parent_status(NEW.id, NEW.main_entity_id, NEW.status);

    RETURN NEW;
END;
$$;

CREATE TRIGGER parent_record_insert_trigger
AFTER INSERT ON parent_record
FOR EACH ROW
EXECUTE PROCEDURE update_parent_and_timeseries();

CREATE OR REPLACE FUNCTION simulate(
    _main_id bigint,
    _amount numeric
) RETURNS void AS $$
BEGIN
    INSERT INTO parent_record (main_entity_id, amount, status)
    VALUES (_main_id, _amount, 'created');

    -- Get the last inserted id
    PERFORM update_parent_status(currval('parent_record_id_seq'), _main_id, 'pending');
    PERFORM update_parent_status(currval('parent_record_id_seq'), _main_id, 'completed');
    PERFORM update_parent_status(currval('parent_record_id_seq'), _main_id, 'settled');
END;
$$ LANGUAGE plpgsql;

-- Setup test data
CREATE OR REPLACE FUNCTION setup_test_data() RETURNS SETOF bigint AS $$
BEGIN
    -- Clean up previous test data if exists
    TRUNCATE parent_event, time_series_record, parent_record, main_entity CASCADE;

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

