-- DuckDB seed data for E2E tests
-- Covers: BigInt, String, Float, Date, Time, Interval, Timestamp, JSON

CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    value DOUBLE,
    event_date DATE,
    event_time TIME,
    duration INTERVAL,
    metadata JSON,
    created_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO events VALUES
    (1, 'Conference', 100.50, '2025-03-15', '09:00:00', INTERVAL '2 hours', '{"type": "tech", "attendees": 500}', '2025-03-15 09:00:00+00'),
    (2, 'Workshop', 50.00, '2025-03-16', '14:00:00', INTERVAL '3 hours', '{"type": "hands-on", "attendees": 50}', '2025-03-16 14:00:00+00'),
    (3, 'Meetup', 0.00, '2025-04-01', '18:30:00', INTERVAL '1 hour 30 minutes', '{"type": "social", "attendees": 30}', '2025-04-01 18:30:00+00'),
    (4, 'Hackathon', 200.00, '2025-04-10', '08:00:00', INTERVAL '24 hours', '{"type": "competition", "attendees": 100}', '2025-04-10 08:00:00+00'),
    (5, 'Webinar', 25.00, '2025-05-01', '11:00:00', INTERVAL '45 minutes', '{"type": "online", "attendees": 200}', '2025-05-01 11:00:00+00');

CREATE TABLE event_tags (
    event_id BIGINT REFERENCES events(id),
    tag VARCHAR,
    PRIMARY KEY (event_id, tag)
);

INSERT INTO event_tags VALUES
    (1, 'technology'),
    (1, 'networking'),
    (2, 'technology'),
    (3, 'social'),
    (4, 'technology'),
    (4, 'competition');

CREATE TABLE items (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    specs STRUCT(cpu VARCHAR, ram_gb INTEGER, storage_gb INTEGER),
    tags JSON,
    created_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO items VALUES
    (1, 'Laptop', {'cpu': 'M2', 'ram_gb': 16, 'storage_gb': 512}, '{"color":"silver"}', '2025-01-15 10:00:00+00'),
    (2, 'Desktop', {'cpu': 'i9', 'ram_gb': 64, 'storage_gb': 2000}, '{"color":"black"}', '2025-02-20 14:00:00+00'),
    (3, 'Tablet', {'cpu': 'A15', 'ram_gb': 8, 'storage_gb': 256}, '{"color":"gray"}', '2025-03-10 09:00:00+00');

-- json_field_demo: dataset for JSONFieldFilter coverage (path / isNull / coalesce / typed sub-filters)
CREATE TABLE json_field_demo (
    id BIGINT PRIMARY KEY,
    data JSON
);

INSERT INTO json_field_demo VALUES
    (1,  '{"user":{"age":31,"country":"DE"},"metrics":{"score":0.92},"shape":{"type":"Point","coordinates":[10,51]},"shape_wkt":"POINT(10 51)","shape_ewkt":"SRID=4326;POINT(10 51)","event":{"at":"2024-06-01T10:00:00Z"}}'),
    (2,  '{"user":{"age":65,"country":"FR"},"metrics":{"score":0.50},"shape":{"type":"Point","coordinates":[2,48]},"shape_wkt":"POINT(2 48)","shape_ewkt":"SRID=4326;POINT(2 48)","event":{"at":"2024-06-02T11:00:00Z"}}'),
    (3,  '{"user":{"age":14,"country":"DE"},"metrics":{"score":0.10},"shape":{"type":"Point","coordinates":[11,52]},"event":{"at":"2024-06-03T09:00:00Z"}}'),
    (4,  '{"user":{"age":null,"country":"DE"},"metrics":{"score":null},"shape":null,"event":{"at":null}}'),
    (5,  '{"user":{"country":"FR"},"metrics":{"score":0.75},"event":{"at":"2024-06-05T12:00:00Z"}}'),
    (6,  NULL),
    (7,  '{}'),
    (8,  '{"user":{"age":17,"country":"DE"},"metrics":{"score":0.65},"shape":{"type":"Point","coordinates":[10.1,51.1]},"event":{"at":"2024-06-08T08:00:00Z"}}'),
    (9,  '{"user":{"age":40,"country":"DE"},"metrics":{"score":0.20},"shape":{"type":"Point","coordinates":[12,53]},"event":{"at":"2024-06-09T14:00:00Z"}}'),
    (10, '{"user":{"age":18,"country":"FR"},"metrics":{"score":0.50},"shape":{"type":"Point","coordinates":[2.5,48.5]},"event":{"at":"2024-06-10T15:00:00Z"}}'),
    -- Rows 11-12 carry extra paths for typed sub-filter coverage
    -- (bigInt, float, bool, date, time, dateTime, interval, timestamp).
    (11, '{"user":{"age":35,"country":"US"},"metrics":{"score":0.80},"shape":{"type":"Point","coordinates":[10.5,51.5]},"event":{"at":"2024-06-11T10:00:00Z","local_dt":"2024-06-11T10:00:00"},"account":{"balance":5000000000},"flags":{"premium":true},"signup":{"day":"2024-01-15"},"lunch":{"at_time":"12:30:00"},"subscription":{"duration":"90 minutes"}}'),
    (12, '{"user":{"age":22,"country":"US"},"metrics":{"score":0.40},"shape":{"type":"Point","coordinates":[10.6,51.6]},"event":{"at":"2024-06-12T10:00:00Z","local_dt":"2024-06-12T10:00:00"},"account":{"balance":2500000000},"flags":{"premium":false},"signup":{"day":"2024-02-20"},"lunch":{"at_time":"13:00:00"},"subscription":{"duration":"2 hours"}}');
