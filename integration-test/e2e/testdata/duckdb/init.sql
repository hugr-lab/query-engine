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
