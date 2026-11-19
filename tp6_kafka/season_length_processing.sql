-- ============================================================
-- KAFKA / KSQLDB - SEASON LENGTH PROCESSING
-- ============================================================

-- 1. Configure offset reset
SET 'auto.offset.reset' = 'earliest';


-- 2. Create custom STRUCT type
CREATE TYPE season_length AS STRUCT<
    season_id INT,
    episode_count INT
>;

SHOW TYPES;


-- 3. Create titles table
CREATE TABLE titles (
    id INT PRIMARY KEY,
    title VARCHAR
) WITH (
    KAFKA_TOPIC = 'titles',
    PARTITIONS = 4,
    VALUE_FORMAT = 'JSON'
);


-- 4. Create production changes stream
CREATE STREAM production_changes (
    rowkey VARCHAR KEY,
    title_id INT,
    change_type VARCHAR,
    before season_length,
    after season_length,
    created_at VARCHAR
) WITH (
    KAFKA_TOPIC = 'production_changes',
    PARTITIONS = 4,
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'created_at',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'
);


-- 5. Inspect created tables and streams
SHOW TABLES;
SHOW STREAMS;

DESCRIBE titles;


-- 6. Insert movie titles
INSERT INTO titles (id, title)
VALUES (1, 'Stranger Things');

INSERT INTO titles (id, title)
VALUES (2, 'The Dark Knight');

INSERT INTO titles (id, title)
VALUES (3, 'The Witcher');


-- 7. Insert production change events

-- Stranger Things: Season 1, episode count 12 -> 8
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key1',
    1,
    'season_length',
    STRUCT(season_id := 1, episode_count := 12),
    STRUCT(season_id := 1, episode_count := 8),
    '2021-02-08 11:30:00'
);

-- Stranger Things: Season 1, episode count 8 -> 10
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key2',
    1,
    'season_length',
    STRUCT(season_id := 1, episode_count := 8),
    STRUCT(season_id := 1, episode_count := 10),
    '2021-02-08 11:45:00'
);

-- The Dark Knight: Season 2, episode count 6 -> 4
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key3',
    2,
    'season_length',
    STRUCT(season_id := 2, episode_count := 6),
    STRUCT(season_id := 2, episode_count := 4),
    '2022-06-10 09:00:00'
);

-- The Witcher: Season 1, episode count 8 -> 6
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key4',
    3,
    'season_length',
    STRUCT(season_id := 1, episode_count := 8),
    STRUCT(season_id := 1, episode_count := 6),
    '2023-05-20 15:00:00'
);

-- A release date change
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key5',
    2,
    'release_date',
    NULL,
    NULL,
    '2023-03-01 10:00:00'
);

-- A season budget change
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key6',
    3,
    'season_budget',
    NULL,
    NULL,
    '2023-06-01 08:00:00'
);


-- 8. Query production changes before a specific date
SELECT *
FROM production_changes
WHERE created_at < '2023-04-14 12:00:00'
EMIT CHANGES;


-- 9. Filter season-related changes
SELECT *
FROM production_changes
WHERE change_type LIKE 'season%'
EMIT CHANGES;


-- 10. Extract season length information from STRUCT fields
CREATE STREAM season_length_changes
WITH (
    KAFKA_TOPIC = 'season_length_changes',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4,
    REPLICAS = 1
) AS
SELECT
    ROWKEY,
    title_id,
    IFNULL(after->season_id, before->season_id) AS season_id,
    before->episode_count AS old_episode_count,
    after->episode_count AS new_episode_count,
    created_at
FROM production_changes
WHERE change_type = 'season_length'
EMIT CHANGES;


-- 11. Inspect the new stream
SHOW STREAMS;

SELECT title_id
FROM season_length_changes
EMIT CHANGES;


-- 12. Join season changes with the titles table
SELECT t.title
FROM season_length_changes s
INNER JOIN titles t
    ON s.title_id = t.id
EMIT CHANGES;


-- 13. Create an enriched stream
CREATE STREAM season_length_changes_enriched
WITH (
    KAFKA_TOPIC = 'season_length_changes_enriched',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4,
    REPLICAS = 1,
    TIMESTAMP = 'created_at',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'
) AS
SELECT
    s.title_id,
    t.title,
    s.season_id,
    s.old_episode_count,
    s.new_episode_count,
    s.created_at
FROM season_length_changes s
INNER JOIN titles t
    ON s.title_id = t.id
EMIT CHANGES;


-- 14. Create an aggregation table
-- KEY_FORMAT = JSON is required because GROUP BY
-- produces a composite key (title_id + title).

CREATE TABLE season_length_change_counts
WITH (
    KAFKA_TOPIC = 'season_length_change_counts',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 4,
    REPLICAS = 1
) AS
SELECT
    title_id,
    title,
    COUNT(*) AS change_count,
    LATEST_BY_OFFSET(new_episode_count) AS episode_count
FROM season_length_changes_enriched
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY title_id, title
EMIT CHANGES;


-- 15. Inspect aggregation results
SELECT *
FROM season_length_change_counts
EMIT CHANGES;


-- 16. Inspect enriched stream
SELECT *
FROM season_length_changes_enriched
EMIT CHANGES;


-- 17. Insert additional production change events

-- Stranger Things: episode count 10 -> 9
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key7',
    1,
    'season_length',
    STRUCT(season_id := 1, episode_count := 10),
    STRUCT(season_id := 1, episode_count := 9),
    '2023-07-01 10:00:00'
);

-- Stranger Things: episode count 9 -> 7
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key8',
    1,
    'season_length',
    STRUCT(season_id := 1, episode_count := 9),
    STRUCT(season_id := 1, episode_count := 7),
    '2023-07-01 10:20:00'
);

-- The Dark Knight: episode count 4 -> 5
INSERT INTO production_changes
    (rowkey, title_id, change_type, before, after, created_at)
VALUES (
    'key9',
    2,
    'season_length',
    STRUCT(season_id := 2, episode_count := 4),
    STRUCT(season_id := 2, episode_count := 5),
    '2023-07-01 11:00:00'
);


-- 18. Verify enriched stream after new events
SELECT *
FROM season_length_changes_enriched
EMIT CHANGES;


-- 19. Verify aggregation results after new events
SELECT *
FROM season_length_change_counts
EMIT CHANGES;