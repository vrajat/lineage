CREATE TABLE query_stats (
    db varchar(100),
    user varchar(100),
    query_group varchar(100),
    timestamp_hour timestamp,
    min_duration float,
    avg_duration float,
    median_duration float,
    p75_duration float,
    p90_duration float,
    p95_duration float,
    p99_duration float,
    p999_duration float,
    max_duration float
);