create table running_queries (
    user_id int,
    slice int,
    query_id int,
    label varchar(100),
    transaction_id int,
    pid int,
    start_time timestamp,
    suspended int,
    poll_time timestamp
);