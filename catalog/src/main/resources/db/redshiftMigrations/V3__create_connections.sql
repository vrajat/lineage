create table user_connections (
    poll_time timestamp,
    start_time timestamp,
    process int,
    user_name varchar(100),
    remote_host varchar(100),
    remote_port varchar(32)
);