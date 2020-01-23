create table stv_inflight (
    userid int,
    slice int,
    query int,
    label varchar(30),
    xid int,
    pid int,
    starttime timestamp,
    suspended int
);