create table STL_QUERY (
    userid int,
    query int,
    label char(15),
    xid int,
    pid int,
    database char(32),
    querytext varchar(4000),
    starttime timestamp,
    endtime timestamp,
    aborted int,
    insert_pristine int
)