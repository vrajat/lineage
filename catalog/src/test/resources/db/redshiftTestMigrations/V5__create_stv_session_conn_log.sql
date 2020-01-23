create table stl_connection_log(
    event varchar(50),
    recordtime timestamp,
    remotehost varchar(32),
    remoteport varchar(32),
    pid	integer,
    dbname varchar(50),
    username varchar(50),
    authmethod varchar(32),
    duration integer,
    sslversion varchar(50),
    sslcipher varchar(128),
    mtu	integer,
    sslcompression varchar(64),
    sslexpansion varchar(64),
    iamauthguid	varchar(36),
    application_name varchar(250),
);

create table stv_sessions(
    starttime timestamp,
    process	integer,
    user_name varchar(50),
    db_name	varchar(50),
);
