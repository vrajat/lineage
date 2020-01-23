create table PG_USER (
    usename varchar,
    usesysid int,
    usecreatedb boolean,
    usesuper boolean,
    usecatupd boolean,
    passwd varchar,
    valuntil timestamp,
    useconfig varchar
)