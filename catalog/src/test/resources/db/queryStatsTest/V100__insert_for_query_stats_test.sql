insert into pg_user(usename, usesysid, usecreatedb, usesuper, usecatupd, passwd, valuntil, useconfig)
    values('inviscid', 101, 1, 1, 1, 'passwd', '2019-01-01 00:00:00', '' );

insert into stl_wlm_query(userid, xid, task, query, service_class, slot_count, service_class_start_time,
    queue_start_time, queue_end_time, total_queue_time, exec_start_time, exec_end_time, total_exec_time,
    service_class_end_time, final_state, est_peak_mem) values (
    101, 1001, 1001, 5001, 1, 1, '2018-09-13 12:00:00', '2018-09-13 12:00:00', '2018-09-13 13:00:00',
    60, '2018-09-13 12:15:00', '2018-09-13 12:30:00', 15, '2018-09-13 14:00:00', 'CLOSED', 100);

insert into stl_query(userid, query, label, xid, pid, database, querytext, starttime, endtime, aborted, insert_pristine)
    values(101, 5001, 'label', 1001, 1201, 'public', 'select count(*) from metrics',
    '2018-09-13 12:01:00', '2018-09-13 12:04:00', 0, 0);