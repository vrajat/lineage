create table STL_WLM_QUERY (
    userid int,
    xid int,
    task int,
    query int,
    service_class int,
    slot_count int,
    service_class_start_time timestamp,
    queue_start_time timestamp,
    queue_end_time timestamp,
    total_queue_time int,
    exec_start_time timestamp,
    exec_end_time timestamp,
    total_exec_time int,
    service_class_end_time timestamp,
    final_state varchar,
    est_peak_mem int
)