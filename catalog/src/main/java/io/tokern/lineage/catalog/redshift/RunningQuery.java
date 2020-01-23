package io.tokern.lineage.catalog.redshift;

import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.time.LocalDateTime;

public class RunningQuery implements Jdbi {
  public final int userId;
  public final int slice;
  public final int queryId;
  public final String label;
  public final long transactionId;
  public final int pid;
  public final LocalDateTime startTime;
  public final boolean suspended;
  public LocalDateTime pollTime;

  /**
   * Create a new RunningQuery row
   * @param userId user id who submitted the query
   * @param slice slice id of the running query
   * @param queryId query id. Can be used to join with stl_querytext
   * @param label Label of the query
   * @param transactionId Transaction ID of the query
   * @param pid Process ID of the query
   * @param startTime Start time of the query
   * @param suspended Is query suspended
   */
  @JdbiConstructor
  public RunningQuery(int userId, int slice, int queryId, String label, long transactionId,
                      int pid, LocalDateTime startTime, boolean suspended) {
    this.userId = userId;
    this.slice = slice;
    this.queryId = queryId;
    this.label = label;
    this.transactionId = transactionId;
    this.pid = pid;
    this.startTime = startTime;
    this.suspended = suspended;
    this.pollTime = null;
  }

  public static final String extractQuery = "select\n"
      + "userid as user_id, slice, query as query_id, label, xid as transaction_id, pid, "
      + "starttime as start_time, suspended \n"
      + "from STV_INFLIGHT";

  public static final String insertQuery = "insert into running_queries values( "
      + ":userId, :slice, :queryId, :label, :transactionId, :pid, :startTime, :suspended, "
      + ":pollTime)";
}
