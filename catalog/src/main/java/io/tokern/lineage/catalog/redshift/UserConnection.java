package io.tokern.lineage.catalog.redshift;

import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.time.LocalDateTime;

public class UserConnection implements Jdbi {
  public LocalDateTime pollTime;
  public final LocalDateTime startTime;
  public final int process;
  public final String userName;
  public final String remoteHost;
  public final String remotePort;

  /**
   * Construct a UserConnection row.
   * @param startTime Start time of the connection
   * @param process PID of the connection
   * @param userName User connected on this connection
   * @param remoteHost Remote host of the connection
   * @param remotePort Remote port of the connection
   */
  @JdbiConstructor
  public UserConnection(LocalDateTime startTime, int process,
                        String userName, String remoteHost, String remotePort) {
    this.startTime = startTime;
    this.process = process;
    this.userName = userName;
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
  }

  public static final String extractQuery = "select distinct\n"
      + " starttime as start_time\n"
      + " , process\n"
      + " , user_name\n"
      + " , remotehost as remote_host\n"
      + " , remoteport as remote_port\n"
      + "from stv_sessions\n"
      + "left join stl_connection_log\n"
      + " on pid = process\n"
      + " and DATEDIFF(second, starttime, recordtime) < 1\n"
      + "order by starttime desc";

  public static final String insertQuery = "insert into user_connections values ("
      + ":pollTime, :startTime, :process, :userName, :remoteHost, :remotePort)";
}
