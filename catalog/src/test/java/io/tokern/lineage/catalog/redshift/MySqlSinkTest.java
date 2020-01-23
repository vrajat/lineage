package io.tokern.lineage.catalog.redshift;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MySqlSinkTest {
  private static final String url = "jdbc:h2:mem:io.dblint.metricsink.sinks.MySqlSinkTest";

  private MetricRegistry metricRegistry;
  private Connection h2db;
  private MySqlSink mySqlSink;

  @BeforeEach
  void setmysqlsink() throws SQLException {
    h2db = DriverManager.getConnection(url);
    metricRegistry = new MetricRegistry();
    mySqlSink = new MySqlSink(url, "", "", metricRegistry);
    mySqlSink.initialize();
  }

  @AfterEach
  void dropAllObjects() throws SQLException {
    Statement statement = h2db.createStatement();
    statement.execute("DROP ALL OBJECTS");
    statement.close();
    h2db.close();
  }

  @Test
  void migrationTest() throws SQLException {
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = h2db.getMetaData();
    ResultSet rs = md.getTables(null, "PUBLIC", null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = new ArrayList<>();
    expected.add("BAD_USER_QUERIES");
    expected.add("QUERY_STATS");
    expected.add("RUNNING_QUERIES");
    expected.add("USER_CONNECTIONS");
    expected.add("flyway_schema_history");
    Assertions.assertIterableEquals(expected, tables);
  }

  @Test
  void insertOneQueryStat() throws SQLException {
    QueryStats queryStats = new QueryStats("db", "user", "user_group", LocalDateTime.now(),
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6,
        0.9, 0.999, 1);
    mySqlSink.insertQueryStats(queryStats);

    Statement statement = h2db.createStatement();
    ResultSet resultSet = statement.executeQuery("select db, user, query_group, timestamp_hour, "
          + "min_duration, avg_duration, median_duration, p75_duration, p90_duration, p95_duration,"
          + "p99_duration, p999_duration, max_duration from PUBLIC.query_stats");

    resultSet.next();

    assertEquals(queryStats.db, resultSet.getString("db"));
    assertEquals(queryStats.user, resultSet.getString("user"));
    assertEquals(queryStats.queryGroup, resultSet.getString("query_group"));
    assertEquals(queryStats.timestampHour,
        resultSet.getTimestamp("timestamp_hour").toLocalDateTime());
    assertEquals(queryStats.minDuration, resultSet.getDouble("min_duration"));
    assertEquals(queryStats.avgDuration, resultSet.getDouble("avg_duration"));
    assertEquals(queryStats.medianDuration, resultSet.getDouble("median_duration"));
    assertEquals(queryStats.p75, resultSet.getDouble("p75_duration"));
    assertEquals(queryStats.p90, resultSet.getDouble("p90_duration"));
    assertEquals(queryStats.p95, resultSet.getDouble("p95_duration"));
    assertEquals(queryStats.p99, resultSet.getDouble("p99_duration"));
    assertEquals(queryStats.p999, resultSet.getDouble("p999_duration"));
    assertEquals(queryStats.maxDuration, resultSet.getDouble("max_duration"));
  }

  @Test
  void insertOneUserQuery() throws SQLException {
    UserQuery userQuery = new UserQuery(1, 1, 1,1, LocalDateTime.now(),
        LocalDateTime.now(), 10L, "db", false, "select something");

    mySqlSink.insertBadQueries(userQuery);

    Statement statement = h2db.createStatement();
    ResultSet resultSet = statement.executeQuery("select query_id, user_id, transaction_id, pid, "
          + "start_time, end_time, duration, db, aborted, query from PUBLIC.bad_user_queries");

    resultSet.next();

    assertEquals(userQuery.queryId, resultSet.getInt("query_id"));
    assertEquals(userQuery.userId, resultSet.getInt("user_id"));
    assertEquals(userQuery.transactionId, resultSet.getInt("transaction_id"));
    assertEquals(userQuery.pid, resultSet.getInt("pid"));
    assertEquals(userQuery.startTime,
        resultSet.getTimestamp("start_time").toLocalDateTime());
    assertEquals(userQuery.endTime,
        resultSet.getTimestamp("end_time").toLocalDateTime());
    assertEquals(userQuery.duration, resultSet.getDouble("duration"));
    assertEquals(userQuery.db, resultSet.getString("db"));
    assertEquals(userQuery.aborted, resultSet.getBoolean("aborted"));
    assertEquals(userQuery.query, resultSet.getString("query"));
  }

  @Test
  void insertOneConnection() throws SQLException {
    UserConnection userConnection
        = new UserConnection(LocalDateTime.now(),
        101, "user", "168.9.1.1", "26" );
    userConnection.pollTime = LocalDateTime.now();
    mySqlSink.insertConnections(userConnection);

    Statement statement = h2db.createStatement();
    ResultSet resultSet = statement.executeQuery("select poll_time, start_time, process, user_name"
          + ", remote_host, remote_port from PUBLIC.user_connections");

    resultSet.next();

    assertEquals(userConnection.pollTime, resultSet.getTimestamp("poll_time").toLocalDateTime());
    assertEquals(userConnection.startTime, resultSet.getTimestamp("start_time").toLocalDateTime());
    assertEquals(userConnection.process, resultSet.getInt("process"));
    assertEquals(userConnection.userName, resultSet.getString("user_name"));
    assertEquals(userConnection.remoteHost, resultSet.getString("remote_host"));
    assertEquals(userConnection.remotePort, resultSet.getString("remote_port"));
  }

  @Test
  void insertRunningQuery() throws SQLException {
    RunningQuery query = new RunningQuery(1, 1, 101, "label", 10001, 909,
        LocalDateTime.now(), false);
    query.pollTime = LocalDateTime.now();
    mySqlSink.insertRunningQueries(query);

    Statement statement = h2db.createStatement();
    ResultSet resultSet = statement.executeQuery("select user_id, slice, query_id, label, "
          + "transaction_id, pid, start_time, suspended, poll_time from PUBLIC.running_queries");

    resultSet.next();

    assertEquals(query.userId, resultSet.getInt("user_id"));
    assertEquals(query.slice, resultSet.getInt("slice"));
    assertEquals(query.queryId, resultSet.getInt("query_id"));
    assertEquals(query.label, resultSet.getString("label"));
    assertEquals(query.transactionId, resultSet.getInt("transaction_id"));
    assertEquals(query.pid, resultSet.getInt("pid"));
    assertEquals(query.startTime, resultSet.getTimestamp("start_time").toLocalDateTime());
    assertEquals(query.pollTime, resultSet.getTimestamp("poll_time").toLocalDateTime());
    assertEquals(query.suspended, resultSet.getBoolean("suspended"));
  }

  @Test
  void queryStatMetricsTest() {
    QueryStats queryStats = new QueryStats("db", "user", "user_group", LocalDateTime.now(),
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6,
        0.9, 0.999, 1);
    mySqlSink.insertQueryStats(queryStats);
    SortedMap<String, Timer> timers = metricRegistry.getTimers();

    assertEquals(1, timers.size());
    assertEquals("inviscid.query.raw", timers.firstKey());
  }

  @Test
  void badQueriesMetricsTest() {
    UserQuery userQuery = new UserQuery(1, 1, 1,1, LocalDateTime.now(),
        LocalDateTime.now(), 10L, "db", false, "select something");

    mySqlSink.insertBadQueries(userQuery);

    SortedMap<String, Timer> timers = metricRegistry.getTimers();

    assertEquals(1, timers.size());
    assertEquals("inviscid.query.raw", timers.firstKey());
  }

  @Test
  void userConnectionMetricsTest() {
    UserConnection userConnection
        = new UserConnection(LocalDateTime.now(),
        101, "user", "168.9.1.1", "26" );
    userConnection.pollTime = LocalDateTime.now();

    mySqlSink.insertConnections(userConnection);

    SortedMap<String, Timer> timers = metricRegistry.getTimers();

    assertEquals(1, timers.size());
    assertEquals("inviscid.query.raw", timers.firstKey());
  }
}