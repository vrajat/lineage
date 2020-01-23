package io.tokern.lineage.catalog.redshift;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.junit.jupiter.api.Assertions.*;

class RedshiftDbTest {
  private static final String url = "jdbc:h2:mem:RedshiftDbTest";

  private Connection h2db;

  @BeforeEach
  void setH2db(TestInfo testInfo) throws SQLException {
    String testName = testInfo.getDisplayName();
    String testLocation = testName.substring(0, testName.length() - 2);
    h2db = DriverManager.getConnection(url);
    Flyway flyway = new Flyway();
    flyway.setDataSource(url, "", "");
    flyway.setLocations("db/redshiftTestMigrations", "db/" + testLocation);

    flyway.migrate();
  }

  @AfterEach
  void tearDownH2db() throws SQLException {
    h2db.close();
  }

  @Test
  void checkTableListTest() throws SQLException {
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md = h2db.getMetaData();
    ResultSet rs = md.getTables(null, "PUBLIC", null, null);
    while (rs.next()) {
      tables.add(rs.getString(3));
    }

    List<String> expected = Arrays.asList("PG_USER", "STL_CONNECTION_LOG",
        "STL_QUERY", "STL_QUERYTEXT", "STL_WLM_QUERY", "STV_INFLIGHT", "STV_SESSIONS",
        "flyway_schema_history");
    assertIterableEquals(expected, tables);
  }

  @Disabled
  @Test
  void queryStatsTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = new RedshiftDb(url, "", "",
        metricRegistry);

    List<QueryStats> queryStatsList = redshiftDb.getQueryStats(true,
        LocalDateTime.of(2018, 9, 13, 12, 0, 0),
        LocalDateTime.of(2018, 9, 13, 13, 0, 0));

    assertEquals(1, queryStatsList.size());

    QueryStats queryStats = queryStatsList.get(0);

    assertEquals("public", queryStats.db);
    assertEquals("inviscid", queryStats.user);
    assertEquals("label", queryStats.queryGroup);
    assertEquals(LocalDateTime.of(2018, 9, 13, 12, 0), queryStats.timestampHour);
    assertEquals(0.000075, queryStats.minDuration);
    assertEquals(0.000075, queryStats.avgDuration);
    assertEquals(0, queryStats.medianDuration);
    assertEquals(0, queryStats.p75);
    assertEquals(0, queryStats.p90);
    assertEquals(0, queryStats.p95);
    assertEquals(0, queryStats.p99);
    assertEquals(0, queryStats.p999);
    assertEquals(0.000075, queryStats.maxDuration);
  }

  @Disabled
  @Test
  void userQueryTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = new RedshiftDb(url, "", "",
        metricRegistry);

    List<UserQuery> userQueries = redshiftDb.getQueries(
        LocalDateTime.of(2018, 9, 19, 11, 0, 0),
        LocalDateTime.of(2018, 9, 19, 16, 0, 0));

    UserQuery userQuery0 = userQueries.get(0);
    UserQuery userQuery1 = userQueries.get(1);

    assertEquals(2, userQueries.size());
    assertEquals(1793, userQuery0.queryId);
    assertEquals(100, userQuery0.userId);
    assertEquals(6919, userQuery0.transactionId);
    assertEquals(19644, userQuery0.pid);
    assertEquals(LocalDateTime.of(2018, 9,19, 13, 8,3, 419322000), userQuery0.startTime);
    assertEquals(LocalDateTime.of(2018, 9,19, 13, 46,51, 926214000), userQuery0.endTime);
    assertEquals(2329, userQuery0.duration);
    assertEquals("dev", userQuery0.db);
    assertEquals(false, userQuery0.aborted);
    assertEquals(200, userQuery0.query.length());

    assertEquals(1224, userQuery1.queryId);
    assertEquals(100, userQuery1.userId);
    assertEquals(5052, userQuery1.transactionId);
    assertEquals(15859, userQuery1.pid);
    assertEquals(LocalDateTime.of(2018, 9,19, 11, 50,42, 221579000), userQuery1.startTime);
    assertEquals(LocalDateTime.of(2018, 9,19, 11, 51,21, 252449000), userQuery1.endTime);
    assertEquals(39, userQuery1.duration);
    assertEquals("dev", userQuery1.db);
    assertEquals(false, userQuery1.aborted);
    assertEquals(647, userQuery1.query.length());
  }

  @Test
  void connectionTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = new RedshiftDb(url, "", "",
        metricRegistry);

    List<UserConnection> userConnections = redshiftDb.getUserConnections();

    UserConnection c = userConnections.get(0);
    assertEquals(1, userConnections.size());
    assertEquals(LocalDateTime.of(2018, 9, 13, 13, 0, 0), c.startTime);
    assertEquals(101, c.process);
    assertEquals("user", c.userName);
    assertEquals("168.9.1.1", c.remoteHost);
    assertEquals("26", c.remotePort);
    assertNull(c.pollTime);

  }

  @Test
  void runningQueriesTest() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RedshiftDb redshiftDb = new RedshiftDb(url, "", "",
        metricRegistry);

    List<RunningQuery> queries = redshiftDb.getRunningQueries();

    RunningQuery r = queries.get(0);

    assertEquals(1, r.userId);
    assertEquals(1, r.slice);
    assertEquals(101, r.queryId);
    assertEquals("label", r.label);
    assertEquals(10001, r.transactionId);
    assertEquals(202, r.pid);
    assertEquals(LocalDateTime.of(2018, 10, 2, 11, 0, 0), r.startTime);
    assertFalse(r.suspended);
  }
}