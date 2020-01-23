package io.tokern.lineage.catalog.redshift;

import com.codahale.metrics.MetricRegistry;
import io.tokern.lineage.catalog.util.JdbiTimer;
import org.flywaydb.core.Flyway;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.FieldMapper;

public class MySqlSink {
  final String url;
  final String user;
  final String password;
  final Flyway flyway;
  final Jdbi jdbi;

  public MySqlSink(String url, String user, String password,
                   MetricRegistry metricRegistry) {
    this(url, user, password, metricRegistry, new Flyway());
  }

  /**
   * Create a MySqlSink for Redshift metrics.
   *
   * @param url URL of the MySQL Database
   * @param user user of the MySQL Database
   * @param password password of the MySQL database
   * @param metricRegistry MetricRegistry to store JDBI metrics
   * @param flyway Migrations library to setup the MySQL database
   */

  public MySqlSink(String url, String user, String password,
                   MetricRegistry metricRegistry, Flyway flyway) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.flyway = flyway;
    this.jdbi = Jdbi.create(url, user, password);
    this.jdbi.setSqlLogger(new JdbiTimer(metricRegistry));
  }

  /**
   * Setup MySQL with tables to store metrics.
   */
  public void initialize() {
    flyway.setDataSource(url, user, password);
    flyway.setLocations("db/redshiftMigrations");
    flyway.migrate();
  }

  public void close() {
    flyway.clean();
  }

  /**
   * Insert one QueryStat row into query_stats table in MySQL.
   * @param queryStats A POJO of queryStats
   */
  public void insertQueryStats(QueryStats queryStats) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(QueryStats.class));
      handle.createUpdate("insert into query_stats(db, user, query_group, timestamp_hour, "
          + "min_duration, avg_duration, median_duration, p75_duration, p90_duration, p95_duration,"
          + "p99_duration, p999_duration, max_duration) values ("
          + ":db, :user, :queryGroup, :timestampHour, :minDuration, :avgDuration, :medianDuration, "
          + ":p75, :p90, :p95, :p99, :p999, :maxDuration)")
          .bindFields(queryStats)
          .execute();
    });
  }

  /**
   * Insert one UserQuery row into bad_queries table in MySQL.
   * @param userQuery A POJO of UserQuery
   */
  public void insertBadQueries(UserQuery userQuery) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserQuery.class));
      handle.createUpdate(UserQuery.insertQuery)
          .bindFields(userQuery)
          .execute();
    });
  }

  /**
   * Insert one userConnection row into connections table in MySQL.
   * @param userConnection A POJO of UserConnection
   */
  public void insertConnections(UserConnection userConnection) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(UserConnection.class));
      handle.createUpdate(UserConnection.insertQuery)
          .bindFields(userConnection)
          .execute();
    });
  }

  /**
   * Insert one RunningQuery row into running_queries table in MySQL.
   * @param query A POJO of RunningQuery
   */
  public void insertRunningQueries(RunningQuery query) {
    jdbi.useHandle(handle -> {
      handle.registerRowMapper(FieldMapper.factory(RunningQuery.class));
      handle.createUpdate(RunningQuery.insertQuery)
          .bindFields(query)
          .execute();
    });
  }
}
