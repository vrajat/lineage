package io.tokern.lineage.catalog.redshift;

import com.codahale.metrics.MetricRegistry;
import io.tokern.lineage.catalog.util.JdbiTimer;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;

import java.time.LocalDateTime;
import java.util.List;

public class RedshiftDb implements Agent {
  final String url;
  final String user;
  final String password;
  final Jdbi jdbi;

  /**
   * Manage a connection to a Redshift database.
   * @param url URL of the Redshift database
   * @param user User of the Redshift database
   * @param password Password of the Redshift database
   * @param metricRegistry MetricRegistry for JDBI metrics
   */
  public RedshiftDb(String url, String user, String password,
                    MetricRegistry metricRegistry) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.jdbi = Jdbi.create(url, user, password);
    jdbi.setSqlLogger(new JdbiTimer(metricRegistry));
  }

  /**
   * Get QueryStats for a specific time period from Redshift.
   * @param inTest Test parameter to choose a H2 compliant sql
   * @return List of QueryStats
   */
  public List<QueryStats> getQueryStats(boolean inTest,
                                        LocalDateTime rangeStart, LocalDateTime rangeEnd) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(QueryStats.class));
      return handle.createQuery(inTest ? QueryStats.getExtractQueryinTest(rangeStart, rangeEnd)
          : QueryStats.getExtractQuery(rangeStart, rangeEnd))
          .mapTo(QueryStats.class)
          .list();
    });
  }

  /**
   * Get all UserQueries for a specific time period from RedShift.
   * @param rangeStart Start time of time window
   * @param rangeEnd End time of time window
   * @return List of User Queries
   */
  @Override
  public List<UserQuery> getQueries(LocalDateTime rangeStart, LocalDateTime rangeEnd) {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(UserQuery.class));
      return handle.createQuery(UserQuery.getExtractQuery(rangeStart, rangeEnd))
          .mapTo(UserQuery.class)
          .list();
    });
  }

  /**
   * Get all connections currently active in Redshift.
   * @return List of UserConnection
   */
  public List<UserConnection> getUserConnections() {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(UserConnection.class));
      return handle.createQuery(UserConnection.extractQuery)
          .mapTo(UserConnection.class)
          .list();
    });
  }

  /**
   * Get all running queries in Redshift.
   * @return List of running queries
   */
  public List<RunningQuery> getRunningQueries() {
    return jdbi.withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(RunningQuery.class));
      return handle.createQuery(RunningQuery.extractQuery)
          .mapTo(RunningQuery.class)
          .list();
    });
  }
}
