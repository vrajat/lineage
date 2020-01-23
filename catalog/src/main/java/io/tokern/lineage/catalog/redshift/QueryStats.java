package io.tokern.lineage.catalog.redshift;

import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.time.LocalDateTime;

public class QueryStats implements Jdbi {

  public final String db;
  public final String user;

  public final String queryGroup;

  public final LocalDateTime timestampHour;
  public final double minDuration;

  public final double avgDuration;

  public final double medianDuration;

  public final double p75;
  public final double p90;
  public final double p95;
  public final double p99;
  public final double p999;

  @ColumnName("max_duration")
  public final double maxDuration;

  /**
   * QueryStats represents Query Stats info in a database.
   *
   * @param db DB of the sql group
   * @param user User of the sql group
   * @param queryGroup Label of the sql group
   * @param timestampHour Timestamp normalized to hour of aggregation
   * @param minDuration Min. duration in the sql group
   * @param avgDuration Average duration in the sql group
   * @param medianDuration Median duration in the sql group
   * @param p75 75th percentile duration in the sql group
   * @param p90 90th percentile duration in the sql group
   * @param p95 95th percentile duration in the sql group
   * @param p99 99th percentile duration in the sql group
   * @param p999 999th percentile duration in the sql group
   * @param maxDuration Max duration in the sql group
   */
  @JdbiConstructor
  public QueryStats(String db,
                    String user,
                    String queryGroup,
                    LocalDateTime timestampHour,
                    double minDuration,
                    double avgDuration,
                    double medianDuration,
                    double p75,
                    double p90,
                    double p95,
                    double p99,
                    double p999,
                    double maxDuration) {
    this.db = db;
    this.user = user;
    this.queryGroup = queryGroup;
    this.timestampHour = timestampHour;
    this.minDuration = minDuration;
    this.avgDuration = avgDuration;
    this.medianDuration = medianDuration;
    this.p75 = p75;
    this.p90 = p90;
    this.p95 = p95;
    this.p99 = p99;
    this.p999 = p999;
    this.maxDuration = maxDuration;
  }

  static String getExtractQuery(LocalDateTime rangeStart, LocalDateTime rangeEnd) {
    return String.format(
        extractQueryTemplate,
        rangeStart.format(dateTimeFormatter), rangeEnd.format(dateTimeFormatter),
        extractQueryCalculatePercentileinRedshift, aggregatePercentileinRedshift
    );
  }

  static String getExtractQueryinTest(LocalDateTime rangeStart, LocalDateTime rangeEnd) {
    return String.format(extractQueryTemplate,
        rangeStart.format(dateTimeFormatter), rangeEnd.format(dateTimeFormatter),
        "", aggregatePercentileinH2);
  }

  // Ref: https://gist.github.com/iconara/3523d89306153eb2ffaf
  private static String extractQueryTemplate = "WITH\n"
      + "durations1 AS (\n"
      + "  SELECT\n"
      + "    TRIM(database) AS db,\n"
      + "    TRIM(u.usename) AS \"user\",\n"
      + "    TRIM(label) AS query_group,\n"
      + "    DATE_TRUNC('hour', endtime) AS timestamp_hour,\n"
      + "    -- total_queue_time/1000000.0 AS duration,\n"
      + "    -- total_exec_time/1000000.0 AS duration,\n"
      + "    (total_queue_time + total_exec_time)/1000000.0 AS duration\n"
      + "  FROM stl_query q, stl_wlm_query w, pg_user u\n"
      + "  WHERE q.query = w.query\n"
      + "    AND q.userid = u.usesysid\n"
      + "    AND aborted = 0\n"
      + "    AND endtime between '%s' and '%s'\n"
      + "),\n"
      + "durations2 AS (\n"
      + "  SELECT\n"
      + "    db,\n"
      + "    \"user\",\n"
      + "    query_group,\n"
      + "    timestamp_hour,\n"
      + "    duration\n"
      + "  %s"
      + "  FROM durations1\n"
      + ")\n"
      + "SELECT\n"
      + "  db,\n"
      + "  \"user\",\n"
      + "  query_group,\n"
      + "  timestamp_hour,\n"
      + "  MIN(duration) AS min_duration,\n"
      + "  AVG(duration) AS avg_duration,\n"
      + "  %s"
      + "  MAX(duration) AS max_duration\n"
      + "FROM durations2\n"
      + "GROUP BY 1, 2, 3, 4\n"
      + "ORDER BY 1, 2, 3, 4;";

  private static String extractQueryCalculatePercentileinRedshift = ","
        + "    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS median_duration,\n"
        + "    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS p75,\n"
        + "    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS p90,\n"
        + "    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS p95,\n"
        + "    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS p99,\n"
        + "    PERCENTILE_CONT(0.999) WITHIN GROUP (ORDER BY duration) "
        + "OVER (PARTITION BY db, \"user\", query_group, timestamp_hour) AS p999\n";

  private static String aggregatePercentileinRedshift =
      "  MAX(median_duration) AS median_duration,\n"
        + "  MAX(p75) AS p75,\n"
        + "  MAX(p90) AS p90,\n"
        + "  MAX(p95) AS p95,\n"
        + "  MAX(p99) AS p99,\n"
        + "  MAX(p999) AS p999,\n";

  private static String aggregatePercentileinH2 = "  0 AS median_duration,\n"
        + "  0 AS p75,\n"
        + "  0 AS p90,\n"
        + "  0 AS p95,\n"
        + "  0 AS p99,\n"
        + "  0 AS p999,\n";
}
