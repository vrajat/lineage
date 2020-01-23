package io.tokern.lineage.catalog.redshift;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.tokern.lineage.catalog.util.MetricAgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedshiftCsv implements Agent {
  private static Logger logger = LoggerFactory.getLogger(RedshiftCsv.class);

  private final InputStream inputStream;
  private final Counter numSplits;
  private final Counter numQueries;
  private final Counter numSplitEndsWithSlash;

  /**
   * Process a CSV with redshift queries.
   * @param is InputStream that points to the CSV
   * @param registry Global metric registry that manages all metrics
   */
  public RedshiftCsv(InputStream is, MetricRegistry registry) {
    this.inputStream = is;
    this.numSplits = registry.counter(MetricRegistry.name("numSplits",
        "io", "dblint", "RedshiftCsv"));
    this.numQueries = registry.counter(MetricRegistry.name("numQueries",
        "io", "dblint", "RedshiftCsv"));
    this.numSplitEndsWithSlash = registry.counter(MetricRegistry.name("numSplitEndsWithSlash",
        "io", "dblint", "RedshiftCsv"));

  }

  /**
   * Parse CSV file and map to SplitUserQuery.
   * In SplitUserQuery, query text is not already combined
   * @return A list of SplitUserQueries
   * @throws IOException Exception thrown if source cannot be read successfully.
   */
  List<SplitUserQuery> getSplitQueries() throws IOException {
    CsvMapper mapper = new CsvMapper();
    CsvSchema schema = CsvSchema.emptySchema().withHeader();
    MappingIterator<SplitUserQuery> iterator = mapper.readerFor(SplitUserQuery.class).with(schema)
        .readValues(inputStream);

    List<SplitUserQuery> queries = new ArrayList<>();
    while (iterator.hasNext()) {
      queries.add(iterator.next());
    }

    numSplits.inc(queries.size());
    return queries;
  }

  /**
   * Get all UserQueries for a specific time period from RedShift.
   * @param rangeStart Start time of time window
   * @param rangeEnd End time of time window
   * @return List of User Queries
   * @throws MetricAgentException Throw an exception if csv cannot be read
   */
  @Override
  public List<UserQuery> getQueries(LocalDateTime rangeStart, LocalDateTime rangeEnd)
      throws MetricAgentException {
    try {
      final List<UserQuery> queries = combineSplits(getSplitQueries());

      logger.info("numSplits: " + numSplits.getCount());
      logger.info("numSplitEndsWithSlash:" + numSplitEndsWithSlash.getCount());
      logger.info("numQueries:" + numQueries.getCount());

      return queries.stream().filter(q ->
          (q.startTime.isEqual(rangeStart) || q.startTime.isAfter(rangeStart))
              && q.startTime.isBefore(rangeEnd))
          .collect(Collectors.toList());
    } catch (IOException exc) {
      throw new MetricAgentException(exc);
    }
  }

  private List<UserQuery> combineSplits(List<SplitUserQuery> splitUserQueries) {
    Map<Integer, List<SplitUserQuery>> groupByQueryId = splitUserQueries.stream().collect(
        Collectors.groupingBy(e -> e.queryId)
    );

    logger.debug("groupByQueryId map size:" + groupByQueryId.size());

    Map<Integer, List<SplitUserQuery>> groupBySortedByQueryId = groupByQueryId.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().stream().sorted().collect(Collectors.toList()))
        );
    logger.debug("groupBySortedByQueryId map size:" + groupBySortedByQueryId.size());

    List<UserQuery> userQueries = new ArrayList<>(groupByQueryId.size());

    groupBySortedByQueryId.forEach((key, value) -> {
      SplitUserQuery splitUserQuery = value.get(0);
      UserQuery userQuery = new UserQuery(
          splitUserQuery.queryId, splitUserQuery.userId, 0, 0,
          splitUserQuery.startTime, splitUserQuery.endTime, splitUserQuery.duration,
          splitUserQuery.db, false, "");

      value.stream().forEach((split) -> {
        String query = split.query.replace("\\n", "\n");
        if (query.endsWith("\\")) {
          this.numSplitEndsWithSlash.inc();
          query = query.substring(0, query.length() - 1);
        }
        userQuery.addQueryFragment(query);
      });
      userQueries.add(userQuery);
      this.numQueries.inc();
    });

    return userQueries;
  }
}
