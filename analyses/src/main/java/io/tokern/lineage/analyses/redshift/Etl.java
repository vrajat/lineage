package io.tokern.lineage.analyses.redshift;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import io.tokern.lineage.catalog.redshift.UserQuery;
import io.tokern.lineage.sqlplanner.QanException;
import io.tokern.lineage.sqlplanner.redshift.QueryClasses;
import io.tokern.lineage.sqlplanner.redshift.RedshiftClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Etl {

  public static class Result {
    final List<Gantt.Entry> gantt;
    final List<Gantt.TimeSlice> timeSlices;
    public final Dag.Graph dag;
    final List<QueryInfo> queries;

    Result(List<Gantt.Entry> gantt, List<Gantt.TimeSlice> timeSlices,
           Dag.Graph dag, List<QueryInfo> queries) {
      this.gantt = gantt;
      this.timeSlices = timeSlices;
      this.dag = dag;
      this.queries = queries;
    }
  }

  class CachedParser extends CacheLoader<UserQuery, QueryInfo> {
    @Override
    public QueryInfo load(UserQuery userQuery) throws Exception {
      QueryClasses classes = classifier.classify(userQuery.query);
      numParsed.inc();

      if (classes.maintenanceContext.isPassed()) {
        numMaintenanceQueries.inc();
      }

      if (classes.insertContext.isPassed()) {
        numInserts.inc();
        if (classes.insertContext.getSources().size() > 0) {
          logger.debug("Num Sources: " + classes.insertContext.getSources().size());
          numInsertsWithSelects.inc();
          return new QueryInfo(userQuery, classes);
        }
      }

      if (classes.ctasContext.isPassed()) {
        numCtasQueries.inc();
        return new QueryInfo(userQuery, classes);
      }

      if (classes.unloadContext.isPassed()) {
        numUnloadQueries.inc();
        return new QueryInfo(userQuery, classes);
      }

      if (classes.copyContext.isPassed()) {
        numCopyQueries.inc();
        return new QueryInfo(userQuery, classes);
      }

      if (classes.selectIntoContext.isPassed()) {
        numSelectInto.inc();
        logger.debug("Select into for " + classes.selectIntoContext.getTargetTable());
        return new QueryInfo(userQuery, classes);
      }
      throw new QanException("Query is not a DML Query");
    }
  }

  private static Logger logger = LoggerFactory.getLogger(Etl.class);

  private Counter numQueries;
  private Counter numParsed;
  private Counter numInserts;
  private Counter numInsertsWithSelects;
  private Counter numMaintenanceQueries;
  private Counter numCtasQueries;
  private Counter numUnloadQueries;
  private Counter numCopyQueries;
  private Counter numSelectInto;

  private RedshiftClassifier classifier;

  private LoadingCache<UserQuery, QueryInfo> parsedQueryCache;

  public Etl(MetricRegistry registry) {
    numQueries = registry.counter("io.tokern.lineage.redshift.Etl.numQueries");
    numParsed = registry.counter("io.tokern.lineage.redshift.Etl.numParsed");
    numInserts = registry.counter("io.tokern.lineage.redshift.Etl.numInserts");
    numInsertsWithSelects = registry.counter("io.tokern.lineage.redshift.Etl.numInsertSelects");
    numMaintenanceQueries = registry.counter("io.tokern.lineage.redshift.Etl.numMaintenanceQueries");
    numCtasQueries = registry.counter("io.tokern.lineage.redshift.Etl.numCtas");
    numUnloadQueries = registry.counter("io.tokern.lineage.redshift.Etl.numUnload");
    numCopyQueries = registry.counter("io.tokern.lineage.redshift.Etl.numCopy");
    numSelectInto = registry.counter("io.tokern.lineage.redshift.Etl.numSelectInto");

    classifier = new RedshiftClassifier();
    parsedQueryCache = CacheBuilder.newBuilder().maximumSize(200000).build(new CachedParser());
  }

  Dag.Node find(Dag.Graph graph, String sourceStr) throws QanException {
    Dag.Node searchNode = new Dag.Node(sourceStr);
    Iterator<Dag.Node> iterator = graph.dag.nodes().iterator();
    while (iterator.hasNext()) {
      Dag.Node i = iterator.next();
      if (i.equals(searchNode)) {
        return i;
      }
    }

    throw new QanException(String.format("Node '%s' not found ", sourceStr));
  }

  public Dag.Graph getSubDag(List<UserQuery> userQueries, String node, boolean isPredecessor) throws QanException {
    Dag.Graph graph = Dag.buildGraph(userQueries, parsedQueryCache);
    Dag.Node source = find(graph, node);

    MutableGraph<Dag.Node> subDag = GraphBuilder.directed().allowsSelfLoops(true).build();
    List<Dag.Node> remainingNodes = new ArrayList<>();
    Set<Dag.Node> processedNodes = new HashSet<>();

    subDag.addNode(source);
    remainingNodes.add(source);

    while (!remainingNodes.isEmpty()) {
      Dag.Node r = remainingNodes.remove(0);
      Set<Dag.Node> connected = isPredecessor ? graph.dag.predecessors(r) : graph.dag.successors(r);
      for (Dag.Node n : connected) {
        if (!processedNodes.contains(n)) {
          subDag.addNode(n);
          remainingNodes.add(n);
          processedNodes.add(n);
        }
        if (isPredecessor) {
          subDag.putEdge(n, r);
        } else {
          subDag.putEdge(r, n);
        }
      }
    }

    ImmutableGraph<Dag.Node> immutableGraph = ImmutableGraph.copyOf(subDag);
    List<Dag.Phase> phases = Dag.topologicalSort(immutableGraph);
    return new Dag.Graph(immutableGraph, phases);
  }

  public Result analyze(List<UserQuery> userQueries) {
    numQueries.inc(userQueries.size());

    List<QueryInfo> queryInfos = null;
    longRunningQueries(userQueries);
    logger.info("Queries parsed");
    final Dag.Graph dag = Dag.buildGraph(userQueries, parsedQueryCache);
    logger.info("DAG created");
    logger.info("Node Graph Map created");
//    final List<Gantt.Entry> gantt = Gantt.sort(queryInfos);
    logger.info("Gantt created");
//    final List<Gantt.TimeSlice> timeSlices = Gantt.histogram(queryInfos);
    logger.info("Histogram created");

    logger.info("numQueries: " + numQueries.getCount());
    logger.info("numMaintenanceQueries: " + numMaintenanceQueries.getCount());
    logger.info("numParsed: " + numParsed.getCount());
    logger.info("numInserts: " + numInserts.getCount());
    logger.info("numInsertWithSelects: " + numInsertsWithSelects.getCount());
    logger.info("numCtas: " + numCtasQueries.getCount());
    logger.info("numUnload: " + numUnloadQueries.getCount());
    logger.info("numCopy: " + numCopyQueries.getCount());
    logger.info("numSelectInto: " + numSelectInto.getCount());

//    return new Result(gantt, timeSlices, dag, queryInfos);
    return new Result(null, null, dag, null);
  }

  private void longRunningQueries(List<UserQuery> queries) {
    logger.info("Long Duration Queries");
    queries.stream()
        .sorted((q1, q2) -> ((Double)q2.duration).compareTo(q1.duration))
        .limit(20)
        .forEach(q -> logger.info(q.toString()));
  }


}
